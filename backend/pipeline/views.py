import functools, json, os, secrets
from django.http import JsonResponse
from django.core.exceptions import ValidationError
from django.middleware.csrf import get_token
from django.views.decorators.http import require_GET, require_POST, require_http_methods
from django.conf import settings
from botocore.exceptions import BotoCoreError, ClientError
from redis.exceptions import RedisError
from .models import Job
from .security import vault_put, vault_get, s3_client, cache
from .tasks import execute_job
from .assistant import table_context, ClarificationRequired, SAMPLE_ROWS, CONTEXT_BYTES

def api(fn):
    @functools.wraps(fn)
    def wrapped(request, *args, **kwargs):
        if not request.session.get("owner"):
            request.session["owner"] = secrets.token_hex(24)
        try:
            return fn(request, *args, **kwargs)
        except ClarificationRequired as exc:
            return JsonResponse({"error": str(exc)}, status=400)
        except (ValueError, KeyError, TypeError, json.JSONDecodeError, ValidationError):
            return JsonResponse({"error": "Invalid request or expired S3 connection. Please check your inputs."}, status=400)
        except Job.DoesNotExist:
            return JsonResponse({"error": "Job not found."}, status=404)
        except (BotoCoreError, ClientError):
            return JsonResponse({"error": "Unable to access S3. Check credentials, bucket, region and permissions."}, status=400)
        except RedisError:
            return JsonResponse({"error": "Task service is unavailable. Please try again shortly."}, status=503)
    return wrapped

def body(request):
    data = json.loads(request.body or b"{}")
    if not isinstance(data, dict):
        raise ValueError()
    return data

def text(data, name, maximum=2000, required=True):
    value = data.get(name, "")
    if not isinstance(value, str) or len(value) > maximum or (required and not value.strip()):
        raise ValueError()
    return value

def serialize(job):
    return {"id": str(job.id), "kind": job.kind, "status": job.status, "progress": job.progress, "stage": job.stage, "metadata": job.metadata, "error": job.error, "created_at": job.created_at.isoformat(), "prompt": job.payload.get("prompt", "")}

def owned(request, job_id):
    return Job.objects.get(pk=job_id, owner=request.session["owner"])

@require_GET
@api
def bootstrap(request):
    token = request.session.get("connection")
    bucket = request.session.get("bucket") if token and cache().exists(f"vault:{request.session['owner']}:{token}") else None
    return JsonResponse({"s3_bucket": bucket, "csrf_token": get_token(request), "demo_enabled": settings.DEMO_ENABLED, "demo_uses_live_ai": settings.DEMO_USE_LIVE_AI, "ai_sample_rows": SAMPLE_ROWS, "ai_context_bytes": CONTEXT_BYTES, "ai_provider": os.getenv("LLM_PROVIDER", "openai")})

@require_POST
@api
def connections(request):
    data = body(request)
    conn = {k: text(data, k, 8192 if k == "session_token" else 512, k in {"access_key", "secret_key", "bucket"}) for k in ["access_key", "secret_key", "bucket", "region", "session_token"]}
    # Session tokens can be longer than long-lived access keys.
    if "/" in conn["bucket"] or len(conn["bucket"]) > 255:
        raise ValueError()
    s3_client(conn).list_objects_v2(Bucket=conn["bucket"], MaxKeys=1)
    token = vault_put(request.session["owner"], conn)
    request.session["connection"] = token
    request.session["bucket"] = conn["bucket"]
    return JsonResponse({"bucket": conn["bucket"], "expires_in": 86400})

@require_GET
@api
def files(request):
    conn = vault_get(request.session["owner"], request.session["connection"])
    args = {"Bucket": conn["bucket"], "MaxKeys": 100}
    cursor = request.GET.get("cursor", "")
    if cursor:
        if len(cursor) > 8192:
            raise ValueError()
        args["ContinuationToken"] = cursor
    result = s3_client(conn).list_objects_v2(**args)
    items = [{"key": x["Key"], "size": x["Size"]} for x in result.get("Contents", []) if x["Key"].lower().endswith((".csv", ".xlsx"))]
    return JsonResponse({"files": items, "cursor": result.get("NextContinuationToken"), "bucket": conn["bucket"]})

@require_http_methods(["GET", "POST"])
@api
def jobs(request):
    if request.method == "GET":
        return JsonResponse({"jobs": [serialize(j) for j in Job.objects.filter(owner=request.session["owner"]).order_by("-created_at")[:30]]})
    data = body(request)
    kind = text(data, "kind", 16)
    if Job.objects.filter(owner=request.session["owner"], status__in=["QUEUED", "RUNNING"]).count() >= 3:
        return JsonResponse({"error": "Wait for an active job to finish before starting another."}, status=429)
    if kind == "ingest":
        demo = data.get("demo") is True
        if demo:
            if not settings.DEMO_ENABLED:
                return JsonResponse({"error": "Demo mode is disabled."}, status=403)
            payload = {"demo": True, "key": "demo-customers.csv"}
        else:
            conn = request.session["connection"]
            vault_get(request.session["owner"], conn)
            key = text(data, "key", 1024)
            if not key.lower().endswith((".csv", ".xlsx")):
                raise ValueError()
            payload = {"connection": conn, "key": key}
    elif kind == "transform":
        dataset = Job.objects.get(pk=text(data, "dataset", 36), owner=request.session["owner"], kind__in=["ingest", "transform"], status="SUCCESS")
        if data.get("operation"):
            # Backward-compatible CLI/API support; the web UI uses only chat.
            operation = text(data, "operation", 16)
            if operation not in {"replace", "extract", "normalize"}:
                raise ValueError()
            columns = data.get("columns")
            if not isinstance(columns, list) or not columns or any(not isinstance(c, str) or c not in dataset.metadata["columns"] for c in columns):
                raise ValueError()
            payload = {"dataset": str(dataset.id), "operation": operation, "columns": list(dict.fromkeys(columns)), "prompt": text(data, "prompt"), "replacement": text(data, "replacement", 2000, False)}
        else:
            messages = data.get("messages", [])
            if not isinstance(messages,list) or len(messages)>6:
                raise ValueError()
            for message in messages:
                if not isinstance(message,dict) or set(message)!={"role","content"} or message["role"] not in {"user","assistant"} or not isinstance(message["content"],str) or len(message["content"])>1500:
                    raise ValueError()
            if sum(len(m["content"]) for m in messages)>6000:
                raise ValueError()
            payload={"mode":"assistant", "dataset":str(dataset.id), "prompt":text(data,"prompt",4000), "messages":messages}

    else:
        raise ValueError()
    job = Job.objects.create(owner=request.session["owner"], kind=kind, payload=payload)
    try:
        execute_job.apply_async(args=[str(job.id)], task_id=str(job.id), argsrepr="(<job id>,)")
    except Exception:
        job.status, job.stage, job.error = "FAILED", "Queue unavailable", "Unable to enqueue job. Please retry."
        job.save()
        return JsonResponse({"error": job.error}, status=503)
    return JsonResponse(serialize(job), status=202)

@require_GET
@api
def job_detail(request, job_id):
    return JsonResponse(serialize(owned(request, job_id)))

@require_POST
@api
def cancel(request, job_id):
    job = owned(request, job_id)
    if job.status not in {"SUCCESS", "FAILED", "CANCELLED"}:
        Job.objects.filter(pk=job.id, status__in=["QUEUED", "RUNNING"]).update(cancel_requested=True, stage="Cancellation requested")
        # A queued task will observe the flag at its first stage; an active Spark
        # task is interrupted by the worker's cancellation monitor.
    job.refresh_from_db()
    return JsonResponse(serialize(job))

@require_GET
@api
def results(request, job_id):
    job = owned(request, job_id)
    if job.status != "SUCCESS":
        return JsonResponse({"error": "Results are not ready."}, status=409)
    page = int(request.GET.get("page", "1"))
    directory = settings.DATA_ROOT / str(job.id) / "pages"
    manifest = json.loads((directory / "manifest.json").read_text())
    if not manifest["pages"] and page == 1:
        rows = []
    elif page < 1 or page > len(manifest["pages"]):
        return JsonResponse({"error": "Page not found."}, status=404)
    else:
        rows = json.loads((directory / manifest["pages"][page - 1]).read_text())
    return JsonResponse({"columns": manifest["columns"], "rows": rows, "total_rows": manifest["rows"], "page": page, "page_count": len(manifest["pages"]), "page_size": 100})

@require_GET
def health(request):
    try:
        Job.objects.exists()
        cache().ping()
        return JsonResponse({"status": "ok"})
    except Exception:
        return JsonResponse({"status": "unavailable"}, status=503)


@require_GET
@api
def ai_context(request, job_id):
    job=owned(request,job_id)
    if job.status!="SUCCESS":
        return JsonResponse({"error":"The dataset is not ready yet."},status=409)
    return JsonResponse(table_context(job))
