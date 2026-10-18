import csv, json, logging, os, shutil, threading, time
from pathlib import Path
from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError
from openai import APIConnectionError, APITimeoutError, RateLimitError, APIStatusError
from django.conf import settings
from django.db import close_old_connections
from .models import Job
from .assistant import table_context, make_table_plan, ClarificationRequired
from .table_engine import execute_table_plan
from pyspark import StorageLevel
from .security import vault_get, s3_client
from .planner import make_plan
from .engine import spark_session, load_frame, transform, materialize, matching_stats

class Cancelled(Exception):
    pass

def update(job_id, progress, stage):
    if Job.objects.filter(pk=job_id, cancel_requested=True).exists():
        raise Cancelled()
    Job.objects.filter(pk=job_id).update(status="RUNNING", progress=progress, stage=stage)

def monitor_cancel(job_id, context, stopped):
    close_old_connections()
    try:
        while not stopped.wait(1):
            if Job.objects.filter(pk=job_id, cancel_requested=True).exists():
                context.cancelJobGroup(str(job_id))
                return
    finally:
        close_old_connections()

@shared_task(bind=True, max_retries=2)
def execute_job(self, job_id):
    job = Job.objects.get(pk=job_id)
    if job.status in {"SUCCESS", "CANCELLED", "FAILED", "NEEDS_INPUT"}:
        return
    stopped, watcher, spark = threading.Event(), None, None
    source, context = None, None
    root = settings.DATA_ROOT / str(job.id)
    try:
        update(job.id, 5, "Preparing data")
        root.mkdir(parents=True, exist_ok=True)
        payload = job.payload
        spark = spark_session()
        spark.sparkContext.setJobGroup(str(job.id), "RegexFlow job", interruptOnCancel=True)
        watcher = threading.Thread(target=monitor_cancel, args=(job.id, spark.sparkContext, stopped), daemon=True)
        watcher.start()
        if job.kind == "ingest":
            path = root / ("source.xlsx" if payload.get("key", "").lower().endswith(".xlsx") else "source.csv")
            if payload.get("demo"):
                with path.open("w", newline="") as out:
                    writer = csv.writer(out)
                    writer.writerow(["ID", "Name", "Email", "Notes"])
                    for i in range(1, 251):
                        writer.writerow([i, ["  John Doe  ", "Jane Smith", "Alice Brown"][i % 3], f"person{i}@example.com", f"Contact person{i}@example.com for details"])
            else:
                connection = vault_get(job.owner, payload["connection"])
                client = s3_client(connection)
                # Downloads stream through boto3 to disk, never into API memory.
                client.download_file(connection["bucket"], payload["key"], str(path))
            update(job.id, 25, "Reading file into Spark partitions")
            frame = load_frame(spark, path, settings.SPARK_PARTITIONS)
            frame.write.mode("overwrite").parquet(str(root / "data.parquet"))
            frame = spark.read.parquet(str(root / "data.parquet"))
            update(job.id, 65, "Building paginated preview")
            metadata = materialize(frame, root / "pages")
            metadata["demo"] = bool(payload.get("demo"))
            metadata["filename"] = payload.get("key", "demo-customers.csv")
        else:
            source = Job.objects.get(pk=payload["dataset"], owner=job.owner, status="SUCCESS", kind__in=["ingest", "transform"])
            if payload.get("mode") == "assistant":
                update(job.id, 15, "Reading table samples and planning edits")
                context=table_context(source)
                plan,hit=make_table_plan(payload["prompt"],context,payload.get("messages",[]))
                if plan["status"] == "clarify":
                    raise ClarificationRequired(plan["message"])
                frame=spark.read.parquet(str(settings.DATA_ROOT/str(source.id)/"data.parquet"))
                def checkpoint(index,total):
                    update(job.id,30+int(index*40/total),f"Applying edit {index+1} of {total}")
                output,statistics=execute_table_plan(frame,plan,checkpoint)
                output=output.persist(StorageLevel.DISK_ONLY)
                output.write.mode("overwrite").parquet(str(root/"data.parquet"))
                update(job.id,80,"Saving the updated table")
                # Use the same persisted frame so ordered edits retain page order.
                metadata=materialize(output,root/"pages")
                output.unpersist()
                original=source.metadata.get("original_dataset") or (source.metadata.get("dataset",str(source.id)) if source.kind=="transform" else str(source.id))
                metadata.update(statistics)
                metadata.update({"mode":"assistant","plan":plan,"reply":plan["message"],"cache_hit":hit,"dataset":str(source.id),"original_dataset":original,"demo":source.metadata.get("demo",False),"preset":False,"provider":os.getenv("LLM_PROVIDER","openai"),"ai_context":{"sample_rows":len(context["sample_rows"]),"truncated":context["truncated"],"total_rows":context["total_rows"]}})
            else:
                update(job.id, 15, "Creating transformation plan")
                preset = source.metadata.get("demo", False) and not settings.DEMO_USE_LIVE_AI
                plan, hit = make_plan(payload["prompt"], payload["operation"], preset, columns=payload["columns"])
                update(job.id, 35, "Transforming data across Spark partitions")
                frame = spark.read.parquet(str(settings.DATA_ROOT / str(source.id) / "data.parquet"))
                if any(c not in frame.columns for c in payload["columns"]):
                    raise ValueError("A selected column is no longer available.")
                update(job.id, 40, "Checking rule matches")
                stats = matching_stats(frame, payload["columns"], payload["operation"], plan)
                output = transform(frame, payload["columns"], payload["operation"], plan, payload.get("replacement", ""))
                # Materialize once to avoid repeating a transformation for preview.
                output.write.mode("overwrite").parquet(str(root / "data.parquet"))
                update(job.id, 75, "Preparing result pages")
                metadata = materialize(spark.read.parquet(str(root / "data.parquet")), root / "pages")
                metadata.update(stats)
                metadata.update({"target_columns": payload["columns"], "plan": plan, "cache_hit": hit, "demo": source.metadata.get("demo", False), "operation": payload["operation"], "dataset": str(source.id), "preset": preset, "provider": "preset" if preset else os.getenv("LLM_PROVIDER", "openai")})
        update(job.id, 95, "Finalizing result")
        Job.objects.filter(pk=job.id, cancel_requested=False).update(status="SUCCESS", progress=100, stage="Completed", metadata=metadata, error="")
        if Job.objects.filter(pk=job.id, cancel_requested=True).exists():
            raise Cancelled()
    except ClarificationRequired as exc:
        if Job.objects.filter(pk=job.id,cancel_requested=True).exists():
            Job.objects.filter(pk=job.id).update(status="CANCELLED",stage="Cancelled")
        else:
            Job.objects.filter(pk=job.id).update(status="NEEDS_INPUT",progress=100,stage="Waiting for clarification",error="",metadata={"mode":"assistant","reply":str(exc),"dataset":job.payload.get("dataset"),"original_dataset":source.metadata.get("original_dataset") or (source.metadata.get("dataset",str(source.id)) if source.kind=="transform" else str(source.id)) if source else job.payload.get("dataset")})
    except Cancelled:
        Job.objects.filter(pk=job.id).update(status="CANCELLED", stage="Cancelled", error="")
    except (APIConnectionError, APITimeoutError, RateLimitError, EndpointConnectionError, ConnectionClosedError) as exc:
        if Job.objects.filter(pk=job.id,cancel_requested=True).exists():
            Job.objects.filter(pk=job.id).update(status="CANCELLED",stage="Cancelled")
            return
        if self.request.retries < self.max_retries:
            Job.objects.filter(pk=job.id).update(status="QUEUED", stage="Temporary service error; retry scheduled")
            delay=5 * 2 ** self.request.retries
            if isinstance(exc, RateLimitError):
                try:delay=max(delay,min(90,float(exc.response.headers.get("retry-after","60"))))
                except (TypeError,ValueError):delay=60
                Job.objects.filter(pk=job.id).update(stage="AI rate limit reached; waiting to retry")
            raise self.retry(countdown=delay, exc=RuntimeError("Temporary external service failure"))
        Job.objects.filter(pk=job.id).update(status="FAILED", stage="Failed", error="External service unavailable after retries. Please try again.")
    except APIStatusError as exc:
        if Job.objects.filter(pk=job.id,cancel_requested=True).exists():
            Job.objects.filter(pk=job.id).update(status="CANCELLED",stage="Cancelled")
            return
        body=getattr(exc,"body",{})
        details=body.get("error",body) if isinstance(body,dict) else {}
        code=details.get("code","") if isinstance(details,dict) else ""
        logging.getLogger(__name__).warning("AI job %s failed: status=%s code=%s",str(job.id),exc.status_code,str(code)[:80])
        if (exc.status_code in {413,429,500,502,503,504} or code=="json_validate_failed") and self.request.retries<self.max_retries:
            Job.objects.filter(pk=job.id).update(status="QUEUED",stage="AI service is busy; waiting to retry")
            raise self.retry(countdown=60,exc=RuntimeError("Temporary AI service failure"))
        Job.objects.filter(pk=job.id).update(status="FAILED",stage="AI request failed",error="The AI service could not complete this request. Retry later or shorten your instruction, and check model access and quota.")
    except Exception as exc:
        logging.getLogger(__name__).warning("Job %s failed (%s)",str(job.id),type(exc).__name__)
        cancelled = Job.objects.filter(pk=job.id, cancel_requested=True).exists()
        # Do not persist exception strings from AWS, Spark or the LLM SDK.
        message = str(exc)[:400] if isinstance(exc, ValueError) else "Processing failed. Check file format, service configuration, and available resources."
        if isinstance(exc, ClientError):
            message = "S3 access failed. Check credentials, object permissions, and bucket region."
        if isinstance(exc, SoftTimeLimitExceeded):
            message = "Job exceeded the two-hour processing limit."
        Job.objects.filter(pk=job.id).update(status="CANCELLED" if cancelled else "FAILED", stage="Cancelled" if cancelled else "Failed", error="" if cancelled else message)
    finally:
        stopped.set()
        if watcher:
            watcher.join(timeout=3)
        if spark:
            # Release cluster executors so other drivers and benchmarks can run.
            spark.stop()
        # Remove S3 source copies; Parquet and pages remain for follow-up operations.
        for name in ("source.csv", "source.xlsx", "source.xlsx.csv"):
            (root / name).unlink(missing_ok=True)
