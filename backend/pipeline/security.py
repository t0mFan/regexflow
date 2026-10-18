import json, os, secrets
import boto3
from botocore.config import Config
from cryptography.fernet import Fernet
from django.conf import settings
import redis

def cache():
    return redis.Redis.from_url(settings.REDIS_URL, socket_connect_timeout=3, socket_timeout=5)

def vault_put(owner, data):
    token = secrets.token_urlsafe(32)
    cipher = Fernet(os.environ["CREDENTIAL_ENCRYPTION_KEY"].encode())
    cache().setex(f"vault:{owner}:{token}", 86400, cipher.encrypt(json.dumps(data).encode()))
    return token

def vault_get(owner, token):
    value = cache().get(f"vault:{owner}:{token}")
    if not value:
        raise ValueError("S3 connection expired. Please reconnect.")
    cipher = Fernet(os.environ["CREDENTIAL_ENCRYPTION_KEY"].encode())
    return json.loads(cipher.decrypt(value))

def s3_client(conn):
    return boto3.client("s3", aws_access_key_id=conn["access_key"], aws_secret_access_key=conn["secret_key"], aws_session_token=conn.get("session_token") or None, region_name=conn.get("region") or "us-east-1", config=Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 2}))
