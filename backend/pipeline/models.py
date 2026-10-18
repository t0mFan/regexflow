import uuid
from django.db import models
class Job(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.CharField(max_length=64, db_index=True)
    kind = models.CharField(max_length=16)
    status = models.CharField(max_length=16, default="QUEUED")
    progress = models.PositiveSmallIntegerField(default=0)
    stage = models.CharField(max_length=160, default="Waiting for a worker")
    payload = models.JSONField(default=dict)
    metadata = models.JSONField(default=dict)
    error = models.CharField(max_length=400, blank=True)
    cancel_requested = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
