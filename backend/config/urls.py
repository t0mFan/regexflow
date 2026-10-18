from django.urls import path
from pipeline import views
urlpatterns = [path("api/bootstrap/", views.bootstrap), path("api/connections/", views.connections), path("api/files/", views.files), path("api/jobs/", views.jobs), path("api/jobs/<uuid:job_id>/", views.job_detail), path("api/jobs/<uuid:job_id>/cancel/", views.cancel), path("api/jobs/<uuid:job_id>/results/", views.results), path("api/jobs/<uuid:job_id>/ai-context/", views.ai_context), path("api/health/", views.health)]
