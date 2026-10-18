#!/usr/bin/env python3
"""Actual Spark benchmark. Run in worker container or configured local backend."""
import argparse, json, os, sys, tempfile, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django
django.setup()
from pipeline.engine import spark_session, transform, materialize
from pipeline.planner import validate_pattern
from pyspark.sql import functions as F
parser = argparse.ArgumentParser()
parser.add_argument("--rows", type=int, default=1000000)
parser.add_argument("--output", default="benchmark-results.json")
args = parser.parse_args()
spark = spark_session()
started = time.perf_counter()
frame = spark.range(args.rows).select(F.col("id").cast("string").alias("ID"), F.concat(F.lit("person"), F.col("id"), F.lit("@example.com")).alias("Email"))
plan = {"pattern": validate_pattern(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), "normalization": "", "explanation": "Benchmark"}
result = transform(frame, ["Email"], "replace", plan, "REDACTED").cache()
count = result.count()
invalid = result.filter(F.col("Email") != "REDACTED").count()
from django.conf import settings
with tempfile.TemporaryDirectory(dir=settings.DATA_ROOT) as out:
    metadata = materialize(result, out)
elapsed = time.perf_counter() - started
report = {"rows": count, "incorrect_rows": invalid, "seconds": round(elapsed, 3), "rows_per_second": round(count / elapsed), "pages": metadata["page_count"], "master": spark.sparkContext.master, "partitions": frame.rdd.getNumPartitions(), "spark_version": spark.version, "includes_paginated_materialization": True}
Path(args.output).write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
spark.stop()
if count != args.rows or invalid:
    raise SystemExit("Benchmark correctness check failed")
