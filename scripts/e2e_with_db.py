"""End-to-end DB-backed validation against SQLite (dev only).

Production target is PostgreSQL. This script exists so a developer can
exercise the full DB write/read path on a laptop without provisioning
PostgreSQL: it installs the dev-only SQLite shim from
``scripts/_dev_db_shim.py``, points the app at a temporary SQLite file,
runs the pipeline once via ``TestClient``, and verifies that

    * the job lifecycle is persisted (queued / started / completed),
    * lifecycle timestamps are timezone-aware UTC,
    * artifacts in the DB match files on disk, and
    * ``/results`` reads back from the DB-first read path.

For an authoritative end-to-end run against the production schema, run
this script with ``TRASHPANDA_DATABASE_URL`` pointing at a real
PostgreSQL instance and remove the ``enable_sqlite_compat()`` call.
"""

from __future__ import annotations

import json
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# ---- Step 1: install dev shim BEFORE importing app.db.* ------------------- #
from scripts._dev_db_shim import configure_sqlite_url, enable_sqlite_compat

DB_FILE = ROOT / "output" / "e2e_db" / "trashpanda_e2e.sqlite"
DB_FILE.parent.mkdir(parents=True, exist_ok=True)
if DB_FILE.exists():
    DB_FILE.unlink()

enable_sqlite_compat()
DB_URL = configure_sqlite_url(DB_FILE.as_posix())

# ---- Step 2: bootstrap schema --------------------------------------------- #
from app.db.init_db import init_db
from app.db.session import (
    dispose_engine,
    is_db_available,
    reset_db_availability_cache,
    session_scope,
)
from app.db import models as _models  # noqa: F401  side-effect import

init_db()
reset_db_availability_cache()
print("DB URL:", DB_URL)
print("DB available:", is_db_available())

# ---- Step 3: drive the API end-to-end ------------------------------------- #
from fastapi.testclient import TestClient
from sqlalchemy import select

from app import server
from app.db.models import Artifact, Job, UploadedFile

SAMPLE = ROOT / "tests" / "data" / "sample_small.csv"
e2e_root = ROOT / "output" / "e2e_db"
for child in ("jobs", "uploads"):
    p = e2e_root / child
    if p.exists():
        shutil.rmtree(p)

server.RUNTIME_ROOT = e2e_root
server.JOB_STORE.clear()

with TestClient(server.app) as client:
    with SAMPLE.open("rb") as fh:
        r = client.post("/jobs", files={"file": (SAMPLE.name, fh, "text/csv")})
    print("POST /jobs ->", r.status_code)
    payload = r.json()
    job_id = payload["job_id"]
    print("job_id:", job_id, "initial:", payload.get("status"))

    body = None
    for _ in range(60):
        body = client.get(f"/status/{job_id}").json()
        if body["status"] in ("completed", "failed"):
            break
        time.sleep(0.5)
    print("FINAL STATUS:", body["status"])

    # Brief settle so the background DB commit lands before we read.
    time.sleep(0.5)

    with session_scope() as s:
        jobs = s.execute(select(Job)).scalars().all()
        print(f"\n=== DB Jobs ({len(jobs)}) ===")
        for j in jobs:
            print(
                f"  id={j.id!r} status={j.status} "
                f"queued_at={j.queued_at} started_at={j.started_at} "
                f"completed_at={j.completed_at}"
            )
            print(
                f"  tz: queued.tz={j.queued_at.tzinfo} "
                f"started.tz={j.started_at.tzinfo if j.started_at else None} "
                f"completed.tz={j.completed_at.tzinfo if j.completed_at else None}"
            )
            print(
                f"  summary: input={j.summary_total_input_rows} "
                f"valid={j.summary_total_valid} "
                f"review={j.summary_total_review} "
                f"invalid={j.summary_total_invalid_or_bounce_risk}"
            )
        arts = s.execute(select(Artifact)).scalars().all()
        print(f"\n=== DB Artifacts: {len(arts)} ===")
        uploads = s.execute(select(UploadedFile)).scalars().all()
        print(f"=== DB Uploads:   {len(uploads)} ===")
        for u in uploads:
            print(f"  key={u.storage_key} status={u.status} size={u.size_bytes}")

    print("\n=== FS<->DB consistency ===")
    with session_scope() as s:
        arts = s.execute(select(Artifact)).scalars().all()
        missing = [a for a in arts if not Path(a.storage_location).is_file()]
        print(f"DB artifacts: {len(arts)}  missing on disk: {len(missing)}")
        for a in missing:
            print("  MISSING:", a.artifact_key, a.storage_location)

    rr = client.get(f"/results/{job_id}").json()
    print(f"\n/results SUMMARY: {json.dumps(rr.get('summary'), indent=2)}")
    print(
        "/results buckets counts: "
        + str({k: v.get("count") for k, v in (rr.get("buckets") or {}).items()})
    )

dispose_engine()
import os, sys, json, shutil, time, glob
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(r"C:\Users\ceo\OneDrive\Desktop\TrashPanda")

# Force SQLite BEFORE importing app.db
db_file = ROOT / "output" / "e2e_db" / "trashpanda_e2e.sqlite"
db_file.parent.mkdir(parents=True, exist_ok=True)
if db_file.exists():
    db_file.unlink()
os.environ["TRASHPANDA_DATABASE_URL"] = f"sqlite:///{db_file.as_posix()}"

sys.path.insert(0, str(ROOT))

# MOCK JSONB and INET for SQLite compatibility
import sqlalchemy.dialects.postgresql as pg
from sqlalchemy import JSON, String
pg.JSONB = JSON
pg.INET = String

from app.db.session import get_engine, dispose_engine, is_db_available, reset_db_availability_cache
from app.db.init_db import init_db
from app.db import models as _m

# Initialize schema
init_db()
reset_db_availability_cache()
print("DB URL:", os.environ["TRASHPANDA_DATABASE_URL"])
print("DB available:", is_db_available())

from fastapi.testclient import TestClient
from app import server

SAMPLE = ROOT / "tests" / "data" / "sample_small.csv"
e2e_root = ROOT / "output" / "e2e_db"
for child in ("jobs", "uploads"):
    p = e2e_root / child
    if p.exists():
        shutil.rmtree(p)
server.RUNTIME_ROOT = e2e_root
server.JOB_STORE.clear()

with TestClient(server.app) as client:
    with SAMPLE.open("rb") as fh:
        r = client.post("/jobs", files={"file": (SAMPLE.name, fh, "text/csv")})
    print("POST /jobs ->", r.status_code)
    payload = r.json()
    job_id = payload["job_id"]
    print("job_id:", job_id, "initial:", payload.get("status"))

    body = None
    for _ in range(60):
        body = client.get(f"/status/{job_id}").json()
        if body["status"] in ("completed", "failed"):
            break
        time.sleep(1.0) # Increased wait to ensure worker finishes DB write
    print("FINAL STATUS:", body["status"])

    # Wait a bit more to ensure DB transaction is committed
    time.sleep(1.0)

    # Now query the DB directly to validate persistence
    from sqlalchemy import select
    from app.db.session import session_scope
    from app.db.models import Job, Artifact, UploadedFile

    with session_scope() as s:
        jobs = s.execute(select(Job)).scalars().all()
        print(f"\n=== DB Jobs ({len(jobs)}) ===")
        for j in jobs:
            # We use id instead of legacy_job_id because id and legacy_job_id are mapped to the same job_id in this context
            print(f"  id={j.id!r} status={j.status} queued_at={j.queued_at} started_at={j.started_at} completed_at={j.completed_at}")
            print(f"  summary: input={j.summary_total_input_rows} valid={j.summary_total_valid} review={j.summary_total_review} invalid={j.summary_total_invalid_or_bounce_risk}")
        arts = s.execute(select(Artifact)).scalars().all()
        print(f"\n=== DB Artifacts ({len(arts)}) ===")
        print(f"Total artifacts: {len(arts)}")
        uploads = s.execute(select(UploadedFile)).scalars().all()
        print(f"\n=== DB Uploads ({len(uploads)}) ===")
        for u in uploads:
            print(f"  key={u.storage_key} status={u.status} kind={u.content_kind} size={u.size_bytes}")

    # Validate filesystem matches DB artifact paths
    print(f"\n=== FS<->DB consistency check ===")
    with session_scope() as s:
        arts = s.execute(select(Artifact)).scalars().all()
        missing = [a for a in arts if not Path(a.storage_location).is_file()]
        print(f"Total artifacts in DB: {len(arts)}")
        print(f"Missing on disk:       {len(missing)}")
        for a in missing:
            print("  MISSING:", a.artifact_key, a.storage_location)

    # Hit /results to verify DB-first read path
    rr = client.get(f"/results/{job_id}").json()
    print(f"\n/results SUMMARY: {json.dumps(rr.get('summary'), indent=2)}")
    print(f"/results buckets counts: " + str({k: v.get('count') for k,v in (rr.get('buckets') or {}).items()}))

dispose_engine()
