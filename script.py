import sys
from pathlib import Path
ROOT = Path(r"C:\Users\ceo\OneDrive\Desktop\TrashPanda")
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient
from app import server
from app.db.session import is_db_available

print("DB available:", is_db_available())
server.RUNTIME_ROOT = ROOT / "output" / "fallback_check"
server.JOB_STORE.clear()

SAMPLE = ROOT / "tests" / "data" / "sample_small.csv"

with TestClient(server.app) as client:
    # Empty store, DB down -> should be {"jobs": []}
    r = client.get("/jobs")
    print("Empty /jobs:", r.status_code, r.json())

    # Submit a job
    with SAMPLE.open("rb") as fh:
        sub = client.post("/jobs", files={"file": (SAMPLE.name, fh, "text/csv")}).json()
    job_id = sub["job_id"]

    # Now /jobs should reflect the in-memory job (since DB is down)
    r = client.get("/jobs")
    body = r.json()
    print("After submit /jobs status:", r.status_code)
    print("Number of jobs in list:", len(body["jobs"]))
    if body["jobs"]:
        j = body["jobs"][0]
        print("First job_id matches submitted?", j["job_id"] == job_id)
        print("First job keys:", sorted(j.keys()))
        print("First job status:", j["status"])
        print("First job started_at:", j["started_at"])
