import json, sys, shutil, time, glob
from pathlib import Path
ROOT = Path(r"C:\Users\ceo\OneDrive\Desktop\TrashPanda")
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient
from app import server
from app.db.session import is_db_available

SAMPLE = ROOT / "tests" / "data" / "sample_small.csv"
print("=== E2E run ===")
print("Sample:", SAMPLE, "exists:", SAMPLE.is_file())

e2e_root = ROOT / "output" / "e2e_validation"
if e2e_root.exists():
    shutil.rmtree(e2e_root)
e2e_root.mkdir(parents=True)
server.RUNTIME_ROOT = e2e_root
server.JOB_STORE.clear()

print("DB available:", is_db_available())

with TestClient(server.app) as client:
    with SAMPLE.open("rb") as fh:
        r = client.post("/jobs", files={"file": (SAMPLE.name, fh, "text/csv")})
    print("POST /jobs ->", r.status_code)
    payload = r.json()
    job_id = payload["job_id"]
    initial_status = payload.get("status")
    print("job_id:", job_id, "initial:", initial_status, "started_at:", payload.get("started_at"))

    body = None
    for _ in range(60):
        body = client.get(f"/status/{job_id}").json()
        if body["status"] in ("completed", "failed"):
            break
        time.sleep(0.2)
    print("FINAL STATUS:", body["status"])
    print("started_at:", body.get("started_at"), "finished_at:", body.get("finished_at"))
    if body.get("error"):
        print("ERROR:", json.dumps(body["error"], indent=2))

    rr = client.get(f"/results/{job_id}")
    print("GET /results ->", rr.status_code)
    if rr.status_code == 200:
        results = rr.json()
        print("SUMMARY:", json.dumps(results.get("summary"), indent=2))
        print("BUCKETS:", json.dumps(results.get("buckets"), indent=2))
        print("REPORTS keys:", list((results.get("reports") or {}).keys()))

    rd = client.get(f"/jobs/{job_id}/artifacts/processing_report_json")
    print("Download processing_report_json ->", rd.status_code, "bytes:", len(rd.content))
    rz = client.get(f"/jobs/{job_id}/artifacts/zip")
    print("Download zip ->", rz.status_code, "bytes:", len(rz.content))

print(f"\n=== Disk artifacts under {e2e_root}/jobs/{job_id} ===")
files = [Path(p) for p in glob.glob(str(e2e_root / "jobs" / job_id / "**"), recursive=True) if Path(p).is_file()]
print(f"Total files: {len(files)}")
ext_count = {}
for p in files:
    ext = p.suffix.lower()
    ext_count[ext] = ext_count.get(ext, 0) + 1
    print(f"  {p.relative_to(e2e_root)}  ({p.stat().st_size} B)")
print("Extensions:", ext_count)
