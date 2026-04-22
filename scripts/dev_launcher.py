"""TrashPanda dev launcher — starts FastAPI + Next.js in parallel."""

import os
import sys
import shutil
import subprocess
import webbrowser
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
VENV = ROOT / ".venv"
FRONTEND = ROOT / "trashpanda-next"

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
RESET  = "\033[0m"


def fail(msg: str) -> None:
    print(f"{RED}[ERROR]{RESET} {msg}")
    sys.exit(1)


def warn(msg: str) -> None:
    print(f"{YELLOW}[WARN]{RESET}  {msg}")


def info(msg: str) -> None:
    print(f"{CYAN}[INFO]{RESET}  {msg}")


def validate() -> None:
    is_windows = sys.platform == "win32"
    activate = VENV / ("Scripts/activate" if is_windows else "bin/activate")
    if not activate.exists():
        fail(f".venv not found at {VENV}. Run: python -m venv .venv && pip install -r requirements.txt")

    if not FRONTEND.exists():
        fail("trashpanda-next/ folder not found.")

    if not (FRONTEND / "node_modules").exists():
        warn("node_modules not found. Running npm install...")
        subprocess.run(["npm", "install"], cwd=FRONTEND, check=True)


def uvicorn_cmd() -> list[str]:
    is_windows = sys.platform == "win32"
    python = VENV / ("Scripts/python.exe" if is_windows else "bin/python")
    return [str(python), "-m", "uvicorn", "app.server:app", "--reload", "--port", "8000"]


def start_backend() -> subprocess.Popen:
    info("Starting FastAPI on http://localhost:8000 ...")
    if sys.platform == "win32":
        return subprocess.Popen(
            uvicorn_cmd(),
            cwd=ROOT,
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )
    return subprocess.Popen(uvicorn_cmd(), cwd=ROOT)


def start_frontend() -> subprocess.Popen:
    info("Starting Next.js on http://localhost:3000 ...")
    npm = shutil.which("npm") or "npm"
    if sys.platform == "win32":
        return subprocess.Popen(
            [npm, "run", "dev"],
            cwd=FRONTEND,
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )
    return subprocess.Popen([npm, "run", "dev"], cwd=FRONTEND)


def main() -> None:
    print(f"\n{CYAN} =========================================")
    print(f"  TrashPanda - Local Dev Launcher")
    print(f" ========================================={RESET}\n")

    validate()

    backend  = start_backend()
    frontend = start_frontend()

    print(f"\n Both servers are starting...")
    print(f" Open: {CYAN}http://localhost:3000{RESET}")
    print(f" Press Ctrl+C to stop both.\n")

    time.sleep(5)
    webbrowser.open("http://localhost:3000")

    try:
        backend.wait()
        frontend.wait()
    except KeyboardInterrupt:
        info("Stopping servers...")
        backend.terminate()
        frontend.terminate()
        info("Done.")


if __name__ == "__main__":
    main()
