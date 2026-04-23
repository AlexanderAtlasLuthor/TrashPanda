"""TrashPanda dev launcher — starts FastAPI + Next.js in parallel."""

import shutil
import socket
import subprocess
import sys
import time
import webbrowser
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


def venv_python() -> Path:
    is_windows = sys.platform == "win32"
    return VENV / ("Scripts/python.exe" if is_windows else "bin/python")


def is_port_in_use(port: int) -> bool:
    """Return True if something is already listening on localhost:port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        try:
            s.connect(("localhost", port))
            return True   # connected → port is occupied
        except (ConnectionRefusedError, TimeoutError, OSError):
            return False  # refused → port is free


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

    info("Checking Python backend dependencies...")
    dependency_check = subprocess.run(
        [
            str(venv_python()),
            "-c",
            "from app.db.dependencies import ensure_database_dependencies; ensure_database_dependencies()",
        ],
        cwd=ROOT,
        check=False,
    )
    if dependency_check.returncode != 0:
        fail(
            "Backend Python dependencies are incomplete. "
            f"Run: {venv_python()} -m pip install -r requirements.txt"
        )

    info("Verifying ports are available...")

    if is_port_in_use(8000):
        fail("Port 8000 is already in use. Please close the existing backend process and try again.")

    if is_port_in_use(3000):
        fail("Port 3000 is already in use. Please close the existing frontend process and try again.")


def uvicorn_cmd() -> list[str]:
    return [str(venv_python()), "-m", "uvicorn", "app.server:app", "--reload", "--port", "8000"]


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


def wait_for_frontend(timeout: int = 60) -> None:
    """Poll localhost:3000 until it responds or timeout is reached."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if is_port_in_use(3000):
            return
        time.sleep(2)


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

    info("Waiting for http://localhost:3000 to be ready...")
    wait_for_frontend()
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
