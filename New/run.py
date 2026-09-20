#!/usr/bin/env python3
"""
================================================================================
          CHAINTRACE — ALL-IN-ONE CYBER COMMAND LAUNCHER
================================================================================
Launches the entire CHAINTRACE ecosystem in one single command:
  1. Verifies / starts Docker infrastructure (Neo4j graph database & Kafka)
  2. Launches FastAPI Backend API (uvicorn on port 8000)
  3. Launches Frontend Cyber Command Dashboard (HTTP server on port 5050)
  4. Automatically opens the browser to the live command center
  5. Cleanly handles graceful shutdown on Ctrl+C
================================================================================
"""

import sys
import os
import time
import socket
import subprocess
import webbrowser
import signal
from pathlib import Path

# Safe UTF-8 reconfiguration on Windows
try:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if sys.stderr.encoding and sys.stderr.encoding.lower() != 'utf-8':
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

# ANSI Terminal Colors
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

# Windows color support
if os.name == "nt":
    os.system("")

def safe_print(text):
    try:
        print(text, flush=True)
    except UnicodeEncodeError:
        print(text.encode("ascii", errors="replace").decode("ascii"), flush=True)

def print_banner():
    banner = f"""
{CYAN}{BOLD}================================================================================{RESET}
{CYAN}{BOLD}        [CT] CHAINTRACE - LIVE MULE-CHAIN & ATM INTERCEPT RADAR         {RESET}
{CYAN}{BOLD}================================================================================{RESET}
{DIM}  Predicting where stolen money will be withdrawn, while it is still moving{RESET}
"""
    safe_print(banner)

def is_port_open(port, host="127.0.0.1", timeout=0.6):
    """Check if a network port is listening."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            return s.connect_ex((host, port)) == 0
    except Exception:
        return False

def find_best_python():
    """Find Python interpreter with FastAPI & Uvicorn installed."""
    candidates = [
        r"C:\Users\Pratik\AppData\Local\Programs\Python\Python312\python.exe",
        "py -3.12",
        sys.executable,
        "python",
        "python3",
    ]
    for cand in candidates:
        try:
            cmd = cand.split() + ["-c", "import uvicorn, fastapi; print('OK')"]
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=3)
            if "OK" in res.stdout:
                return cand
        except Exception:
            continue
    return sys.executable

def resolve_directories():
    """Find root and New/ folder paths regardless of current working directory."""
    script_dir = Path(__file__).resolve().parent
    if (script_dir / "New" / "index.html").exists():
        root_dir = script_dir
        new_dir = script_dir / "New"
    elif (script_dir / "index.html").exists() and (script_dir.name == "New"):
        root_dir = script_dir.parent
        new_dir = script_dir
    else:
        root_dir = script_dir
        new_dir = script_dir / "New"
    return root_dir, new_dir

def check_docker_services(new_dir):
    """Check and start Neo4j and Kafka if not running."""
    neo4j_up = is_port_open(7687)
    kafka_up = is_port_open(9092)

    if neo4j_up and kafka_up:
        safe_print(f"  {GREEN}[OK]{RESET} {BOLD}Docker Infrastructure:{RESET} Neo4j (7687) and Kafka (9092) are {GREEN}ACTIVE{RESET}")
        return True

    safe_print(f"  {YELLOW}[..]{RESET} Checking Docker infrastructure...")
    try:
        chk = subprocess.run(["docker", "ps"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if chk.returncode == 0:
            compose_file = new_dir / "docker-compose.yml"
            if compose_file.exists():
                safe_print(f"  {CYAN}[..]{RESET} Starting Neo4j & Kafka via docker compose...")
                subprocess.run(["docker", "compose", "-f", str(compose_file), "up", "-d"], check=False)
                for _ in range(10):
                    time.sleep(1)
                    if is_port_open(7687):
                        safe_print(f"  {GREEN}[OK]{RESET} Neo4j graph database started successfully!")
                        break
        else:
            safe_print(f"  {YELLOW}[!]{RESET} Docker not detected or daemon not running. Running in standalone mode.")
    except Exception as e:
        safe_print(f"  {YELLOW}[!]{RESET} Docker check skipped: {e}")
    return False

def main():
    print_banner()

    root_dir, new_dir = resolve_directories()
    py_exec = find_best_python()
    safe_print(f"{DIM}Working Directory:{RESET} {new_dir}")
    safe_print(f"{DIM}Python Environment:{RESET} {py_exec}\n")

    processes = []

    def cleanup(signum=None, frame=None):
        safe_print(f"\n\n{YELLOW}Stopping all CHAINTRACE services...{RESET}")
        for p, name in processes:
            try:
                safe_print(f"  Stopping {name}...")
                p.terminate()
                p.wait(timeout=2)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
        safe_print(f"{GREEN}All services stopped cleanly. Goodbye!{RESET}\n")
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, cleanup)

    # 1. Docker Services Check
    check_docker_services(new_dir)

    # 2. FastAPI Backend on Port 8000
    if is_port_open(8000):
        safe_print(f"  {GREEN}[OK]{RESET} {BOLD}FastAPI Backend:{RESET} Already active on {CYAN}http://localhost:8000{RESET}")
    else:
        safe_print(f"  {CYAN}[..]{RESET} Starting FastAPI Backend on port 8000...")
        backend_cmd = py_exec.split() + ["-m", "uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000"]
        backend_proc = subprocess.Popen(backend_cmd, cwd=str(new_dir))
        processes.append((backend_proc, "FastAPI Backend (port 8000)"))

        # Wait for backend to be ready
        for _ in range(12):
            time.sleep(0.5)
            if is_port_open(8000):
                safe_print(f"  {GREEN}[OK]{RESET} {BOLD}FastAPI Backend:{RESET} Running on {CYAN}http://localhost:8000{RESET}")
                break

    # 3. Frontend Web Server on Port 5050
    if is_port_open(5050):
        safe_print(f"  {GREEN}[OK]{RESET} {BOLD}Frontend Dashboard:{RESET} Already active on {CYAN}http://localhost:5050{RESET}")
    else:
        safe_print(f"  {CYAN}[..]{RESET} Starting Frontend Web Server on port 5050...")
        fe_cmd = [sys.executable, "-m", "http.server", "5050"]
        fe_proc = subprocess.Popen(fe_cmd, cwd=str(new_dir), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append((fe_proc, "Frontend Web Server (port 5050)"))

        # Wait for frontend to be ready
        for _ in range(8):
            time.sleep(0.3)
            if is_port_open(5050):
                safe_print(f"  {GREEN}[OK]{RESET} {BOLD}Frontend Dashboard:{RESET} Running on {CYAN}http://localhost:5050{RESET}")
                break

    # 4. Optional Stream Generator (if --stream or --pipeline flag provided)
    if "--stream" in sys.argv or "--pipeline" in sys.argv:
        safe_print(f"\n  {MAGENTA}[STREAM]{RESET} Launching Live Kafka Simulation Pipeline...")
        p_det = subprocess.Popen(py_exec.split() + ["detector.py"], cwd=str(new_dir))
        processes.append((p_det, "Detector"))
        p_data = subprocess.Popen(py_exec.split() + ["data.py"], cwd=str(new_dir))
        processes.append((p_data, "Data Generator"))
        safe_print(f"  {GREEN}[OK]{RESET} Detector & Transaction Generator streaming into Kafka!")

    # 5. Open Web Browser
    dashboard_url = "http://localhost:5050/index.html"
    safe_print(f"\n{BOLD}{CYAN}--------------------------------------------------------------------------------{RESET}")
    safe_print(f"  >> {BOLD}CHAINTRACE SYSTEM IS ONLINE!{RESET}")
    safe_print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")
    safe_print(f"  * {BOLD}Command Center UI:{RESET}   {CYAN}{BOLD}{dashboard_url}{RESET}")
    safe_print(f"  * {BOLD}Backend API Docs:{RESET}    {CYAN}http://localhost:8000/docs{RESET}")
    if is_port_open(7474):
        safe_print(f"  * {BOLD}Neo4j Browser:{RESET}       {CYAN}http://localhost:7474{RESET} {DIM}(user: neo4j / pass: password123){RESET}")
    if is_port_open(9092):
        safe_print(f"  * {BOLD}Kafka Message Broker:{RESET}{CYAN} localhost:9092{RESET}")
    safe_print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")
    safe_print(f"  {YELLOW}Press Ctrl+C to stop all servers and exit.{RESET}\n")

    # Launch browser automatically
    try:
        webbrowser.open(dashboard_url)
    except Exception:
        pass

    # Keep alive until Ctrl+C
    try:
        while True:
            for p, name in processes:
                if p.poll() is not None:
                    safe_print(f"  {RED}[!]{RESET} Process '{name}' exited with code {p.returncode}")
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup()

if __name__ == "__main__":
    main()
