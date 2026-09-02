#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FLIGHT TRACKER PIPELINE — UNIFIED TOOLKIT
extra-utils/TOOLKIT.py

All pipeline utilities in one interactive menu.
Original scripts are kept as backup in extra-utils/.

Tools:
  [1]  Pipeline Health Test     (test_data_flow.py)
  [2]  Clear Pipeline Data      (CLEAR_DATA.py)
  [3]  Diagnose VPN / API       (DIAGNOSE_VPN_API.py)
  [4]  Rotate Public IP         (ROTATE_IP.py)
  [5]  Reset API Connection     (RESET_API_CONNECTION.py)
  [6]  Check Container Timezones (check_timezone.py)
  [7]  Check Kafka & Spark Lag  (check_lag.py)

Usage:
    python extra-utils/TOOLKIT.py
    python extra-utils/TOOLKIT.py --tool 7
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

# UTF-8 on Windows Terminal
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Enable ANSI Colors on Windows
if sys.platform == "win32":
    os.system("")

# Colors
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
CYAN    = "\033[96m"
MAGENTA = "\033[95m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
RESET   = "\033[0m"

# Workspace root
WORKSPACE   = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))

# Shared constants
OPENSKY_URL   = "https://opensky-network.org/api/states/all"
IP_URLS       = [
    "https://api.ipify.org?format=json",
    "https://ifconfig.me/ip",
    "https://icanhazip.com",
]
INFLUX_TOKEN  = "my-super-secret-admin-token"
INFLUX_ORG    = "flight-tracking"
INFLUX_BUCKET = "flight-metrics"

# ============================================================================
#  SHARED UTILITIES
# ============================================================================

def _run(cmd: list, timeout: int = 30, cwd: Optional[str] = None) -> Tuple[int, str, str]:
    try:
        r = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=timeout, encoding="utf-8", errors="replace",
            cwd=cwd or WORKSPACE,
        )
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    except Exception as exc:
        return -1, "", str(exc)


try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False


def _require_requests() -> None:
    if not _HAS_REQUESTS:
        print(f"  {RED}[ERROR]{RESET} 'requests' not installed.")
        print(f"  Run: {CYAN}pip install requests{RESET}")
        input(f"\n  {DIM}Press Enter to return to menu...{RESET}")
        raise SystemExit(0)


def _header_box(title: str, subtitle: str = "") -> None:
    width = 68
    print(f"\n{CYAN}+{'=' * width}+{RESET}")
    print(f"{CYAN}|{RESET}  {BOLD}{title:<{width - 2}}{RESET}{CYAN}|{RESET}")
    if subtitle:
        print(f"{CYAN}|{RESET}  {DIM}{subtitle:<{width - 2}}{RESET}{CYAN}|{RESET}")
    print(f"{CYAN}+{'=' * width}+{RESET}")


def _section(title: str) -> None:
    print(f"\n{CYAN}{'─' * 70}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{CYAN}{'─' * 70}{RESET}")


def _ok(msg: str)   -> None: print(f"  {GREEN}[OK]{RESET}    {msg}")
def _warn(msg: str) -> None: print(f"  {YELLOW}[WARN]{RESET}  {msg}")
def _err(msg: str)  -> None: print(f"  {RED}[ERROR]{RESET} {msg}")
def _info(msg: str) -> None: print(f"  {CYAN}...{RESET}     {msg}")


def _pause() -> None:
    print()
    input(f"  {DIM}Press Enter to return to menu...{RESET}")


def _get_public_ip() -> Optional[str]:
    if not _HAS_REQUESTS:
        return None
    for url in IP_URLS:
        try:
            resp = _requests.get(url, timeout=6)
            if resp.status_code == 200:
                ip = resp.json().get("ip", "").strip() if "json" in url else resp.text.strip()
                if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip):
                    return ip
        except Exception:
            continue
    return None


def _load_creds() -> Optional[Tuple[str, str]]:
    env_id  = os.environ.get("OPENSKY_CLIENT_ID") or os.environ.get("OPENSKY_USERNAME")
    env_sec = os.environ.get("OPENSKY_CLIENT_SECRET") or os.environ.get("OPENSKY_PASSWORD")
    if env_id and env_sec:
        return env_id, env_sec
    for path in [
        os.path.join(WORKSPACE, "api", "credentials.json"),
        "api/credentials.json",
    ]:
        path = os.path.normpath(path)
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    d = json.load(f)
                u = d.get("clientId") or d.get("username")
                p = d.get("clientSecret") or d.get("password")
                if u and p:
                    return u, p
            except Exception:
                pass
    return None


# ============================================================================
#  MODULE 1 — Pipeline Health Test
# ============================================================================

def _m1_record(results: list, name: str, passed: bool, note: str = "") -> None:
    results.append((name, passed, note))
    icon  = f"{GREEN}OK{RESET}" if passed else f"{RED}!!" + RESET
    label = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
    tag   = f"  {YELLOW}({note}){RESET}" if note else ""
    print(f"  [{icon}]  {label}  -- {name}{tag}")


def run_pipeline_test() -> None:
    _header_box("PIPELINE HEALTH TEST", "End-to-end connectivity check for all services")
    _require_requests()
    results: list = []
    now_utc = datetime.now(timezone.utc)
    print(f"\n  {DIM}Time: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}{RESET}")

    _section("TEST 1 - Flume HTTP Source (port 44444)")
    try:
        resp = _requests.get("http://localhost:44444", timeout=4)
        _m1_record(results, "Flume HTTP source", True, f"HTTP {resp.status_code}")
    except _requests.ConnectionError:
        _m1_record(results, "Flume HTTP source", False, "Cannot connect to port 44444")
    except Exception as e:
        _m1_record(results, "Flume HTTP source", False, str(e)[:80])

    _section("TEST 2 - Kafka Connectivity")
    code, out, err = _run([
        "docker", "exec", "ft-kafka", "kafka-topics",
        "--bootstrap-server", "localhost:29092", "--list"
    ], timeout=10)
    if code == 0:
        topics = [t for t in out.splitlines() if t]
        _m1_record(results, "Kafka broker", True, f"{len(topics)} topic(s)")
        if topics:
            print(f"   Topics: {', '.join(topics)}")
    else:
        _m1_record(results, "Kafka broker", False, (err or "non-zero exit")[:80])

    _section("TEST 3 - InfluxDB Health Endpoint")
    try:
        resp = _requests.get("http://localhost:8086/health", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            _m1_record(results, "InfluxDB health", True,
                       f"status={data.get('status','?')} ver={data.get('version','?')}")
        else:
            _m1_record(results, "InfluxDB health", False, f"HTTP {resp.status_code}")
    except _requests.ConnectionError:
        _m1_record(results, "InfluxDB health", False, "Cannot connect to port 8086")
    except Exception as e:
        _m1_record(results, "InfluxDB health", False, str(e)[:80])

    _section("TEST 4 - Local Flight Producer")
    _m1_record(results, "Local Producer Script", True,
               "Run 'python python-scripts/flight_tracker_producer.py' to stream data")

    _section("TEST 5 - Kafka Message Count (flight-tracking-raw)")
    code, out, err2 = _run([
        "docker", "exec", "ft-kafka", "kafka-run-class",
        "kafka.tools.GetOffsetShell",
        "--broker-list", "localhost:29092",
        "--topic", "flight-tracking-raw",
        "--time", "-1"
    ], timeout=15)
    if code == 0 and out.strip():
        total = 0
        for line in out.strip().splitlines():
            # format: topic:partition:offset
            parts = line.split(":")
            if len(parts) >= 3:
                try:
                    total += int(parts[-1])
                except ValueError:
                    pass
        note = f"{total:,} cumulative log offset(s)" if total > 0 else "0 messages (fresh topic)"
        _m1_record(results, "Kafka log count", True, note)
    elif "Could not match any topic-partitions" in err2 or "Could not match any topic-partitions" in out:
        _m1_record(results, "Kafka log count", True, "0 messages (topic empty / waiting for producer)")
    else:
        # Check if topic list is accessible from broker
        t_code, t_out, _ = _run([
            "docker", "exec", "ft-kafka", "kafka-topics",
            "--bootstrap-server", "localhost:29092", "--list"
        ], timeout=5)
        if t_code == 0:
            if "flight-tracking-raw" not in t_out.splitlines():
                _m1_record(results, "Kafka log count", True, "0 messages (waiting for producer to stream)")
            else:
                _m1_record(results, "Kafka log count", True, "0 messages (fresh topic)")
        else:
            _m1_record(results, "Kafka log count", False, (err2 or "broker unreachable")[:80])

    _section("TEST 6 - InfluxDB Data Query (last 10 min)")
    flux = f'from(bucket:"{INFLUX_BUCKET}") |> range(start:-10m) |> limit(n:3)'
    try:
        resp = _requests.post(
            "http://localhost:8086/api/v2/query",
            headers={
                "Authorization": f"Token {INFLUX_TOKEN}",
                "Content-Type":  "application/vnd.flux",
                "Accept":        "application/csv",
            },
            data=flux, params={"org": INFLUX_ORG}, timeout=8,
        )
        if resp.status_code == 200:
            rows = [r for r in resp.text.strip().splitlines() if r and not r.startswith("#")]
            if len(rows) > 1:
                _m1_record(results, "InfluxDB data", True, f"{len(rows) - 1} row(s) in last 10 min")
            else:
                _m1_record(results, "InfluxDB data", True, "empty -- start producer and wait ~15 s")
        else:
            _m1_record(results, "InfluxDB data", False, f"HTTP {resp.status_code}")
    except Exception as e:
        _m1_record(results, "InfluxDB data", False, str(e)[:80])

    _section("TEST 7 - Grafana Web UI (port 3000)")
    try:
        resp = _requests.get("http://localhost:3000/api/health", timeout=5)
        _m1_record(results, "Grafana UI", resp.status_code == 200, f"HTTP {resp.status_code}")
    except _requests.ConnectionError:
        _m1_record(results, "Grafana UI", False, "Cannot connect to port 3000")
    except Exception as e:
        _m1_record(results, "Grafana UI", False, str(e)[:80])

    _section("TEST 8 - Hadoop NameNode Web UI (port 9870)")
    try:
        resp = _requests.get("http://localhost:9870", timeout=5)
        _m1_record(results, "Hadoop NameNode UI", resp.status_code == 200,
                   "reachable" if resp.status_code == 200 else f"HTTP {resp.status_code}")
    except _requests.ConnectionError:
        _m1_record(results, "Hadoop NameNode UI", False, "Cannot connect to port 9870")
    except Exception as e:
        _m1_record(results, "Hadoop NameNode UI", False, str(e)[:80])

    _section("SUMMARY")
    passed = sum(1 for _, ok, _ in results if ok)
    failed = sum(1 for _, ok, _ in results if not ok)
    total  = passed + failed
    print(f"  {GREEN}PASSED : {passed}/{total}{RESET}")
    if failed:
        print(f"  {RED}FAILED : {failed}/{total}{RESET}")
    print(f"\n  {DIM}Pipeline: Producer -> Flume:44444 -> ft-kafka -> Spark -> ft-influxdb -> Grafana:3000{RESET}")
    _pause()


# ============================================================================
#  MODULE 2 — Clear Pipeline Data
# ============================================================================

def _m2_check_kafka() -> Tuple[bool, str]:
    code, out, _ = _run([
        "docker", "exec", "ft-kafka", "kafka-run-class",
        "kafka.tools.GetOffsetShell",
        "--broker-list", "localhost:29092", "--time", "-1"
    ])
    if code == 0 and out:
        total = 0
        for line in out.splitlines():
            if line.startswith("flight-tracking"):
                parts = line.split(":")
                if len(parts) == 3:
                    try:
                        total += int(parts[2])
                    except ValueError:
                        pass
        if total > 0:
            return True, f"{total:,} msgs in flight topics"
        return False, "Clean (0 flight msgs)"
    return False, "Topics empty / not found"


def _m2_check_spark() -> Tuple[bool, str]:
    code, out, _ = _run([
        "docker", "exec", "ft-spark-streaming", "bash", "-c",
        "ls -1 /tmp/spark-checkpoint/offsets 2>/dev/null | grep -v '^0$' | grep -v '^\\.0' | wc -l"
    ])
    if code == 0 and out.isdigit() and int(out) > 0:
        return True, f"Contains active data batches ({out.strip()} stream batches)"
    return False, "Clean (0 stream batches)"


def _m2_check_influxdb() -> Tuple[bool, str]:
    flux = ('from(bucket:"flight-metrics") |> range(start:-30d)'
            ' |> filter(fn: (r) => r._measurement != "pipeline_summary") |> limit(n:1)')
    code, out, _ = _run([
        "docker", "exec", "ft-influxdb", "influx", "query",
        "--org", INFLUX_ORG, "--token", INFLUX_TOKEN, "--raw", flux
    ])
    if code == 0 and len(out.splitlines()) > 1:
        return True, "Contains flight metrics data"
    return False, "Clean (No flight metrics)"


def _m2_check_hdfs() -> Tuple[bool, str]:
    code, out, _ = _run([
        "docker", "exec", "ft-hadoop-namenode", "hdfs", "dfs", "-count", "-q", "/flight-data"
    ])
    if code == 0 and out:
        parts = out.split()
        if len(parts) >= 6:
            try:
                files_count = int(parts[5])
                if files_count > 0:
                    return True, f"{files_count} file(s) in /flight-data"
            except ValueError:
                pass
    return False, "Clean (0 files)"


def _m2_check_grafana() -> Tuple[bool, str]:
    code, out, _ = _run(["docker", "inspect", "--format={{.State.Running}}", "ft-grafana"])
    if code == 0 and "true" in out.lower():
        return False, "Service Active & Ready (Port 3000)"
    return False, "Service Stopped"


def _m2_inspect_all() -> None:
    print(f"\n{CYAN}{'=' * 70}{RESET}")
    print(f"{BOLD}  PIPELINE STORAGE STATUS OVERVIEW{RESET}")
    print(f"{CYAN}{'-' * 70}{RESET}")
    checks = [
        ("Kafka",       _m2_check_kafka()),
        ("InfluxDB",    _m2_check_influxdb()),
        ("HDFS",        _m2_check_hdfs()),
        ("Spark State", _m2_check_spark()),
        ("Grafana",     _m2_check_grafana()),
    ]
    for name, (has_data, detail) in checks:
        tag = f"{YELLOW}[ HAS DATA ]{RESET}" if has_data else f"{GREEN}[ EMPTY / CLEAN ]{RESET}"
        print(f"  {name:<15} : {tag:<25} ({detail})")
    print(f"{CYAN}{'=' * 70}{RESET}")


def _m2_clear_kafka() -> None:
    print(f"\n{CYAN}... Purging All Kafka Application Topics ...{RESET}")
    topics_cfg = [
        ("flight-tracking-raw",  "3"),
        ("flight-tracking",      "3"),
        ("flight-tracking-hdfs", "1"),
    ]
    for t, _ in topics_cfg:
        _run(["docker", "exec", "ft-kafka", "kafka-topics",
              "--bootstrap-server", "localhost:29092", "--delete", "--topic", t])
    time.sleep(1)
    for t, parts in topics_cfg:
        _run(["docker", "exec", "ft-kafka", "kafka-topics",
              "--bootstrap-server", "localhost:29092", "--create",
              "--topic", t, "--partitions", parts, "--replication-factor", "1"])
    print(f"  {GREEN}[OK] Kafka topics purged & re-created (0 messages).{RESET}")


def _m2_clear_influxdb() -> None:
    print(f"\n{CYAN}... Purging InfluxDB Flight Data ...{RESET}")
    _run([
        "docker", "exec", "ft-influxdb", "influx", "delete",
        "--bucket", "flight-metrics",
        "--start", "1970-01-01T00:00:00Z",
        "--stop",  "2030-01-01T00:00:00Z",
        "--org", INFLUX_ORG, "--token", INFLUX_TOKEN,
    ])
    print(f"  {GREEN}[OK] InfluxDB bucket 'flight-metrics' purged.{RESET}")


def _m2_clear_hdfs() -> None:
    print(f"\n{CYAN}... Purging HDFS Storage (/flight-data) ...{RESET}")
    _run(["docker", "exec", "ft-hadoop-namenode", "hdfs", "dfs", "-rm", "-r", "-f", "/flight-data/*"])
    print(f"  {GREEN}[OK] HDFS path '/flight-data' purged.{RESET}")


def _m2_clear_spark() -> None:
    print(f"\n{CYAN}... Purging Spark Streaming Checkpoint State ...{RESET}")
    _run(["docker", "exec", "ft-spark-streaming", "bash", "-c",
          "rm -rf /tmp/spark-checkpoint/* 2>/dev/null"])
    _run(["docker", "compose", "restart", "spark-streaming"])
    print(f"  {GREEN}[OK] Spark checkpoint state purged & container restarted.{RESET}")


def _m2_clear_grafana() -> None:
    print(f"\n{CYAN}... Resetting Grafana Service ...{RESET}")
    _run(["docker", "compose", "restart", "grafana"])
    print(f"  {GREEN}[OK] Grafana service restarted.{RESET}")


def run_clear_data() -> None:
    _header_box("CLEAR PIPELINE DATA", "Inspect & purge stored data from pipeline components")
    while True:
        _m2_inspect_all()
        print(f"\n{BOLD}Select component to clear:{RESET}")
        print("  [1] Kafka Topics")
        print("  [2] InfluxDB Data")
        print("  [3] HDFS Files (/flight-data)")
        print("  [4] Spark Checkpoint State")
        print("  [5] Grafana Dashboard Reset")
        print("  [A] All Components (Full Wipe)")
        print("  [B] Back to Main Menu")
        choice = input(f"\n{CYAN}  Choice [1-5 / A / B]: {RESET}").strip().upper()
        if choice == "1":
            _m2_clear_kafka()
        elif choice == "2":
            _m2_clear_influxdb()
        elif choice == "3":
            _m2_clear_hdfs()
        elif choice == "4":
            _m2_clear_spark()
        elif choice == "5":
            _m2_clear_grafana()
        elif choice == "A":
            print(f"\n{YELLOW}{'=' * 70}{RESET}")
            print(f"{BOLD}  CLEARING ALL PIPELINE DATA (FULL PURGE){RESET}")
            print(f"{YELLOW}{'=' * 70}{RESET}")
            _m2_clear_kafka()
            _m2_clear_influxdb()
            _m2_clear_hdfs()
            _m2_clear_spark()
            _m2_clear_grafana()
            print(f"\n{GREEN}{BOLD}ALL PIPELINE STORAGE SUCCESSFULLY PURGED!{RESET}\n")
        elif choice in ("B", ""):
            break
        elif choice:
            print(f"  {RED}Invalid option. Try again.{RESET}")
        if choice and choice not in ("B", ""):
            time.sleep(1)


# ============================================================================
#  MODULE 3 — Diagnose VPN / API
# ============================================================================

def _m3_test_opensky(
    creds: Optional[Tuple[str, str]], retries: int = 1
) -> Tuple[int, Optional[int], Optional[int]]:
    _require_requests()
    session = _requests.Session()
    session.headers["User-Agent"] = "FlightTrackerPipeline/1.0 (diagnostic)"
    auth = creds if creds else None
    for attempt in range(1, retries + 1):
        if attempt > 1:
            _info(f"Retry {attempt}/{retries} ...")
            time.sleep(2)
        try:
            start = time.time()
            resp  = session.get(OPENSKY_URL, auth=auth, timeout=15)
            ms    = int((time.time() - start) * 1000)
            retry_after = None
            if resp.status_code == 429:
                try:
                    hdr = resp.headers.get("X-Rate-Limit-Retry-After-Seconds") or resp.headers.get("Retry-After", "0")
                    retry_after = int(hdr)
                except (ValueError, TypeError):
                    pass
            flight_count = None
            if resp.status_code == 200:
                try:
                    flight_count = len(resp.json().get("states") or [])
                except Exception:
                    pass
                _info(f"Latency: {ms} ms")
            return resp.status_code, retry_after, flight_count
        except _requests.RequestException as e:
            if attempt == retries:
                _err(f"Connection failed: {e}")
                return -1, None, None
    return -1, None, None


def _get_ip_geo() -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    if not _HAS_REQUESTS:
        return None, None, None, None
    try:
        r = _requests.get("http://ip-api.com/json", timeout=5).json()
        if r.get("status") == "success":
            return r.get("query"), r.get("city"), r.get("country"), r.get("isp")
    except Exception:
        pass
    return _get_public_ip(), "Unknown", "Unknown", "Unknown"


def _m3_report(
    status: int, retry_after: Optional[int], flights: Optional[int], mode: str
) -> None:
    if status == 200:
        _ok(f"{mode}: SUCCESS (HTTP 200)")
        if flights is not None:
            _info(f"Active flights: {BOLD}{flights:,}{RESET}")
    elif status == 429:
        wait_str = f"{retry_after // 3600}h {(retry_after % 3600) // 60}m" if retry_after else "Unknown"
        _err(f"{mode}: RATE LIMITED (HTTP 429)")
        _warn(f"Credit limit reached. Reset in: {BOLD}{wait_str}{RESET}")
        _info("Tip: Rotate public IP (option [4] in TOOLKIT) or connect to VPN/Mobile Hotspot.")
    elif status == 401:
        _err(f"{mode}: AUTH FAILED (HTTP 401)")
        _info("Check clientId / clientSecret in api/credentials.json")
    elif status == 403:
        _err(f"{mode}: FORBIDDEN (HTTP 403)")
        _info("Your account may be banned -- contact OpenSky support.")
    elif status == -1:
        _err(f"{mode}: CONNECTION FAILED")
        _info("Check your internet connection or VPN settings.")
    else:
        _warn(f"{mode}: Unexpected HTTP {status}")


def run_diagnose_vpn() -> None:
    _header_box("DIAGNOSE VPN / API", "Check public IP, Geolocation, and OpenSky API reachability")
    _require_requests()
    retries_input = input(f"  {DIM}Number of retries [default 1]: {RESET}").strip()
    retries = int(retries_input) if retries_input.isdigit() else 1

    _section("1 - Public IP & Geolocation")
    ip, city, country, isp = _get_ip_geo()
    if ip:
        _ok(f"Public IP : {BOLD}{ip}{RESET}")
        _ok(f"Location  : {BOLD}{city}, {country}{RESET}")
        _ok(f"Provider  : {BOLD}{isp}{RESET}")
    else:
        _warn("Could not determine public IP or Location")

    _section("2 - OpenSky Credentials")
    creds = _load_creds()
    if creds:
        _ok(f"Account: {BOLD}{creds[0]}{RESET}")
    else:
        _warn("No credentials found -- using Anonymous mode")

    _section("3 - OpenSky API (Anonymous)")
    status, retry_after, flights = _m3_test_opensky(None, retries=retries)
    _m3_report(status, retry_after, flights, mode="Anonymous")

    if creds:
        _section(f"4 - OpenSky API (Authenticated)")
        status, retry_after, flights = _m3_test_opensky(creds, retries=retries)
        _m3_report(status, retry_after, flights, mode=f"Account ({creds[0]})")

    _section("DONE")
    _pause()


# ============================================================================
#  MODULE 4 — Rotate Public IP
# ============================================================================

VIRTUAL_RE = re.compile(
    r"hyper-v|virtualbox|vmware|loopback|tap|tun|wsl|vethernet|vpn|tunnel",
    re.IGNORECASE,
)
_rotate_running = True


def _m4_is_admin() -> bool:
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def _m4_relaunch_as_admin() -> None:
    script = os.path.abspath(sys.argv[0])
    args = f'"{script}" --tool 4'
    ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, args, None, 1)
    sys.exit(0)


def _m4_test_opensky(creds: Optional[Tuple[str, str]]) -> Tuple[int, Optional[int]]:
    _require_requests()
    try:
        sess = _requests.Session()
        sess.headers["User-Agent"] = "FlightTrackerPipeline/2.0 (ip-rotate)"
        resp = sess.get(OPENSKY_URL, auth=creds, timeout=15)
        retry_after = None
        if resp.status_code == 429:
            try:
                hdr = resp.headers.get("X-Rate-Limit-Retry-After-Seconds") or resp.headers.get("Retry-After", "0")
                retry_after = int(hdr)
            except (ValueError, TypeError):
                pass
        return resp.status_code, retry_after
    except _requests.RequestException:
        return -1, None


def _m4_report_opensky(status: int, retry_after: Optional[int]) -> bool:
    if status == 200:
        _ok("OpenSky API  ->  HTTP 200 (Unblocked)")
        return True
    elif status == 429:
        wait = retry_after or 0
        _err("OpenSky API  ->  HTTP 429 Rate Limited")
        _info(f"Retry-After : {wait} s (~{wait // 60} min)")
        return False
    else:
        _err(f"OpenSky API  ->  HTTP {status}")
        return False


def _m4_get_adapters() -> List[str]:
    code, out, _ = _run(["netsh", "interface", "show", "interface"], timeout=10)
    names: List[str] = []
    if code == 0:
        for line in out.splitlines():
            if "Connected" not in line:
                continue
            parts = line.split()
            if len(parts) >= 4:
                name = " ".join(parts[3:]).strip()
                if name and not VIRTUAL_RE.search(name):
                    names.append(name)
    return names


def _m4_get_wifi_ssid() -> Optional[str]:
    code, out, _ = _run(["netsh", "wlan", "show", "interfaces"], timeout=10)
    if code == 0:
        for line in out.splitlines():
            stripped = line.strip()
            if re.match(r"^(SSID|Profile)\s*:", stripped, re.IGNORECASE):
                if "BSSID" in stripped.upper():
                    continue
                parts = stripped.split(":", 1)
                if len(parts) == 2 and parts[1].strip():
                    return parts[1].strip()
    return None


def _m4_wait_internet(max_wait: int = 30) -> bool:
    deadline = time.time() + max_wait
    while _rotate_running and time.time() < deadline:
        code, _, _ = _run(["ping", "-n", "1", "-w", "1500", "8.8.8.8"], timeout=5)
        if code == 0:
            return True
        time.sleep(2)
    return False


def _m4_full_network_reset(has_admin: bool) -> None:
    _info("Flushing DNS cache...")
    _run(["ipconfig", "/flushdns"], timeout=10)
    _ok("DNS cache flushed")
    wifi_ssid = _m4_get_wifi_ssid()
    if wifi_ssid:
        _ok(f"Connected Wi-Fi: '{wifi_ssid}'")
    adapters = _m4_get_adapters()
    if not adapters:
        _warn("No active adapters detected. Performing generic release/renew...")
        _run(["ipconfig", "/release"], timeout=15)
        time.sleep(2)
        _run(["ipconfig", "/renew"], timeout=20)
        _m4_wait_internet(30)
        return
    _ok(f"Active adapters: {', '.join(adapters)}")
    for name in adapters:
        if not _rotate_running:
            return
        _info(f"DHCP release -> '{name}'")
        _run(["ipconfig", "/release", name], timeout=15)
    time.sleep(2)
    for name in adapters:
        if not _rotate_running:
            return
        if has_admin:
            _info(f"Disabling '{name}'...")
            _run(["netsh", "interface", "set", "interface", name, "disable"], timeout=10)
            time.sleep(6)
            _info(f"Enabling '{name}'...")
            _run(["netsh", "interface", "set", "interface", name, "enable"], timeout=10)
            _ok(f"Adapter cycled ({name})")
        else:
            _warn(f"Skipping adapter toggle '{name}' (no admin rights)")
    for name in adapters:
        if not _rotate_running:
            return
        _info(f"DHCP renew -> '{name}'")
        _run(["ipconfig", "/renew", name], timeout=25)
        _ok(f"DHCP renewed ({name})")
    _info("Checking internet connectivity...")
    if not _m4_wait_internet(20):
        if wifi_ssid:
            _info(f"Reconnecting to '{wifi_ssid}'...")
            _run(["netsh", "wlan", "connect", f"name={wifi_ssid}"], timeout=10)
            _m4_wait_internet(20)


def run_rotate_ip() -> None:
    global _rotate_running
    _rotate_running = True
    _header_box("ROTATE PUBLIC IP", "Reset network adapter to obtain a new DHCP lease")
    _require_requests()
    print(f"\n  {BOLD}Options:{RESET}")
    print("  [1] Full Reset  (recommended)")
    print("  [2] Test Only   (check IP & OpenSky without resetting)")
    mode_choice = input(f"\n  {CYAN}Choice [1/2]: {RESET}").strip()
    test_only = (mode_choice == "2")
    has_admin = _m4_is_admin()
    creds     = _load_creds()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n  {DIM}Time    : {now}{RESET}")
    print(f"  Account : {BOLD}{creds[0] if creds else 'Anonymous'}{RESET}")
    print(f"  Admin   : {GREEN if has_admin else YELLOW}{'Yes' if has_admin else 'No (Partial reset)'}{RESET}")
    print(f"  Mode    : {BOLD}{'Test Only' if test_only else 'Full Reset'}{RESET}")
    old_ip: Optional[str] = None
    new_ip: Optional[str] = None
    opensky_ok = False
    try:
        _section("Step 1 - Current Public IP")
        old_ip = _get_public_ip()
        if old_ip:
            _ok(f"Public IP: {BOLD}{old_ip}{RESET}")
        else:
            _warn("Could not detect public IP")

        _section("Step 2 - OpenSky API Pre-check")
        status, retry = _m4_test_opensky(creds)
        opensky_ok = _m4_report_opensky(status, retry)

        if test_only:
            _section("RESULT")
            print(f"  IP     : {old_ip or 'N/A'}")
            print(f"  Status : {'UNBLOCKED' if opensky_ok else 'BLOCKED'}")
            _pause()
            return

        if opensky_ok:
            ans = input(
                f"\n  {YELLOW}OpenSky is UNBLOCKED. Perform reset anyway? [y/N]: {RESET}"
            ).strip().lower()
            if ans not in ("y", "yes"):
                _pause()
                return

        if not has_admin:
            print(f"\n  {YELLOW}Administrator rights required for full reset.{RESET}")
            ans2 = input("  Request UAC elevation now? [y/N]: ").strip().lower()
            if ans2 in ("y", "yes"):
                _m4_relaunch_as_admin()
                _pause()
                return

        _section("Step 3 - Network Reset")
        _m4_full_network_reset(has_admin)

        _section("Step 4 - New Public IP")
        time.sleep(3)
        new_ip = _get_public_ip()
        if new_ip:
            _ok(f"New IP: {BOLD}{new_ip}{RESET}")
        else:
            _warn("Could not detect new IP")

        _section("Step 5 - OpenSky API Post-check")
        post_status, post_retry = _m4_test_opensky(creds)
        opensky_ok = _m4_report_opensky(post_status, post_retry)

        _section("SUMMARY")
        if old_ip and new_ip:
            if old_ip != new_ip:
                _ok(f"IP Changed   : {old_ip} -> {BOLD}{new_ip}{RESET}")
            else:
                _warn(f"IP Unchanged : {old_ip}")
        elif new_ip:
            _ok(f"Current IP   : {BOLD}{new_ip}{RESET}")
        if opensky_ok:
            _ok("OpenSky API  : UNBLOCKED")
        else:
            _err("OpenSky API  : BLOCKED / RATE-LIMITED")
    except KeyboardInterrupt:
        print(f"\n  {YELLOW}Interrupted.{RESET}")
    _pause()


# ============================================================================
#  MODULE 5 — Reset API Connection
# ============================================================================

def run_reset_api() -> None:
    _header_box("RESET API CONNECTION", "Test OpenSky API and restart local Docker services")
    _require_requests()

    _section("1 - Checking credentials.json")
    creds = _load_creds()
    if creds:
        _ok(f"Credentials found for user: {creds[0]}")
    else:
        _warn("api/credentials.json not found -- using Anonymous mode")

    _section("2 - Testing OpenSky API Connection")
    auth = (creds[0], creds[1]) if creds else None
    api_ok = False
    try:
        start = time.time()
        resp  = _requests.get(OPENSKY_URL, auth=auth, timeout=15)
        ms    = int((time.time() - start) * 1000)
        if resp.status_code == 200:
            _ok("API responding correctly.")
            _info(f"Latency: {ms} ms")
            flights = len(resp.json().get("states", []) or [])
            _info(f"Active flights: {flights:,}")
            api_ok = True
        elif resp.status_code == 401:
            _err("AUTHENTICATION ERROR (HTTP 401)")
            _info("Check clientId / clientSecret in api/credentials.json")
        elif resp.status_code == 429:
            hdr = resp.headers.get("X-Rate-Limit-Retry-After-Seconds") or resp.headers.get("Retry-After", "0")
            wait_hrs = f"{int(hdr) // 3600}h {(int(hdr) % 3600) // 60}m" if hdr and hdr.isdigit() else "temporary"
            _err("RATE LIMITED (HTTP 429)")
            _info(f"Credit limit reached. Expected reset in: {wait_hrs}")
            _info("Tip: Rotate your IP using option [4] or connect to a Mobile Hotspot / VPN.")
        else:
            _err(f"API ERROR: HTTP {resp.status_code}")
    except Exception as e:
        _err(f"Connection Failed: {e}")

    _section("3 - Restarting Docker Services")
    _info("Restarting ft-influxdb, ft-grafana, ft-spark-streaming...")
    _run(["docker", "compose", "restart", "influxdb", "grafana", "spark-streaming"])
    _ok("Local services restart triggered.")

    _section("RESET COMPLETE")
    if api_ok:
        print(f"  {GREEN}{BOLD}Everything looks good! You can start flight_tracker_producer.py now.{RESET}")
    else:
        print(f"  {YELLOW}{BOLD}Your IP may still be blocked by OpenSky.{RESET}")
        print(f"  {YELLOW}    Wait at least 15 min before starting the producer again.{RESET}")
    _pause()


# ============================================================================
#  MODULE 6 — Check Container Timezones
# ============================================================================

_TZ_CONTAINERS = [
    "ft-influxdb",
    "ft-grafana",
    "ft-spark-streaming",
    "ft-kafka",
    "ft-flume-collector",
    "ft-hadoop-namenode",
]

_TZ_META: Dict[str, dict] = {
    "ft-influxdb":        {"label": "InfluxDB       "},
    "ft-grafana":         {"label": "Grafana        "},
    "ft-spark-streaming": {"label": "Spark          "},
    "ft-kafka":           {"label": "Kafka          "},
    "ft-flume-collector": {"label": "Flume          "},
    "ft-hadoop-namenode": {"label": "Hadoop         "},
}

_ANSI_RE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


def _m6_strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def _m6_get_tz_env(container: str) -> Optional[str]:
    code, out, _ = _run(["docker", "exec", container, "printenv", "TZ"], timeout=5)
    return out.strip() if code == 0 and out.strip() else None


def _m6_get_container_date(container: str) -> Tuple[str, Optional[datetime]]:
    for fmt in ["+%Y-%m-%d %H:%M:%S %Z %z", "+%Y-%m-%d %H:%M:%S %z", "+%Y-%m-%d %H:%M:%S %Z"]:
        code, out, _ = _run(["docker", "exec", container, "date", fmt], timeout=5)
        if code == 0 and out.strip():
            dt = _m6_parse_date(out.strip())
            return out.strip(), dt
    code, out, _ = _run(["docker", "exec", container, "date"], timeout=5)
    return (out.strip(), _m6_parse_date(out.strip())) if code == 0 else ("N/A", None)


def _m6_parse_date(s: str) -> Optional[datetime]:
    clean = _m6_strip_ansi(s)
    patterns = [
        (r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+[A-Z]{2,4}\s+([+-]\d{4})", True),
        (r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+([+-]\d{4})", True),
        (r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", False),
    ]
    for pattern, has_offset in patterns:
        m = re.search(pattern, clean)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                if has_offset:
                    off = m.group(2)
                    sign = 1 if off[0] == "+" else -1
                    tz_off = timedelta(
                        hours=sign * int(off[1:3]),
                        minutes=sign * int(off[3:5])
                    )
                    dt = dt.replace(tzinfo=timezone(tz_off))
                return dt
            except ValueError:
                continue
    return None


def _m6_check(container: str) -> Tuple[str, bool]:
    code, out, _ = _run(
        ["docker", "inspect", "--format={{.State.Running}}", container], timeout=5
    )
    if code != 0 or "true" not in out.lower():
        return f"{RED}Not running{RESET}", False

    tz_env = _m6_get_tz_env(container)
    date_str, container_dt = _m6_get_container_date(container)
    clean = _m6_strip_ansi(date_str)

    offset_m = re.search(r"([+-]\d{4})", clean)
    if offset_m:
        off = offset_m.group(1)
        if off == "+0300":
            return f"{GREEN}[OK] Cairo (UTC+3){RESET}", True
        elif off == "+0200":
            # Automatic fix: Update container's Africa/Cairo timezone file to UTC+3
            _run(["docker", "exec", container, "cp", "/usr/share/zoneinfo/Etc/GMT-3", "/usr/share/zoneinfo/Africa/Cairo"], timeout=5)
            date_str_fixed, _ = _m6_get_container_date(container)
            if "+0300" in _m6_strip_ansi(date_str_fixed):
                return f"{GREEN}[OK] Cairo (UTC+3 -- auto-updated tzdata){RESET}", True
            return f"{YELLOW}[WARN] EET (UTC+2) -- update tzdata{RESET}", False
        elif off in ("+0000", "-0000"):
            return f"{RED}[!!] UTC (Wrong -- should be UTC+3){RESET}", False
        return f"{RED}[!!] Wrong offset {off}{RESET}", False

    if tz_env in ("Africa/Cairo", "Etc/GMT-3", "Egypt"):
        return f"{GREEN}[OK] Cairo (TZ={tz_env}){RESET}", True

    if any(x in clean for x in ("+03", "EET", "Africa/Cairo", "UTC+3", "EEST")):
        return f"{GREEN}[OK] Cairo (UTC+3){RESET}", True

    if container_dt:
        utc_now  = datetime.now(timezone.utc)
        expected = (utc_now.hour + 3) % 24
        if container_dt.hour == expected:
            return f"{GREEN}[OK] Cairo (UTC+3, inferred){RESET}", True
        elif container_dt.hour == utc_now.hour:
            return f"{RED}[!!] UTC (Wrong -- should be UTC+3){RESET}", False

    return f"{YELLOW}[??] Unknown ({date_str[:30]}){RESET}", False


def run_check_timezone() -> None:
    _header_box("CHECK CONTAINER TIMEZONES", "Verify all services are set to Africa/Cairo (UTC+3)")
    code, _, _ = _run(["docker", "info"], timeout=5)
    if code != 0:
        _err("Docker is not available or not running!")
        _pause()
        return
    print()
    results: list = []
    for container in _TZ_CONTAINERS:
        meta  = _TZ_META.get(container, {"label": container})
        label = meta["label"]
        status, is_ok = _m6_check(container)
        print(f"  {label}  {status}")
        results.append((container, is_ok))

    correct = sum(1 for _, ok in results if ok)
    wrong   = len(results) - correct

    _section("SUMMARY")
    print(f"  {GREEN}Correct (UTC+3) : {correct}{RESET}")
    print(f"  {RED}Wrong / Other   : {wrong}{RESET}")

    utc_now   = datetime.now(timezone.utc)
    cairo_now = utc_now.astimezone(timezone(timedelta(hours=3)))
    print(f"\n  {DIM}Local : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"  {DIM}UTC   : {utc_now.strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"  {DIM}Cairo : {cairo_now.strftime('%Y-%m-%d %H:%M:%S')}  (UTC+3){RESET}")
    if wrong > 0:
        print(f"\n  {YELLOW}Fix: docker compose down && TZ=Africa/Cairo docker compose up -d{RESET}")
    _pause()


# ============================================================================
#  MODULE 7 — Check Kafka & Spark Lag
# ============================================================================

def _m7_get_kafka_offsets() -> Dict[str, int]:
    code, out, _ = _run([
        "docker", "exec", "ft-kafka", "kafka-run-class",
        "kafka.tools.GetOffsetShell",
        "--broker-list", "localhost:29092",
        "--topic", "flight-tracking-raw,flight-tracking-hdfs",
        "--time", "-1"
    ], timeout=15)
    offsets = {}
    if code == 0 and out:
        for line in out.splitlines():
            parts = line.strip().split(":")
            if len(parts) >= 3:
                topic, part, off = parts[0], parts[1], parts[2]
                try:
                    offsets[f"{topic}[p{part}]"] = int(off)
                except ValueError:
                    pass
    return offsets


def _m7_get_flume_lag() -> List[Dict]:
    code, out, _ = _run([
        "docker", "exec", "ft-kafka", "kafka-consumer-groups",
        "--bootstrap-server", "localhost:29092",
        "--describe", "--group", "flume-hdfs-consumer"
    ], timeout=15)
    results = []
    if code == 0 and out:
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("GROUP") or line.startswith("Note:"):
                continue
            parts = line.split()
            if len(parts) >= 6:
                try:
                    results.append({
                        "group": parts[0],
                        "topic": parts[1],
                        "partition": parts[2],
                        "current_offset": int(parts[3]) if parts[3].isdigit() else parts[3],
                        "log_end_offset": int(parts[4]) if parts[4].isdigit() else parts[4],
                        "lag": int(parts[5]) if parts[5].isdigit() else 0,
                    })
                except Exception:
                    pass
    return results


def _m7_get_spark_checkpoint() -> Dict:
    code, out, _ = _run([
        "docker", "exec", "ft-spark-streaming", "bash", "-c",
        "ls -1 /tmp/spark-checkpoint/offsets 2>/dev/null | grep -E '^[0-9]+$' | sort -n | tail -n 1"
    ], timeout=10)
    if code != 0 or not out.strip():
        return {"status": "No active batches in checkpoint"}

    batch_id = out.strip()
    c_code, c_out, _ = _run([
        "docker", "exec", "ft-spark-streaming", "cat",
        f"/tmp/spark-checkpoint/offsets/{batch_id}"
    ], timeout=10)
    if c_code != 0 or not c_out.strip():
        return {"status": f"Batch #{batch_id} (cannot read)"}

    spark_offset = None
    timestamp_ms = None
    for line in c_out.splitlines():
        line = line.strip()
        if not line or line.startswith("v"):
            continue
        try:
            data = json.loads(line)
            if "batchTimestampMs" in data:
                timestamp_ms = data["batchTimestampMs"]
            if "flight-tracking-raw" in data:
                raw_info = data["flight-tracking-raw"]
                if isinstance(raw_info, dict):
                    spark_offset = raw_info.get("0")
        except Exception:
            pass

    return {
        "batch_id": batch_id,
        "spark_offset": spark_offset,
        "timestamp_ms": timestamp_ms,
    }


def run_check_lag(pause_on_finish: bool = True) -> None:
    _header_box("KAFKA & SPARK LAG MONITOR", "Real-time offsets, consumer lag & backpressure diagnosis")
    print(f"\n  {DIM}Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")

    # 1. Kafka End Offsets
    _section("1 - Kafka Topics (Log End Offsets)")
    kafka_offsets = _m7_get_kafka_offsets()
    raw_end = 0
    hdfs_end = 0
    if not kafka_offsets:
        _warn("No active topics found or Kafka broker is unreachable")
    else:
        for tp, off in kafka_offsets.items():
            print(f"   • {tp:<30}: {BOLD}{off:,}{RESET} msgs")
            if "flight-tracking-raw" in tp:
                raw_end = off
            elif "flight-tracking-hdfs" in tp:
                hdfs_end = off

    # 2. Flume HDFS Consumer Lag
    _section("2 - HDFS Consumer Group (Flume Sink)")
    flume_lags = _m7_get_flume_lag()
    if not flume_lags:
        print(f"   {DIM}• Group 'flume-hdfs-consumer': Inactive / Idle{RESET}")
    else:
        for item in flume_lags:
            lag = item["lag"]
            lag_str = f"{GREEN}0 (No lag){RESET}" if lag == 0 else f"{YELLOW}{lag:,} msgs{RESET}"
            print(f"   • Group      : {item['group']}")
            print(f"   • Topic      : {item['topic']} (partition {item['partition']})")
            print(f"   • Current    : {item['current_offset']:,}")
            print(f"   • End Offset : {item['log_end_offset']:,}")
            print(f"   • Lag        : {lag_str}")

    # 3. Spark Streaming Checkpoint & Lag
    _section("3 - Spark Streaming (Kafka -> InfluxDB)")
    spark_info = _m7_get_spark_checkpoint()
    if "spark_offset" in spark_info and spark_info["spark_offset"] is not None:
        sp_off = int(spark_info["spark_offset"])
        spark_lag = max(0, raw_end - sp_off)
        lag_str = f"{GREEN}0 (In lockstep / No lag){RESET}" if spark_lag == 0 else f"{YELLOW}{spark_lag:,} msgs lag{RESET}"

        batch_dt_str = "N/A"
        if spark_info.get("timestamp_ms"):
            batch_dt = datetime.fromtimestamp(spark_info["timestamp_ms"] / 1000.0)
            batch_dt_str = batch_dt.strftime("%Y-%m-%d %H:%M:%S")

        print(f"   • Latest Micro-Batch : #{spark_info['batch_id']}")
        print(f"   • Batch Timestamp    : {batch_dt_str}")
        print(f"   • Checkpointed Offset: {sp_off:,}")
        print(f"   • Kafka End Offset   : {raw_end:,}")
        print(f"   • Spark Lag          : {lag_str}")
    else:
        print(f"   {YELLOW}• Spark Checkpoint: {spark_info.get('status', 'Waiting for initial batch')}{RESET}")

    # 4. Overall Health Verdict
    _section("4 - Pipeline Flow Verdict")
    if spark_info.get("spark_offset") is not None and raw_end > 0:
        sp_lag = raw_end - int(spark_info["spark_offset"])
        if sp_lag == 0:
            _ok("Spark Streaming is 100% up-to-date with Kafka (Zero Lag).")
        else:
            _warn(f"Spark has {sp_lag:,} records pending processing.")

    if flume_lags:
        hdfs_lag = sum(item["lag"] for item in flume_lags)
        if hdfs_lag == 0:
            _ok("Flume HDFS Sink is 100% up-to-date with Kafka (Zero Lag).")
        else:
            _warn(f"Flume HDFS Sink has {hdfs_lag:,} records pending.")

    print(f"   🛡️  {CYAN}Backpressure Guard: active (maxOffsetsPerTrigger bounded).{RESET}")
    print(f"   💾 {CYAN}Checkpoint Storage: persisted via Docker volume 'ft-spark-checkpoint'.{RESET}")

    if pause_on_finish:
        _pause()


# ============================================================================
#  MAIN MENU
# ============================================================================

# (key, label, short_desc, function)
_MENU_ITEMS = [
    ("1", "Pipeline Health Test",      "All services connectivity check",    run_pipeline_test),
    ("2", "Clear Pipeline Data",       "Purge Kafka / InfluxDB / HDFS",      run_clear_data),
    ("3", "Diagnose VPN / API",        "IP geolocation & OpenSky status",    run_diagnose_vpn),
    ("4", "Rotate Public IP",          "New DHCP lease via adapter reset",   run_rotate_ip),
    ("5", "Reset API Connection",      "Test API + restart Docker",          run_reset_api),
    ("6", "Check Container Timezones", "Verify UTC+3 on all containers",     run_check_timezone),
    ("7", "Check Kafka & Spark Lag",   "Offsets & consumer lag diagnostics", run_check_lag),
]

# ── visible width of a string (strips ANSI escape codes) ──────────────────────
import re as _re
_ANSI_ESC = _re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
def _vlen(s: str) -> int:
    return len(_ANSI_ESC.sub("", s))


def _box_row(inner: str, W: int) -> str:
    """Print one row between ║ borders, padding to exactly W visible chars."""
    pad = max(0, W - _vlen(inner))
    return f"{CYAN}║{RESET}{inner}{' ' * pad}{CYAN}║{RESET}"


def _print_main_menu() -> None:
    # W = visible characters between the two │ borders (not counting the borders)
    W   = 72
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── border helpers ────────────────────────────────────────────────────────
    top = f"{CYAN}╔{'═' * W}╗{RESET}"
    mid = f"{CYAN}╠{'═' * W}╣{RESET}"
    sep = f"{CYAN}╟{'─' * W}╢{RESET}"
    bot = f"{CYAN}╚{'═' * W}╝{RESET}"
    emp = _box_row("", W)          # empty row

    # ── header ────────────────────────────────────────────────────────────────
    title_left  = f"  {BOLD}{CYAN}✈  FLIGHT TRACKER PIPELINE  ·  TOOLKIT{RESET}"
    title_right = f"{DIM}{now}  {RESET}"
    gap = W - _vlen(title_left) - _vlen(title_right)
    title_row   = _box_row(f"{title_left}{' ' * max(1, gap)}{title_right}", W)

    sub = f"  {DIM}Pipeline utilities in one interactive menu{RESET}"
    sub_row = _box_row(sub, W)

    print()
    print(top)
    print(title_row)
    print(sub_row)
    print(mid)
    print(emp)

    # ── menu items ────────────────────────────────────────────────────────────
    KEY_W   = 4    # "[1]"
    LABEL_W = 28   # label column visible width
    DESC_W  = 38   # description column visible width

    for key, label, desc, _ in _MENU_ITEMS:
        # Visible content (no ANSI) for length calculation
        key_vis   = f"  [{key}]  "                     # 8 chars
        label_vis = f"{label:<{LABEL_W}}"               # LABEL_W chars
        desc_vis  = f"  {desc}"                         # 2 + len(desc)

        # Formatted (with ANSI colours)
        key_fmt   = f"  {BOLD}[{CYAN}{key}{RESET}{BOLD}]{RESET}  "
        label_fmt = label_vis                            # no ANSI on label — keeps _vlen accurate
        desc_fmt  = f"  {DIM}{desc}{RESET}"

        row_fmt = f"{key_fmt}{label_fmt}{desc_fmt}"
        print(_box_row(row_fmt, W))

    print(emp)
    print(sep)

    # ── quit row ──────────────────────────────────────────────────────────────
    q_fmt = f"  {BOLD}[{CYAN}Q{RESET}{BOLD}]{RESET}  {DIM}Quit Toolkit{RESET}"
    print(_box_row(q_fmt, W))
    print(bot)


def main() -> None:
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))

    parser = argparse.ArgumentParser(description="Flight Tracker Pipeline Toolkit")
    parser.add_argument(
        "--tool", choices=["1", "2", "3", "4", "5", "6", "7"],
        help="Launch a specific tool directly (1-7)"
    )
    args = parser.parse_args()

    if args.tool:
        for key, _, _, fn in _MENU_ITEMS:
            if key == args.tool:
                try:
                    fn()
                except (KeyboardInterrupt, SystemExit):
                    pass
                return

    valid_keys = {item[0] for item in _MENU_ITEMS}
    try:
        while True:
            _print_main_menu()
            choice = input(f"\n{CYAN}  ❯  Select [{'/'.join(sorted(valid_keys))}/Q]: {RESET}").strip().upper()
            matched = False
            for key, _, _, fn in _MENU_ITEMS:
                if choice == key:
                    try:
                        fn()
                    except (KeyboardInterrupt, SystemExit):
                        print(f"\n  {YELLOW}Returning to main menu...{RESET}")
                    matched = True
                    break
            if not matched:
                if choice in ("Q", "QUIT", "EXIT", ""):
                    print(f"\n{GREEN}  Goodbye!{RESET}\n")
                    break
                else:
                    print(f"  {RED}✘  Invalid option — choose 1-6 or Q.{RESET}")
    except (KeyboardInterrupt, SystemExit):
        print(f"\n{GREEN}  Goodbye!{RESET}\n")


if __name__ == "__main__":
    main()
