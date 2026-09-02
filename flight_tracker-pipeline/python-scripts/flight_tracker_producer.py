#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flight Tracking Producer - Real-time Flight Data Pipeline
Fetches data from OpenSky API and forwards it to Apache Flume HTTP Source.

Architecture:
                       [ OpenSky API (Web Server) ]
                                    │
                                    ▼
                          [ Producer (Python) ]
                                    │ (HTTP POST on Port 44444)
                                    ▼
                         [ Flume Collector Agent ]
                                    │
                                    ▼
                           [ Kafka Broker ]
                            ╱            ╲
                           ╱              ╲
                          ▼                ▼
                  [ Flume HDFS ]    [ Spark Streaming ]
                        │                  │
                        ▼                  ▼
                  [ Hadoop HDFS ]     [ InfluxDB ]
                                           │
                                           ▼
                                      [ Grafana ]

Usage:
    python flight_tracker_producer.py
"""

import json
import os
import signal
import subprocess
import sys
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# Force UTF-8 output on Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Try importing colorama for Windows terminal color support
try:
    import colorama
    colorama.init(autoreset=True)
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    BOLD = "\033[1m"
    RESET = "\033[0m"
except ImportError:
    GREEN = YELLOW = RED = CYAN = BLUE = MAGENTA = BOLD = RESET = ""


# --------------------------------------------------------------------------
# Configuration & Constants
# --------------------------------------------------------------------------
OPENSKY_API_URL = "https://opensky-network.org/api/states/all"
FLUME_HTTP_URL = os.environ.get("FLUME_HTTP_URL", "http://localhost:44444")

KAFKA_TOPIC_MAIN = "flight-tracking"
KAFKA_TOPIC_INFLUX_RAW = "flight-tracking-raw"
KAFKA_TOPIC_HDFS_RAW = "flight-tracking-hdfs"

# OpenSky rate limits — tuned after credentials load (see below)
FETCH_INTERVAL_SECONDS = 15
FETCH_INTERVAL_MIN = 15
FETCH_INTERVAL_MAX = 120

# Smart Guard: after repeated 429s, probe slowly and serve cached data
GUARD_MODE_AFTER_429S = 2
GUARD_PROBE_INTERVAL = 120       # seconds between API probes while blocked
CACHE_TTL_LIVE = 90.0            # fresh cache window during normal operation
CACHE_TTL_GUARD = 3600.0         # stale cache OK for up to 1 hour in guard mode
CACHE_FILE = os.path.join(os.path.dirname(__file__), "data", "opensky_cache.json")

# Persistent state file – stores last cycle number so "keep" can continue
STATE_FILE = os.path.join(os.path.dirname(__file__), "data", "producer_state.json")

# Zero-padding width for cycle numbers (filenames + JSON body sort correctly)
CYCLE_PAD = 6  # supports up to 999 999 cycles; e.g. 000001, 000042, 001000

# Global flag for graceful shutdown
running = True


# --------------------------------------------------------------------------
# Credential Loading
# --------------------------------------------------------------------------
# --------------------------------------------------------------------------
# Persistent State (cycle counter)
# --------------------------------------------------------------------------
def _load_last_cycle() -> dict:
    """Load last cycles from local state file. Returns dict with influx/hdfs cycles."""
    result = {"influx": 0, "hdfs": 0}
    # Priority 1: local state file
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Support new format (separate per store) and old format (single value)
                if "influx_cycle" in data or "hdfs_cycle" in data:
                    result["influx"] = int(data.get("influx_cycle", 0))
                    result["hdfs"]   = int(data.get("hdfs_cycle", 0))
                elif "last_cycle" in data:
                    # Old format: use same value for both
                    val = int(data.get("last_cycle", 0))
                    result["influx"] = val
                    result["hdfs"]   = val
                return result
    except (json.JSONDecodeError, IOError, OSError, ValueError, TypeError):
        pass

    # Priority 2: HDFS scan (offline/both mode)
    try:
        r = subprocess.run(
            ["docker", "exec", "ft-hadoop-namenode", "hdfs", "dfs", "-ls", "/flight-data/"],
            capture_output=True, text=True, timeout=8,
        )
        if r.returncode == 0:
            max_cycle = 0
            for line in r.stdout.splitlines():
                if "/flight-data/" in line:
                    matches = re.findall(r'\b\d{6}\b', line)
                    for m in matches:
                        try:
                            val = int(m)
                            if val > max_cycle:
                                max_cycle = val
                        except ValueError:
                            pass
            if max_cycle > 0:
                result["hdfs"] = max_cycle
    except Exception:
        pass
    return result


def _get_real_cycle_from_sources() -> dict:
    """
    Detect the actual last cycle by scanning real data sources directly.
    Returns dict with influx/hdfs cycle numbers detected from live sources.
    Falls back to producer_state.json if sources are unreachable.

    Priority:
      1. HDFS file scan  → max cycle number from flights_cycle-NNNNNN.log filenames
      2. InfluxDB query  → max cycle field value in flight-metrics bucket
      3. producer_state.json → stored cycle numbers (fallback)
    """
    result = {"influx": 0, "hdfs": 0}

    # ── 1. HDFS: scan actual filenames ──────────────────────────────────────
    try:
        r = subprocess.run(
            ["docker", "exec", "ft-hadoop-namenode",
             "hdfs", "dfs", "-ls", "-R", "/flight-data/"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0:
            max_c = 0
            for line in r.stdout.splitlines():
                m = re.search(r'flights_cycle-(\d+)\.', line)
                if m:
                    val = int(m.group(1))
                    if val > max_c:
                        max_c = val
            if max_c > 0:
                result["hdfs"] = max_c
    except Exception:
        pass

    # ── 2. InfluxDB: query max cycle from stored records ────────────────────
    try:
        import requests as _req
        query = (
            'from(bucket: "flight-metrics")'
            ' |> range(start: -30d)'
            ' |> filter(fn: (r) => r["_field"] == "cycle")'
            ' |> max()'
        )
        resp = _req.post(
            "http://localhost:8086/api/v2/query",
            headers={
                "Authorization": "Token my-super-secret-admin-token",
                "Content-Type": "application/vnd.flux",
            },
            params={"org": "flight-tracking"},
            data=query,
            timeout=6,
        )
        if resp.status_code == 200:
            # Parse CSV response — value is in last column of data rows
            for line in resp.text.splitlines():
                if line.startswith(",result") or line.startswith("#"):
                    continue
                parts = line.split(",")
                if len(parts) >= 7:
                    try:
                        val = int(float(parts[-1].strip()))
                        if val > result["influx"]:
                            result["influx"] = val
                    except (ValueError, TypeError):
                        pass
    except Exception:
        pass

    # ── 3. Fallback: producer_state.json ────────────────────────────────────
    saved = _load_last_cycle()
    if result["hdfs"] == 0 and saved["hdfs"] > 0:
        result["hdfs"] = saved["hdfs"]
    if result["influx"] == 0 and saved["influx"] > 0:
        result["influx"] = saved["influx"]

    return result


def _save_last_cycle(influx_cycle: int = 0, hdfs_cycle: int = 0) -> None:
    """Persist last cycle numbers per store to local state file."""
    try:
        # Read existing values first to avoid overwriting the other store's cycle
        existing = {"influx_cycle": 0, "hdfs_cycle": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    existing["influx_cycle"] = int(data.get("influx_cycle", data.get("last_cycle", 0)))
                    existing["hdfs_cycle"]   = int(data.get("hdfs_cycle",   data.get("last_cycle", 0)))
            except Exception:
                pass
        # Only update non-zero values passed in
        if influx_cycle > 0:
            existing["influx_cycle"] = influx_cycle
        if hdfs_cycle > 0:
            existing["hdfs_cycle"] = hdfs_cycle
        existing["saved_at"] = time.time()
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(existing, f)
    except (IOError, OSError):
        pass


# --------------------------------------------------------------------------
# Credential Loading
# --------------------------------------------------------------------------
def load_credentials():
    """Load OpenSky API credentials from environment variables first, then file."""
    # ── Priority 1: environment variables (works in Docker without file mounts) ──
    env_id = os.environ.get("OPENSKY_CLIENT_ID") or os.environ.get("OPENSKY_USERNAME")
    env_secret = os.environ.get("OPENSKY_CLIENT_SECRET") or os.environ.get("OPENSKY_PASSWORD")
    if env_id and env_secret:
        return env_id, env_secret

    # ── Priority 2: credentials.json file (works locally) ──────────────────────
    possible_paths = [
        os.path.join(os.path.dirname(__file__), "..", "api", "credentials.json"),
        os.path.join(os.path.dirname(__file__), "api", "credentials.json"),
        os.path.join(os.path.dirname(__file__), "credentials.json"),
        "api/credentials.json",
        "credentials.json",
    ]
    for path in possible_paths:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    username = data.get("clientId") or data.get("username") or data.get("user")
                    password = data.get("clientSecret") or data.get("password") or data.get("pass")
                    if username and password:
                        return username, password
            except (json.JSONDecodeError, IOError, OSError):
                continue
    return None


CREDS = load_credentials()

# Tune fetch intervals based on auth (400 req/h auth, 100 req/h anonymous)
if CREDS:
    FETCH_INTERVAL_SECONDS = 12
    FETCH_INTERVAL_MIN = 12
else:
    FETCH_INTERVAL_SECONDS = 36
    FETCH_INTERVAL_MIN = 36


# --------------------------------------------------------------------------
# OAuth2 Token Manager
# --------------------------------------------------------------------------
class OpenSkyTokenManager:
    """Manages OAuth2 tokens for OpenSky API with automatic refresh."""

    def __init__(self, cid: str, csec: str):
        self.cid = cid
        self.csec = csec
        self.token: Optional[str] = None
        self.expiry: float = 0

    def get_token(self) -> Optional[str]:
        """Get valid token, refreshing if necessary."""
        if not self.token or time.time() > self.expiry:
            try:
                url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
                r = requests.post(
                    url,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self.cid,
                        "client_secret": self.csec,
                    },
                    timeout=10,
                )
                if r.status_code == 200:
                    json_data = r.json()
                    self.token = json_data.get("access_token")
                    expires_in = json_data.get("expires_in", 1800)
                    self.expiry = time.time() + expires_in - 300
                else:
                    self.token = None
            except requests.exceptions.RequestException:
                self.token = None
        return self.token


TOKEN_MANAGER = OpenSkyTokenManager(CREDS[0], CREDS[1]) if CREDS else None


# --------------------------------------------------------------------------
# Adaptive Rate Limiter
# --------------------------------------------------------------------------
class AdaptiveRateLimiter:
    """
    Tracks API response history and adapts the fetch interval intelligently.

    Strategy:
    - On success: cache response to disk, gradually decrease interval
    - On 429: enter Smart Guard — probe every 120s, serve cached data
    - Never hammer the API when IP is blocked
    - Pipeline keeps running on cached data until OpenSky unblocks
    """

    def __init__(self):
        self.current_interval: float = FETCH_INTERVAL_SECONDS
        self.consecutive_successes: int = 0
        self.consecutive_429s: int = 0
        self.blocked_until: float = 0.0
        self.guard_mode: bool = False
        self._last_response_data: Optional[Dict] = None
        self._last_response_time: float = 0.0
        self._load_disk_cache()

    def record_success(self) -> None:
        self.consecutive_successes += 1
        self.consecutive_429s = 0
        if self.guard_mode:
            self.guard_mode = False
            self.current_interval = FETCH_INTERVAL_SECONDS
            print(f"   {GREEN}Smart Guard OFF — live API restored{RESET}")
        if self.consecutive_successes % 3 == 0:
            self.current_interval = max(
                FETCH_INTERVAL_MIN, self.current_interval - 0.5
            )

    def record_rate_limit(self, retry_after: Optional[int] = None) -> None:
        self.consecutive_successes = 0
        self.consecutive_429s += 1
        if retry_after and retry_after > 0:
            wait = float(retry_after)
        elif self.guard_mode:
            wait = float(GUARD_PROBE_INTERVAL)
        else:
            wait = min(30 * (1.5 ** (self.consecutive_429s - 1)), FETCH_INTERVAL_MAX)
        self.blocked_until = time.monotonic() + wait
        self.current_interval = max(self.current_interval, GUARD_PROBE_INTERVAL)

        if self.consecutive_429s >= GUARD_MODE_AFTER_429S and not self.guard_mode:
            self.guard_mode = True
            self.current_interval = GUARD_PROBE_INTERVAL
            print(
                f"   {YELLOW}Smart Guard ON — probing every {GUARD_PROBE_INTERVAL}s, "
                f"serving cached data{RESET}"
            )

    def record_error(self) -> None:
        self.consecutive_successes = 0

    def is_blocked(self) -> bool:
        return time.monotonic() < self.blocked_until

    def seconds_blocked(self) -> float:
        return max(0.0, self.blocked_until - time.monotonic())

    def update_cache(self, data: Dict) -> None:
        self._last_response_data = data
        self._last_response_time = time.monotonic()
        self._persist_disk_cache()

    def get_cached(self, allow_stale: bool = False) -> Optional[Dict]:
        if not self._last_response_data:
            return None
        age = time.monotonic() - self._last_response_time
        ttl = CACHE_TTL_GUARD if (allow_stale or self.guard_mode) else CACHE_TTL_LIVE
        if age < ttl:
            return self._last_response_data
        return None

    def cache_age_seconds(self) -> float:
        if not self._last_response_data:
            return -1.0
        return time.monotonic() - self._last_response_time

    def _persist_disk_cache(self) -> None:
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "saved_at": time.time(),
                        "data": self._last_response_data,
                    },
                    f,
                )
        except (IOError, OSError, TypeError):
            pass

    def _load_disk_cache(self) -> None:
        try:
            if not os.path.exists(CACHE_FILE):
                return
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
            data = payload.get("data")
            saved_at = payload.get("saved_at", 0)
            if data and saved_at:
                self._last_response_data = data
                age = time.time() - float(saved_at)
                self._last_response_time = time.monotonic() - age
        except (json.JSONDecodeError, IOError, OSError, TypeError, ValueError):
            pass


RATE_LIMITER = AdaptiveRateLimiter()


# --------------------------------------------------------------------------
# Signal Handling
# --------------------------------------------------------------------------
def _handle_signal(signum, frame):
    """First Ctrl+C stops gracefully; second forces immediate exit."""
    global running
    if not running:
        print(f"\n{RED}Force quit.{RESET}")
        raise SystemExit(130)
    running = False
    print(
        f"\n{YELLOW}⏹  Stopping producer... (press Ctrl+C again to force quit){RESET}"
    )


def _interruptible_sleep(seconds: float) -> bool:
    """Sleep in short chunks so Ctrl+C is picked up quickly."""
    if seconds <= 0:
        return running
    deadline = time.monotonic() + seconds
    while running:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.25, remaining))
    return running


# --------------------------------------------------------------------------
# CLI Utility Functions
# --------------------------------------------------------------------------
def print_banner() -> None:
    """Print the startup header banner."""
    sep = "═" * 70
    print(f"\n{BOLD}{CYAN}{sep}")
    print(f"  ✈️  {BOLD}FLIGHT TRACKING PRODUCER (FLUME INGESTION EDITION){RESET}")
    print(f"  {CYAN}  Real-time flight data forwarding from OpenSky to Flume{RESET}")
    print(f"{CYAN}{sep}{RESET}")
    print(f"  🕐 Start Time       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  🌐 Flume Endpoint   : {BOLD}{FLUME_HTTP_URL}{RESET}")
    print(f"  ⏱️  Fetch Interval  : {FETCH_INTERVAL_SECONDS}s (adaptive {FETCH_INTERVAL_MIN}–{FETCH_INTERVAL_MAX}s)")
    print(f"  🛡️  Smart Guard     : ON (probe every {GUARD_PROBE_INTERVAL}s when blocked, cache fallback)")
    auth_status = (
        f"{GREEN}Authenticated ({CREDS[0]}){RESET}"
        if CREDS
        else f"{YELLOW}Anonymous (Strict Limits){RESET}"
    )
    print(f"  🔑 API Auth Status  : {auth_status}")
    print(f"{CYAN}{sep}{RESET}\n")


def print_status(label: str, ok: bool, detail: str = "") -> None:
    icon = f"{GREEN}✅ [OK]{RESET}" if ok else f"{RED}❌ [ERR]{RESET}"
    detail_str = f" {detail}" if detail else ""
    print(f"  {icon}  {label:<30}{RESET}{detail_str}")


def print_warning(label: str, detail: str = "") -> None:
    icon = f"{YELLOW}⚠️ [WARN]{RESET}"
    detail_str = f" {detail}" if detail else ""
    print(f"  {icon}  {label:<30}{RESET}{detail_str}")


# --------------------------------------------------------------------------
# Data Fetching  ← Smart, non-blocking, cache-aware
# --------------------------------------------------------------------------
def get_smart_session() -> requests.Session:
    """Create a requests session with sensible defaults and proxy support."""
    session = requests.Session()
    # Use a real client User-Agent — spoofing browsers triggers IP bans from OpenSky
    session.headers.update({
        "User-Agent": "FlightTrackerPipeline/1.0 (Python requests; flight-data-pipeline)",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    })
    
    # Read standard docker-compose proxy envs
    http_p = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    https_p = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if http_p or https_p:
        session.proxies.update({
            "http": http_p,
            "https": https_p or http_p
        })
        print(f"   {GREEN}🔒 Docker Traffic routed via Proxy: {https_p or http_p}{RESET}")
    return session


def _fetch_direct(session: requests.Session) -> Optional[requests.Response]:
    """Try direct connection to OpenSky API."""
    try:
        kwargs: Dict[str, Any] = {"timeout": 12}
        # Prefer OAuth Bearer (set on session); basic auth only as fallback
        if "Authorization" not in session.headers and CREDS:
            kwargs["auth"] = (CREDS[0], CREDS[1])
        response = session.get(
            OPENSKY_API_URL,
            **kwargs,
        )
        return response
    except requests.exceptions.RequestException as e:
        print(f"   {RED}Connection Error: {e}{RESET}")
        return None


def _serve_cached(source: str = "cache") -> Tuple[Optional[Dict], str]:
    """Return cached flight data if available."""
    cached = RATE_LIMITER.get_cached(allow_stale=True)
    if cached:
        age = RATE_LIMITER.cache_age_seconds()
        print(
            f"   {YELLOW}Serving cached data ({age:.0f}s old) — pipeline stays alive{RESET}"
        )
        return cached, source
    return None, "none"


def fetch_flight_data(session: requests.Session) -> Tuple[Optional[Dict], str]:
    """
    Fetch live flight states from OpenSky with Smart Guard rate-limit handling.

    Returns: (data_dict_or_None, source)
      source: "live" | "cache" | "none"

    Smart Guard (after repeated 429s):
      - Probes API only every GUARD_PROBE_INTERVAL seconds
      - Serves disk-backed cached data between probes
      - Auto-resumes live fetching when OpenSky unblocks
    """
    global running
    if not running:
        return None, "none"

    # ── Smart Guard: skip API call, serve cache until probe window opens ──
    if RATE_LIMITER.guard_mode and RATE_LIMITER.is_blocked():
        secs = RATE_LIMITER.seconds_blocked()
        print(
            f"   {YELLOW}Smart Guard: next probe in {secs:.0f}s "
            f"(no API spam){RESET}"
        )
        return _serve_cached("cache")

    # ── Normal backoff window ─────────────────────────────────────────────
    if RATE_LIMITER.is_blocked():
        secs = RATE_LIMITER.seconds_blocked()
        print(f"   {YELLOW}Backoff window ({secs:.0f}s) before API call...{RESET}")
        if not _interruptible_sleep(secs):
            return None, "none"

    if not running:
        return None, "none"

    response = _fetch_direct(session)

    if response is None:
        RATE_LIMITER.record_error()
        cached = _serve_cached("cache")
        if cached[0]:
            return cached
        print(f"   {RED}Connection error. Skipping cycle.{RESET}")
        return None, "none"

    if response.status_code == 200:
        data = response.json()
        RATE_LIMITER.record_success()
        RATE_LIMITER.update_cache(data)
        return data, "live"

    if response.status_code == 429:
        retry_after = None
        try:
            retry_after = int(response.headers.get("Retry-After", 0))
        except (ValueError, TypeError):
            pass

        RATE_LIMITER.record_rate_limit(retry_after)
        wait_secs = RATE_LIMITER.seconds_blocked()

        if RATE_LIMITER.guard_mode:
            print(
                f"   {RED}429 Rate-Limited — Smart Guard active.{RESET}\n"
                f"   {YELLOW}   Next probe in {wait_secs:.0f}s. "
                f"Using cached data meanwhile.{RESET}"
            )
            return _serve_cached("cache")

        print(
            f"   {RED}429 Rate-Limited — OpenSky is blocking requests.{RESET}\n"
            f"   {YELLOW}   Waiting {wait_secs:.0f}s then one retry...{RESET}"
        )
        if not _interruptible_sleep(wait_secs):
            return None, "none"

        if not running:
            return None, "none"
        response = _fetch_direct(session)

        if response is not None and response.status_code == 200:
            data = response.json()
            RATE_LIMITER.record_success()
            RATE_LIMITER.update_cache(data)
            print(f"   {GREEN}API unblocked after backoff — live data received!{RESET}")
            return data, "live"

        if response is not None and response.status_code == 429:
            RATE_LIMITER.record_rate_limit(retry_after)

        print(
            f"   {YELLOW}Still rate-limited. Smart Guard will take over.{RESET}\n"
            f"   {YELLOW}   Next probe in {RATE_LIMITER.current_interval:.0f}s.{RESET}"
        )
        return _serve_cached("cache")

    if response.status_code in (401, 403):
        print(f"   {RED}Authentication failed (HTTP {response.status_code}) — check credentials{RESET}")
        return None, "none"

    if response.status_code >= 500:
        RATE_LIMITER.record_error()
        cached = _serve_cached("cache")
        if cached[0]:
            return cached
        print(f"   {YELLOW}OpenSky server error {response.status_code}. Skipping cycle.{RESET}")
        return None, "none"

    print(f"   {YELLOW}Unexpected HTTP {response.status_code}. Skipping cycle.{RESET}")
    return None, "none"


# --------------------------------------------------------------------------
# Data Transformation
# --------------------------------------------------------------------------
def transform_flight_state(state: List, timestamp: int, cycle_num: int) -> Dict:
    """Transform OpenSky state array into a clean JSON dictionary."""
    # Indices according to OpenSky Network documentation
    ICAO24 = 0
    CALLSIGN = 1
    ORIGIN_COUNTRY = 2
    TIME_POSITION = 3
    LAST_CONTACT = 4
    LONGITUDE = 5
    LATITUDE = 6
    BARO_ALTITUDE = 7
    ON_GROUND = 8
    VELOCITY = 9
    TRUE_TRACK = 10
    VERTICAL_RATE = 11
    SENSORS = 12
    GEO_ALTITUDE = 13
    SQUAWK = 14
    SPI = 15
    POSITION_SOURCE = 16

    # Unit conversions
    altitude_m = state[BARO_ALTITUDE] if len(state) > BARO_ALTITUDE else None
    altitude_ft = altitude_m * 3.28084 if altitude_m is not None else None
    velocity_ms = state[VELOCITY] if len(state) > VELOCITY else None
    velocity_kmh = velocity_ms * 3.6 if velocity_ms is not None else None

    current_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    snapshot_time = (
        datetime.fromtimestamp(timestamp, timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
        if timestamp
        else current_time
    )

    return {
        "icao24": state[ICAO24] if len(state) > ICAO24 else None,
        "callsign": (state[CALLSIGN] or "").strip() if len(state) > CALLSIGN and state[CALLSIGN] else "UNKNOWN",
        "origin_country": state[ORIGIN_COUNTRY] if len(state) > ORIGIN_COUNTRY else None,
        "time_position": state[TIME_POSITION] if len(state) > TIME_POSITION else None,
        "last_contact": state[LAST_CONTACT] if len(state) > LAST_CONTACT else None,
        "longitude": state[LONGITUDE] if len(state) > LONGITUDE else None,
        "latitude": state[LATITUDE] if len(state) > LATITUDE else None,
        "altitude_meters": altitude_m,
        "altitude_feet": round(altitude_ft, 0) if altitude_ft is not None else None,
        "on_ground": state[ON_GROUND] if len(state) > ON_GROUND else None,
        "velocity_ms": velocity_ms,
        "velocity_kmh": round(velocity_kmh, 1) if velocity_kmh is not None else None,
        "true_track": state[TRUE_TRACK] if len(state) > TRUE_TRACK else None,
        "vertical_rate": state[VERTICAL_RATE] if len(state) > VERTICAL_RATE else None,
        "geo_altitude_meters": state[GEO_ALTITUDE] if len(state) > GEO_ALTITUDE else None,
        "squawk": state[SQUAWK] if len(state) > SQUAWK else None,
        "position_source": state[POSITION_SOURCE] if len(state) > POSITION_SOURCE else None,
        "snapshot_timestamp": snapshot_time,
        "ingestion_timestamp": current_time,
        "processing_time_ms": int(time.time() * 1000),
        "cycle": cycle_num,  # Integer for Spark filtering (zero-padding only for HDFS filenames)
    }


def transform_batch_data(raw_data: Dict, cycle_num: int) -> List[Dict]:
    """Filter and transform raw aircraft states list."""
    states = raw_data.get("states", []) or []
    timestamp = raw_data.get("time", 0)

    transformed = []
    for state in states:
        # Validate state is a list with minimum length
        if not isinstance(state, list) or len(state) < 7:
            continue

        # Filter: Skip flights without valid positions (coordinates)
        if state[5] is not None and state[6] is not None:
            try:
                flight = transform_flight_state(state, timestamp, cycle_num)
                transformed.append(flight)
            except (IndexError, TypeError, ValueError):
                continue
    return transformed


# --------------------------------------------------------------------------
# Aggregation Logic
# --------------------------------------------------------------------------
def create_aggregated_data(flights: List[Dict]) -> Dict:
    """Create aggregated real-time metrics for Flume/Kafka main topic."""
    if not flights:
        return {}

    total_flights = len(flights)
    in_air = sum(1 for f in flights if not f.get("on_ground", False))
    on_ground = total_flights - in_air

    # Only calculate altitude/velocity from airborne flights
    airborne_flights = [f for f in flights if not f.get("on_ground", False)]

    altitudes = [
        f["altitude_meters"]
        for f in airborne_flights
        if f.get("altitude_meters") is not None
    ]
    avg_altitude = sum(altitudes) / len(altitudes) if altitudes else 0
    max_altitude = max(altitudes) if altitudes else 0

    velocities = [
        f["velocity_kmh"] 
        for f in airborne_flights 
        if f.get("velocity_kmh") is not None
    ]
    avg_velocity = sum(velocities) / len(velocities) if velocities else 0
    max_velocity = max(velocities) if velocities else 0

    country_counts = {}
    for flight in flights:
        country = flight.get("origin_country")
        if country:
            country_counts[country] = country_counts.get(country, 0) + 1

    top_countries = sorted(country_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    highest = None
    if altitudes and airborne_flights:
        highest = max(
            airborne_flights,
            key=lambda x: x.get("altitude_meters") or -999999,
        )

    return {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "total_flights": total_flights,
        "in_air": in_air,
        "on_ground": on_ground,
        "avg_altitude_meters": round(avg_altitude, 0),
        "avg_altitude_feet": round(avg_altitude * 3.28084, 0) if avg_altitude else 0,
        "max_altitude_meters": round(max_altitude, 0),
        "avg_velocity_kmh": round(avg_velocity, 1),
        "max_velocity_kmh": round(max_velocity, 1),
        "top_countries": [{"country": c, "count": cnt} for c, cnt in top_countries],
        "highest_aircraft": {
            "callsign": highest.get("callsign") if highest else None,
            "altitude_meters": highest.get("altitude_meters") if highest else None,
            "altitude_feet": highest.get("altitude_feet") if highest else None,
            "country": highest.get("origin_country") if highest else None,
        }
        if highest
        else None,
        "data_quality": {
            "has_position": total_flights,
            "has_altitude": sum(1 for f in flights if f.get("altitude_meters") is not None),
            "has_velocity": sum(1 for f in flights if f.get("velocity_ms") is not None),
            "has_callsign": sum(1 for f in flights if f.get("callsign") != "UNKNOWN"),
        },
    }


# --------------------------------------------------------------------------
# Flume HTTP Forwarding
# --------------------------------------------------------------------------
def send_to_flume(events: List[Dict]) -> bool:
    """
    POST structured events list to Flume HTTP JSONHandler endpoint.

    Format required by Flume JSONHandler:
    [
        {
            "headers": {"topic": "...", "timestamp": "..."},
            "body": "<JSON-serialized string>"
        }
    ]
    """
    max_retries = 3
    retry_delay = 2  # seconds

    for attempt in range(1, max_retries + 1):
        if not running:
            return False
        try:
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                FLUME_HTTP_URL, 
                json=events, 
                headers=headers, 
                timeout=15
            )

            if response.status_code == 200:
                return True
            else:
                error_msg = f"HTTP {response.status_code}"
                if attempt < max_retries:
                    print_warning(
                        f"Forwarding to Flume (attempt {attempt}/{max_retries})",
                        error_msg,
                    )
                    if not _interruptible_sleep(retry_delay):
                        return False
                else:
                    print_status(
                        "Forwarding to Flume Agent",
                        False,
                        f"{error_msg} - {response.text[:100]}",
                    )
                    return False

        except requests.exceptions.Timeout:
            if attempt < max_retries:
                print_warning(
                    f"Flume timeout (attempt {attempt}/{max_retries})",
                    "Connection timeout - retrying...",
                )
                if not _interruptible_sleep(retry_delay):
                    return False
            else:
                print_status("Forwarding to Flume Agent", False, "Connection timeout")
                return False

        except requests.exceptions.ConnectionError:
            if attempt < max_retries:
                print_warning(
                    f"Flume connection error (attempt {attempt}/{max_retries})",
                    "Retrying...",
                )
                if not _interruptible_sleep(retry_delay):
                    return False
            else:
                print_status("Forwarding to Flume Agent", False, "Connection refused")
                return False

        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                print_warning(
                    f"Flume error (attempt {attempt}/{max_retries})", 
                    str(e)[:50]
                )
                if not _interruptible_sleep(retry_delay):
                    return False
            else:
                print_status("Forwarding to Flume Agent", False, str(e)[:100])
                return False

    return False


# --------------------------------------------------------------------------
# Docker/Hadoop/InfluxDB Helpers
# --------------------------------------------------------------------------
def _cairo_time() -> str:
    """Return current Cairo time string (UTC+3)."""
    try:
        r = subprocess.run(
            [
                "docker",
                "exec",
                "hadoop-namenode",
                "bash",
                "-c",
                "TZ='Etc/GMT-3' date '+%Y-%m-%d %H:%M:%S +03:00 (Cairo)'",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        t = r.stdout.strip()
        if t:
            return t
    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError):
        pass

    cairo = datetime.now(timezone(timedelta(hours=3)))
    return cairo.strftime("%Y-%m-%d %H:%M:%S +03:00 (Cairo)")


def _check_hdfs() -> tuple:
    """Return (is_empty, display_lines)."""
    try:
        r = subprocess.run(
            [
                "docker", "exec", "ft-hadoop-namenode",
                "hdfs", "dfs", "-ls", "/flight-data/",
            ],
            capture_output=True,
            text=True,
            timeout=8,
        )
        lines = [l for l in r.stdout.strip().splitlines() if "/flight-data/" in l]
        if lines:
            display = []
            for l in lines:
                parts = l.split()
                if len(parts) >= 8:
                    folder = parts[-1].split("/")[-1]
                    size = parts[4]
                    mod_date = f"{parts[5]} {parts[6]}"
                    display.append(
                        f"       {BOLD}{folder}{RESET}  size={size}  modified={mod_date}"
                    )
            return False, display
    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError):
        pass
    return True, []


def _check_influx() -> bool:
    """Return True if bucket 'flight-metrics' contains data."""
    try:
        # Check if bucket exists
        r = subprocess.run(
            [
                "docker", "exec", "ft-influxdb",
                "influx", "bucket", "list",
                "--host",  "http://127.0.0.1:8086",
                "--org",   "flight-tracking",
                "--token", "my-super-secret-admin-token",
            ],
            capture_output=True,
            text=True,
            timeout=8,
        )
        if "flight-metrics" not in r.stdout:
            return False
        
        # Check if bucket has data using the API instead of CLI
        # Query via HTTP API is more reliable across platforms
        try:
            import requests
            query = 'from(bucket: "flight-metrics") |> range(start: -24h) |> limit(n: 1)'
            response = requests.post(
                "http://localhost:8086/api/v2/query",
                headers={
                    "Authorization": "Token my-super-secret-admin-token",
                    "Content-Type": "application/vnd.flux",
                },
                params={"org": "flight-tracking"},
                data=query,
                timeout=5,
            )
            # If data exists, response will contain CSV with records
            return response.status_code == 200 and len(response.text.strip()) > 100
        except Exception:
            # Fallback: assume bucket has data if it exists
            return True
    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError):
        return False


def _wipe_influx() -> bool:
    """
    Drop and recreate the InfluxDB flight-metrics bucket.
    Returns True on success, False on failure.
    Uses --host http://127.0.0.1:8086 to force IPv4 (avoids [::1] IPv6 issues).
    """
    INFLUX_HOST = "http://127.0.0.1:8086"
    INFLUX_ORG  = "flight-tracking"
    INFLUX_TOK  = "my-super-secret-admin-token"
    INFLUX_BKT  = "flight-metrics"

    print(f"  {YELLOW}🗑️  Resetting InfluxDB bucket '{INFLUX_BKT}' …{RESET}")
    try:
        # Step 1: Delete existing bucket (ignore errors – may not exist)
        subprocess.run(
            [
                "docker", "exec", "ft-influxdb",
                "influx", "bucket", "delete",
                "--host",  INFLUX_HOST,
                "--name",  INFLUX_BKT,
                "--org",   INFLUX_ORG,
                "--token", INFLUX_TOK,
            ],
            capture_output=True,
            timeout=15,
        )

        # Step 2: Create fresh bucket
        r = subprocess.run(
            [
                "docker", "exec", "ft-influxdb",
                "influx", "bucket", "create",
                "--host",      INFLUX_HOST,
                "--name",      INFLUX_BKT,
                "--org",       INFLUX_ORG,
                "--retention", "0",
                "--token",     INFLUX_TOK,
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

        if r.returncode == 0:
            print(f"  {GREEN}  ✅ InfluxDB bucket recreated{RESET}")
            return True
        else:
            err = (r.stderr or r.stdout).strip()
            print(f"  {YELLOW}  ⚠️  InfluxDB wipe failed: {err}{RESET}")
            return False

    except subprocess.TimeoutExpired:
        print(f"  {YELLOW}  ⚠️  InfluxDB wipe timed out – skipped{RESET}")
        return False
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        print(f"  {YELLOW}  ⚠️  InfluxDB wipe error: {e}{RESET}")
        return False


def _wipe_hdfs():
    """Delete all data under /flight-data/ in HDFS and recreate the root folder."""
    print(f"  {YELLOW}🗑️  Wiping all HDFS flight data …{RESET}")
    HDFS_CONTAINER = "ft-hadoop-namenode"
    try:
        # Step 1: Remove entire /flight-data/ tree recursively
        subprocess.run(
            [
                "docker", "exec", HDFS_CONTAINER,
                "hdfs", "dfs", "-rm", "-r", "-f", "/flight-data/",
            ],
            capture_output=True, text=True, timeout=30,
        )

        # Step 2: Recreate empty root folder
        r = subprocess.run(
            [
                "docker", "exec", HDFS_CONTAINER,
                "hdfs", "dfs", "-mkdir", "-p", "/flight-data/",
            ],
            capture_output=True, text=True, timeout=15,
        )
        if r.returncode == 0:
            print(f"  {GREEN}  ✅ HDFS /flight-data/ cleared and recreated{RESET}")
        else:
            print(f"  {YELLOW}  ⚠️  HDFS mkdir: {r.stderr.strip()}{RESET}")
    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError) as e:
        print(f"  {YELLOW}  ⚠️  HDFS wipe failed: {e}{RESET}")


# --------------------------------------------------------------------------
# User Interaction Wizard
# --------------------------------------------------------------------------
def _ask_storage_mode() -> str:
    """
    Step 1 – Ask the user which storage backend to use.
    Returns: "online" | "offline" | "both" | "abort"
    """
    env_mode = os.environ.get("PRODUCER_STORAGE_MODE")
    if env_mode in ("online", "offline", "both", "abort"):
        print(f"  Using storage mode from environment: {env_mode}")
        return env_mode
    if not sys.stdin.isatty():
        fallback_mode = "both"
        print(f"  Non-interactive shell detected. Defaulting to storage mode: {fallback_mode}")
        return fallback_mode

    sep = "═" * 62
    print(f"\n{BOLD}{CYAN}{sep}")
    print(f"  ✈️  FLIGHT TRACKER PRODUCER  –  Cairo Time: {_cairo_time()}")
    print(f"{sep}{RESET}")
    print(f"\n  {BOLD}Where do you want to store the data?{RESET}\n")
    print(f"  {GREEN}[1] Online  (Real-time){RESET}  → InfluxDB  → Grafana Dashboard")
    print(f"  {CYAN}[2] Offline (Archive)  {RESET}  → Hadoop HDFS  (raw LOG files)")
    print(f"  {MAGENTA}[3] Both               {RESET}  → InfluxDB  +  Hadoop HDFS")
    print(f"  {YELLOW}[4] Abort              {RESET}  → exit\n")

    while True:
        try:
            c = input(f"  {BOLD}Enter choice [1/2/3/4]: {RESET}").strip()
        except (EOFError, KeyboardInterrupt):
            print(f"\n{YELLOW}Aborted.{RESET}")
            return "abort"
        if c == "1":
            print(f"\n  {GREEN}✅ Mode: Online (InfluxDB + Grafana){RESET}\n")
            return "online"
        elif c == "2":
            print(f"\n  {CYAN}✅ Mode: Offline (Hadoop HDFS){RESET}\n")
            return "offline"
        elif c == "3":
            print(f"\n  {MAGENTA}✅ Mode: Both (InfluxDB + Hadoop HDFS){RESET}\n")
            return "both"
        elif c == "4":
            print(f"\n{YELLOW}Aborted.{RESET}")
            return "abort"
        else:
            print(f"  {RED}Invalid – enter 1, 2, 3, or 4.{RESET}")


def _ask_data_action(store_label: str, is_empty: bool, display_lines: list) -> str:
    """
    Step 2 – For a given store, ask keep / delete / abort.
    Returns: "keep" | "delete" | "abort"
    """
    env_action = os.environ.get("PRODUCER_DATA_ACTION")
    if env_action in ("keep", "delete", "abort"):
        print(f"  Using data action from environment: {env_action}")
        return env_action
    if not sys.stdin.isatty():
        fallback_action = "keep"
        print(f"  Non-interactive shell detected. Defaulting to data action: {fallback_action}")
        return fallback_action

    sep = "─" * 62
    print(f"\n{BOLD}{YELLOW}{sep}")
    print(f"  🗂️  {store_label} – Existing Data")
    print(f"{sep}{RESET}")

    if is_empty:
        print(f"  {GREEN}  (empty – no previous data found){RESET}")
    else:
        for l in display_lines:
            print(f"  {l}")

    print(f"\n  {BOLD}What would you like to do with {store_label}?{RESET}\n")
    print(f"  {GREEN}[1] Keep    {RESET}– append new data on top of existing")
    print(f"  {RED}[2] Delete  {RESET}– wipe all existing data, start fresh")
    print(f"  {YELLOW}[3] Abort   {RESET}– exit\n")

    while True:
        try:
            c = input(f"  {BOLD}Enter choice [1/2/3]: {RESET}").strip()
        except (EOFError, KeyboardInterrupt):
            print(f"\n{YELLOW}Aborted.{RESET}")
            return "abort"
        if c == "1":
            if is_empty:
                print(f"  {GREEN}✅ Starting fresh (nothing to keep).{RESET}\n")
            else:
                print(f"  {GREEN}✅ Keeping existing data – will append.{RESET}\n")
            return "keep"
        elif c == "2":
            if is_empty:
                print(
                    f"  {YELLOW}ℹ️  Nothing to delete – store is already empty.{RESET}\n"
                )
                return "delete"  # treat as fresh start → resets cycle counter
            print(f"  {RED}🗑️  Deleting all data in {store_label}...{RESET}")
            return "delete"
        elif c == "3":
            print(f"\n{YELLOW}Aborted.{RESET}")
            return "abort"
        else:
            print(f"  {RED}Invalid – enter 1, 2, or 3.{RESET}")


def _startup_choice() -> dict:
    """
    Full startup wizard.
    Returns: {"use_influx": bool, "use_hdfs": bool, "start_cycle": int,
              "influx_kept": bool, "hdfs_kept": bool,
              "influx_deleted": bool, "hdfs_deleted": bool}
    Raises SystemExit on abort.

    NOTE on keep/delete semantics:
      - "kept"    → user chose to preserve existing data (or store was already
                    empty) → wiping should NEVER happen for this store again,
                    including at shutdown.
      - "deleted" → user explicitly chose to wipe existing data. The wipe
                    happens exactly ONCE, here at startup. Shutdown must NOT
                    wipe again — that would be redundant and misleading
                    (e.g. showing "wiped" twice for a single user decision).
    """
    mode = _ask_storage_mode()
    if mode == "abort":
        sys.exit(0)

    use_influx = mode in ("online", "both")
    use_hdfs = mode in ("offline", "both")

    # Track keep/delete decisions independently per store.
    # These flags are mutually exclusive per store and are the single
    # source of truth used later for cycle numbering and shutdown behavior.
    influx_kept = False
    hdfs_kept = False
    influx_deleted = False
    hdfs_deleted = False

    # InfluxDB decision
    if use_influx:
        influx_has_data = _check_influx()
        label = "InfluxDB (online)"
        action = _ask_data_action(
            label,
            is_empty=not influx_has_data,
            display_lines=[
                f"{CYAN}bucket 'flight-metrics' contains existing data{RESET}"
            ]
            if influx_has_data
            else [],
        )
        if action == "abort":
            sys.exit(0)
        if action == "delete":
            _wipe_influx()
            influx_deleted = True
            print(f"  {GREEN}✅ InfluxDB wiped – ready for fresh data.{RESET}\n")
        else:
            influx_kept = True

    # HDFS decision
    if use_hdfs:
        hdfs_empty, hdfs_lines = _check_hdfs()
        action = _ask_data_action(
            "Hadoop HDFS (offline)", is_empty=hdfs_empty, display_lines=hdfs_lines
        )
        if action == "abort":
            sys.exit(0)
        if action == "delete":
            _wipe_hdfs()
            hdfs_deleted = True
            print(f"  {GREEN}✅ HDFS wiped – ready for fresh data.{RESET}\n")
        else:
            hdfs_kept = True

    # Determine starting cycle number independently per store.
    if influx_kept or hdfs_kept:
        saved = _get_real_cycle_from_sources()

        influx_last_cycle = saved["influx"] if influx_kept else (0 if influx_deleted else saved["influx"])

        # For HDFS: already scanned in _get_real_cycle_from_sources via HDFS filenames
        if hdfs_kept:
            hdfs_last_cycle = saved["hdfs"]
        else:
            # hdfs not kept (deleted or not used) — preserve saved value if not deleted
            hdfs_last_cycle = 0 if hdfs_deleted else saved["hdfs"]

        # Use max so both storages stay in sync from their latest point
        last_cycle = max(influx_last_cycle, hdfs_last_cycle)

        if last_cycle > 0:
            if influx_kept and hdfs_kept:
                msg = f"InfluxDB #{influx_last_cycle} | HDFS #{hdfs_last_cycle}"
            elif influx_kept:
                msg = f"InfluxDB #{influx_last_cycle} (HDFS deleted/not used)"
            else:
                msg = f"HDFS #{hdfs_last_cycle} (InfluxDB deleted/not used)"
            print(f"  {CYAN}🔢 Resuming from cycle #{last_cycle} ({msg}){RESET}\n")
        else:
            print(f"  {GREEN}🔢 Starting from cycle #1 (fresh start){RESET}\n")
    else:
        # Both stores deleted — read existing state to preserve the other store's cycle
        # Never delete the file; just reset only the deleted store's counter
        saved = _get_real_cycle_from_sources()
        # Reset only the stores that were explicitly deleted
        reset_influx = influx_deleted
        reset_hdfs   = hdfs_deleted
        # Preserve the untouched store's last cycle value (for the file only)
        preserved_influx = 0 if reset_influx else saved["influx"]
        preserved_hdfs   = 0 if reset_hdfs   else saved["hdfs"]
        # Per-store last cycle: 0 for deleted stores (fresh start)
        influx_last_cycle = 0
        hdfs_last_cycle   = 0
        # Always start from 0 when all active stores were deleted
        last_cycle = 0
        # Write updated state (zeroing only deleted stores, keeping others)
        _save_last_cycle(
            influx_cycle=preserved_influx,
            hdfs_cycle=preserved_hdfs,
        )
        print(f"  {GREEN}🔢 Starting from cycle #1 (all data deleted){RESET}\n")

    return {
        "use_influx": use_influx,
        "use_hdfs": use_hdfs,
        "start_cycle": last_cycle,
        # Per-store last cycle — 0 means "start fresh for this store"
        "influx_last_cycle": influx_last_cycle if influx_kept else 0,
        "hdfs_last_cycle":   hdfs_last_cycle   if hdfs_kept   else 0,
        "influx_kept": influx_kept,
        "hdfs_kept": hdfs_kept,
        "influx_deleted": influx_deleted,
        "hdfs_deleted": hdfs_deleted,
    }


# --------------------------------------------------------------------------
# Summary Display
# --------------------------------------------------------------------------
def _print_batch_summary(aggregates: Dict) -> None:
    """Print formatted batch summary statistics."""
    print(f"\n{MAGENTA}{'   📊 BATCH SUMMARY':50}{RESET}")
    print(f"   {'─' * 50}")
    print(
        f"   ✈️  Total Flights     : {BOLD}{aggregates.get('total_flights', 0):>6,}{RESET}"
    )
    print(
        f"   🔵 In Air            : {BOLD}{aggregates.get('in_air', 0):>6,}{RESET}"
    )
    print(
        f"   🟢 On Ground         : {BOLD}{aggregates.get('on_ground', 0):>6,}{RESET}"
    )
    print(
        f"   📈 Avg Altitude      : {BOLD}{aggregates.get('avg_altitude_meters', 0):>6,.0f}{RESET} m ({aggregates.get('avg_altitude_feet', 0):,.0f} ft)"
    )
    print(
        f"   ⚡ Avg Speed         : {BOLD}{aggregates.get('avg_velocity_kmh', 0):>6,.0f}{RESET} km/h"
    )
    print(
        f"   🚀 Max Speed         : {BOLD}{aggregates.get('max_velocity_kmh', 0):>6,.0f}{RESET} km/h"
    )

    highest = aggregates.get("highest_aircraft")
    if highest:
        print(
            f"   🏆 Highest Aircraft  : {BOLD}{highest.get('callsign', 'N/A')}{RESET} ({highest.get('country', 'N/A')}) at {highest.get('altitude_meters', 0):,.0f} m"
        )

    top_countries = aggregates.get("top_countries", [])
    if top_countries:
        countries_str = ", ".join(
            [f"{c.get('country', 'N/A')}({c.get('count', 0)})" for c in top_countries[:3]]
        )
        print(f"   🌍 Top Countries     : {countries_str}")

    print(f"   {'─' * 50}")


# --------------------------------------------------------------------------
# Main Program Loop
# --------------------------------------------------------------------------
def main():
    global running

    # Setup signal handlers
    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    print_banner()

    # Step 1: Ask user before doing anything
    storage = _startup_choice()

    # Initialize requests Session
    session = get_smart_session()

    # Verify Flume Connectivity first
    print("\n🔍 Verifying Pipeline Connectivity...\n")
    flume_accessible = False
    try:
        r = requests.get(FLUME_HTTP_URL, timeout=3)
        flume_accessible = True
    except requests.exceptions.ConnectionError:
        flume_accessible = False
    except requests.exceptions.RequestException:
        flume_accessible = True  # Other errors might mean it's up but misconfigured

    if not flume_accessible:
        print_status(
            "Checking Flume HTTP Source", False, f"Cannot connect to {FLUME_HTTP_URL}"
        )
        print(f"\n{YELLOW}💡 Make sure Docker Compose services are running:{RESET}")
        print(f"   {BOLD}docker compose up -d{RESET}\n")
        sys.exit(1)

    print_status("Checking Flume HTTP Source", True, "Listening on port 44444")

    cached = RATE_LIMITER.get_cached(allow_stale=True)
    if cached:
        age = RATE_LIMITER.cache_age_seconds()
        states = len(cached.get("states", []) or [])
        print_status(
            "OpenSky Cache",
            True,
            f"{states:,} aircraft cached ({age:.0f}s old) — pipeline can run while blocked",
        )
    else:
        print_warning("OpenSky Cache", "empty — first cycle needs a successful API call")

    print(f"\n{GREEN}{BOLD}All systems ready for data ingestion!{RESET}\n")

    # Statistics tracking
    total_influx_raw_sent = 0
    total_hdfs_raw_sent = 0
    total_aggregates_sent = 0
    total_fetch_errors = 0
    cycles = storage.get("start_cycle", 0)  # Continue from last saved cycle

    # Per-store cycle tracking — use exact values from startup choice (0 = fresh start)
    influx_cycles_saved = storage.get("influx_last_cycle", 0)
    hdfs_cycles_saved   = storage.get("hdfs_last_cycle",   0)

    print(f"🚀 {BOLD}Starting Real-Time Flight Data Ingestion{RESET}")
    print(f"{YELLOW}Press Ctrl+C to gracefully stop the producer{RESET}\n")

    try:
        while running:
            # Each store has its own next cycle number
            influx_next_cycle = influx_cycles_saved + 1
            hdfs_next_cycle   = hdfs_cycles_saved   + 1
            # current_cycle used for data tagging = max of both (keeps data consistent)
            current_cycle = max(influx_next_cycle, hdfs_next_cycle)
            cycle_start = datetime.now(timezone.utc)

            # Convert to Cairo Time for display
            cairo_tz = timezone(timedelta(hours=3))
            cairo_time = datetime.now(cairo_tz)

            print(f"\n{BLUE}{'─' * 70}")
            # Build per-store cycle label
            if storage["use_influx"] and storage["use_hdfs"]:
                cycle_label = f"InfluxDB #{influx_next_cycle} | HDFS #{hdfs_next_cycle}"
            elif storage["use_influx"]:
                cycle_label = f"InfluxDB #{influx_next_cycle}"
            else:
                cycle_label = f"HDFS #{hdfs_next_cycle}"
            print(
                f"📡 INGESTION CYCLE | {cycle_label} | {cairo_time.strftime('%H:%M:%S')} Cairo Time (+03:00)"
            )
            print(f"{'─' * 70}{RESET}")

            # Update OAuth2 token if available
            if TOKEN_MANAGER:
                token = TOKEN_MANAGER.get_token()
                if token:
                    session.headers.update({"Authorization": f"Bearer {token}"})
                else:
                    session.headers.pop("Authorization", None)

            # Phase 1: Fetch Flight Data
            print(f"{CYAN}1️⃣  FETCH: Requesting flight data from OpenSky API...{RESET}")
            raw_data, fetch_source = fetch_flight_data(session)
            if not running:
                break
            if not raw_data:
                total_fetch_errors += 1
                print(f"   {YELLOW}⏸️  No data available, retrying next cycle...{RESET}")
                if not _interruptible_sleep(RATE_LIMITER.current_interval):
                    break
                continue

            states_count = len(raw_data.get("states", []) or [])
            if fetch_source == "live":
                source_label = f"{GREEN}[LIVE]{RESET}"
            elif fetch_source == "cache":
                cache_age = RATE_LIMITER.cache_age_seconds()
                source_label = f"{YELLOW}[CACHE {cache_age:.0f}s]{RESET}"
            else:
                source_label = f"{YELLOW}[NONE]{RESET}"
            interval_label = f"{RATE_LIMITER.current_interval:.0f}s"
            print(
                f"   ✅ {source_label} {BOLD}{states_count:,}{RESET} aircraft records  "
                f"| interval={interval_label}"
            )

            # Phase 2: Transform Data
            print(f"{CYAN}2️⃣  TRANSFORM: Processing flight records...{RESET}")
            flights = transform_batch_data(raw_data, current_cycle)
            if not flights:
                print(
                    f"   {YELLOW}⏸️  No valid flights found, retrying next cycle...{RESET}"
                )
                if not _interruptible_sleep(RATE_LIMITER.current_interval):
                    break
                continue

            print(f"   ✅ Transformed {BOLD}{len(flights):,}{RESET} active flights")
            if not running:
                break

            # Phase 3: Route Raw Flight Records
            cycle_str = str(current_cycle).zfill(CYCLE_PAD)  # e.g. 000001, 000042
            
            # Track success of each store independently
            influx_cycle_ok = not storage["use_influx"]  # True if not used (skip = success)
            hdfs_cycle_ok   = not storage["use_hdfs"]    # True if not used (skip = success)

            if storage["use_influx"]:
                print(
                    f"{CYAN}3️⃣  STREAM: Sending raw flight records to Flume → Kafka → Spark → InfluxDB/Grafana...{RESET}"
                )
                influx_events = []
                for flight in flights:
                    flight_event = dict(flight)
                    flight_event["influxable"] = True
                    flight_event["storage_mode"] = (
                        "both" if storage["use_hdfs"] else "online"
                    )
                    influx_events.append(
                        {
                            "headers": {
                                "topic": KAFKA_TOPIC_INFLUX_RAW,
                                "timestamp": str(flight["processing_time_ms"]),
                                "cycle": cycle_str,
                            },
                            "body": json.dumps(flight_event),
                        }
                    )

                if send_to_flume(influx_events):
                    total_influx_raw_sent += len(influx_events)
                    influx_cycle_ok = True
                    print(
                        f"   {GREEN}✅ Sent {BOLD}{len(flights):,}{RESET}{GREEN} flight records to '{KAFKA_TOPIC_INFLUX_RAW}' (InfluxDB/Grafana){RESET}"
                    )
                else:
                    print(
                        f"   {RED}❌ Failed to send raw flights to InfluxDB/Grafana topic{RESET}"
                    )
            else:
                print(
                    f"{CYAN}3️⃣  STREAM: Skipping InfluxDB/Grafana (Offline-only mode){RESET}"
                )

            if storage["use_hdfs"]:
                print(
                    f"{CYAN}3️⃣  ARCHIVE: Sending raw flight records to Flume → Kafka → HDFS...{RESET}"
                )
                hdfs_events = []
                for flight in flights:
                    flight_event = dict(flight)
                    flight_event["influxable"] = False
                    flight_event["storage_mode"] = (
                        "both" if storage["use_influx"] else "offline"
                    )
                    hdfs_events.append(
                        {
                            "headers": {
                                "topic": KAFKA_TOPIC_HDFS_RAW,
                                "timestamp": str(flight["processing_time_ms"]),
                                "cycle": cycle_str,
                            },
                            "body": json.dumps(flight_event),
                        }
                    )

                if send_to_flume(hdfs_events):
                    total_hdfs_raw_sent += len(hdfs_events)
                    hdfs_cycle_ok = True
                    print(
                        f"   {GREEN}✅ Sent {BOLD}{len(flights):,}{RESET}{GREEN} flight records to '{KAFKA_TOPIC_HDFS_RAW}' (HDFS){RESET}"
                    )
                else:
                    print(f"   {RED}❌ Failed to send raw flights to HDFS topic{RESET}")
            else:
                print(f"{CYAN}3️⃣  ARCHIVE: Skipping HDFS (Online-only mode){RESET}")

            if not running:
                break

            # Phase 4: Send Aggregated Metrics
            if storage["use_influx"]:
                print(
                    f"{CYAN}4️⃣  AGGREGATE: Computing and sending metrics → InfluxDB...{RESET}"
                )
                aggregates = create_aggregated_data(flights)
                if aggregates:
                    aggregate_event = [
                        {
                            "headers": {
                                "topic": KAFKA_TOPIC_MAIN,
                                "timestamp": str(int(time.time() * 1000)),
                            },
                            "body": json.dumps(aggregates),
                        }
                    ]

                    if send_to_flume(aggregate_event):
                        total_aggregates_sent += 1
                        print(
                            f"   {GREEN}✅ Sent aggregated metrics to '{KAFKA_TOPIC_MAIN}' (InfluxDB){RESET}"
                        )
                        _print_batch_summary(aggregates)
                    else:
                        print(f"   {RED}❌ Failed to send aggregates{RESET}")
                        influx_cycle_ok = False  # aggregate failed → influx cycle not complete
            else:
                print(
                    f"{CYAN}4️⃣  AGGREGATE: Skipping InfluxDB (Offline-only mode){RESET}"
                )
                # Still show batch summary for info
                aggregates = create_aggregated_data(flights)
                if aggregates:
                    _print_batch_summary(aggregates)

            # Cycle Complete — use adaptive interval
            cycle_duration = (datetime.now(timezone.utc) - cycle_start).total_seconds()
            wait_time = max(0, RATE_LIMITER.current_interval - cycle_duration)

            # Persist cycle per store independently — only advance if that store succeeded
            new_influx_cycle = influx_next_cycle if influx_cycle_ok else influx_cycles_saved
            new_hdfs_cycle   = hdfs_next_cycle   if hdfs_cycle_ok   else hdfs_cycles_saved

            cycle_completed_successfully = influx_cycle_ok and hdfs_cycle_ok
            if cycle_completed_successfully:
                if storage["use_influx"] and storage["use_hdfs"]:
                    done_label = f"InfluxDB #{new_influx_cycle} | HDFS #{new_hdfs_cycle}"
                elif storage["use_influx"]:
                    done_label = f"InfluxDB #{new_influx_cycle}"
                else:
                    done_label = f"HDFS #{new_hdfs_cycle}"
                print(
                    f"\n{GREEN}✅ Cycle Complete [{done_label}] "
                    f"(took {cycle_duration:.1f}s | next in {wait_time:.1f}s){RESET}"
                )
            else:
                status_parts = []
                if storage["use_influx"]:
                    status_parts.append(f"InfluxDB={'✅' if influx_cycle_ok else '❌'} #{new_influx_cycle}")
                if storage["use_hdfs"]:
                    status_parts.append(f"HDFS={'✅' if hdfs_cycle_ok else '❌'} #{new_hdfs_cycle}")
                print(
                    f"\n{YELLOW}⚠️  Cycle Partial [{' | '.join(status_parts)}] "
                    f"(took {cycle_duration:.1f}s | next in {wait_time:.1f}s){RESET}"
                )

            # Update tracking variables and persist
            cycles = max(new_influx_cycle, new_hdfs_cycle)
            influx_cycles_saved = new_influx_cycle
            hdfs_cycles_saved   = new_hdfs_cycle
            _save_last_cycle(
                influx_cycle=new_influx_cycle if storage.get("use_influx") else 0,
                hdfs_cycle=new_hdfs_cycle     if storage.get("use_hdfs")   else 0,
            )

            if wait_time > 0:
                if not _interruptible_sleep(wait_time):
                    break

    except KeyboardInterrupt:
        running = False
    finally:
        # Always persist per-store cycle on exit (covers mid-cycle interrupts)
        if cycles > 0:
            _save_last_cycle(
                influx_cycle=influx_cycles_saved if storage.get("use_influx") else 0,
                hdfs_cycle=hdfs_cycles_saved     if storage.get("use_hdfs")   else 0,
            )

    # Graceful shutdown
    if not running and cycles > 0:
        print(f"\n{YELLOW}{'─' * 70}")
        print(f"🛑 Received shutdown signal. Gracefully stopping producer...")
        print(f"{'─' * 70}{RESET}\n")

        # ── InfluxDB shutdown handling ──────────────────────────────────
        if storage.get("use_influx"):
            if storage.get("influx_kept"):
                print(
                    f"{GREEN}💾 InfluxDB data preserved (Keep mode – Grafana retains last values).{RESET}\n"
                )
            else:
                # influx_deleted: data was wiped at startup, then new data was produced
                # this session's data stays in InfluxDB — do NOT wipe again
                print(
                    f"{GREEN}💾 InfluxDB session data preserved (Delete mode – only startup data was wiped).{RESET}\n"
                )

        # ── HDFS shutdown handling ──────────────────────────────────────
        if storage.get("use_hdfs"):
            if storage.get("hdfs_kept"):
                print(
                    f"{GREEN}💾 HDFS archive data preserved (Keep mode).{RESET}\n"
                )
            else:
                # hdfs_deleted: data was wiped at startup, then new data was produced
                # this session's data stays in HDFS — do NOT wipe again
                print(
                    f"{GREEN}💾 HDFS session data preserved (Delete mode – only startup data was wiped).{RESET}\n"
                )

    # Final Summary Report
    print(f"\n{BOLD}{CYAN}{'═' * 70}")
    print(f"{'  🏁 PRODUCER EXECUTION SUMMARY':50}{RESET}")
    print(f"{CYAN}{'═' * 70}{RESET}")
    print(f"  📊 Total Ingestion Cycles       : {BOLD}{cycles}{RESET}")
    print(
        f"  📤 Influx/Grafana Records Sent  : {BOLD}{total_influx_raw_sent:,}{RESET} records"
    )
    print(
        f"  🗄️  HDFS Archive Records Sent   : {BOLD}{total_hdfs_raw_sent:,}{RESET} records"
    )
    print(
        f"  📈 Aggregate Metrics Sent       : {BOLD}{total_aggregates_sent:,}{RESET} snapshots"
    )
    print(f"  ⚠️  Fetch Errors Encountered    : {BOLD}{total_fetch_errors}{RESET}")
    print(
        f"  ⏱️  Total Duration              : {BOLD}{cycles * FETCH_INTERVAL_SECONDS:.0f}{RESET}s"
    )
    print(f"  ✅ Status                       : {GREEN}Shutdown Cleanly{RESET}")
    print(f"{CYAN}{'═' * 70}{RESET}\n")


if __name__ == "__main__":
    main()