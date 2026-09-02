#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flight Analytics - Enhanced & Production-Ready
================================================
Spark Streaming: Kafka → InfluxDB with robust error handling,
performance optimizations, and graceful degradation.

Pipeline:
    [ Kafka ] → [ Spark Streaming ] → [ InfluxDB ] → [ Grafana ]

Usage (Streaming):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 flight_analytics.py

Usage (Batch):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 flight_analytics.py --batch /path/to/flights.log
"""

import argparse
import glob
import json
import logging
import os
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple

# ── Logging Setup ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("FlightAnalytics")

# ── PySpark Environment ────────────────────────────────────────────────────
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import DataFrame, SparkSession  # type: ignore[import-untyped]
from pyspark.sql.functions import (  # type: ignore[import-untyped]
    avg,
    col,
    count,
    current_timestamp,
    desc,
    lit,
    max as spark_max,
    min as spark_min,
    stddev,
    when,
)
from pyspark.sql.types import (  # type: ignore[import-untyped]
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── InfluxDB Imports ────────────────────────────────────────────────────────
try:
    from influxdb_client import InfluxDBClient, Point  # type: ignore[import-untyped]
    from influxdb_client.client.exceptions import InfluxDBError  # type: ignore[import-untyped]
    from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions  # type: ignore[import-untyped]
except ImportError:
    logger.error("❌ influxdb-client not installed. Run: pip install influxdb-client")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

class Config:
    """Centralized configuration with validation."""

    # InfluxDB
    INFLUX_URL: str = os.getenv("INFLUX_URL", "http://influxdb:8086")
    INFLUX_TOKEN: str = os.getenv(
        "INFLUX_TOKEN", "my-super-secret-admin-token"
    )
    INFLUX_ORG: str = os.getenv("INFLUX_ORG", "flight-tracking")
    INFLUX_BUCKET: str = os.getenv("INFLUX_BUCKET", "flight-metrics")
    INFLUX_TIMEOUT: int = int(os.getenv("INFLUX_TIMEOUT", "30000"))
    INFLUX_BATCH_SIZE: int = int(os.getenv("INFLUX_BATCH_SIZE", "5000"))
    INFLUX_FLUSH_INTERVAL: int = int(os.getenv("INFLUX_FLUSH_INTERVAL", "1000"))
    INFLUX_MAX_RETRIES: int = int(os.getenv("INFLUX_MAX_RETRIES", "3"))
    INFLUX_RETRY_DELAY: float = float(os.getenv("INFLUX_RETRY_DELAY", "1.0"))

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
    )
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "flight-tracking-raw")
    KAFKA_MAX_OFFSETS_PER_TRIGGER: int = int(
        os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "15000")
    )
    KAFKA_STARTING_OFFSETS: str = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    KAFKA_FAIL_ON_DATA_LOSS: bool = (
        os.getenv("KAFKA_FAIL_ON_DATA_LOSS", "false").lower() == "true"
    )

    # Streaming
    STREAMING_TRIGGER_INTERVAL: str = os.getenv(
        "STREAMING_TRIGGER_INTERVAL", "3 seconds"
    )
    STREAMING_CHECKPOINT_LOCATION: str = os.getenv(
        "STREAMING_CHECKPOINT_LOCATION", "/tmp/spark-checkpoint"
    )

    # Analytics
    SUPERSONIC_THRESHOLD: float = float(os.getenv("SUPERSONIC_THRESHOLD", "1200"))
    # Staleness watchdog: warn if no successful write happens within this many seconds
    STALE_THRESHOLD_SECONDS: int = int(os.getenv("STALE_THRESHOLD_SECONDS", "60"))
    ARAB_COUNTRIES: frozenset = frozenset({
        "Egypt", "Saudi Arabia", "United Arab Emirates", "Jordan", "Kuwait",
        "Bahrain", "Qatar", "Oman", "Yemen", "Iraq", "Syria", "Lebanon",
        "Libya", "Tunisia", "Algeria", "Morocco", "Sudan", "Palestine"
    })
    EGYPT_COUNTRY_NAME: str = "Egypt"

    @classmethod
    def validate(cls) -> None:
        """Validate critical configuration values."""
        if not cls.INFLUX_TOKEN or cls.INFLUX_TOKEN == "my-super-secret-admin-token":
            logger.warning("⚠️  Using default InfluxDB token. Set INFLUX_TOKEN env var for production.")
        if not cls.KAFKA_TOPIC:
            raise ValueError("KAFKA_TOPIC cannot be empty")


# ═══════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def safe_cast(value: Any, cast_type: Callable, default: Any = None) -> Any:
    """Safely cast a value to a type, returning default on failure."""
    if value is None:
        return default
    try:
        return cast_type(value)
    except (ValueError, TypeError):
        return default


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    return safe_cast(value, float, default)


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    return safe_cast(value, int, default)


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def retry(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[type, ...] = (Exception,),
) -> Callable:
    """Decorator for retry logic with exponential backoff."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception: BaseException = RuntimeError(
                f"{func.__name__} failed after {max_retries} retries"
            )
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"⚠️  {func.__name__} failed (attempt {attempt + 1}/{max_retries}): {e}. "
                            f"Retrying in {current_delay}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"❌ {func.__name__} failed after {max_retries} attempts: {e}"
                        )
            raise last_exception
        return wrapper
    return decorator


@contextmanager
def influx_client_manager(url: str, token: str, org: str, timeout: int):
    """Context manager for InfluxDB client with guaranteed cleanup."""
    client = None
    try:
        client = InfluxDBClient(url=url, token=token, org=org, timeout=timeout)
        yield client
    except Exception as e:
        logger.error(f"❌ Failed to create InfluxDB client: {e}")
        raise
    finally:
        if client is not None:
            try:
                client.close()
                logger.debug("🔒 InfluxDB client closed")
            except Exception as e:
                logger.warning(f"⚠️  Error closing InfluxDB client: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# FLIGHT SCHEMA (for structured parsing)
# ═══════════════════════════════════════════════════════════════════════════

FLIGHT_SCHEMA = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("time_position", LongType(), True),
    StructField("last_contact", LongType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("altitude_meters", DoubleType(), True),
    StructField("altitude_feet", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity_ms", DoubleType(), True),
    StructField("velocity_kmh", DoubleType(), True),
    StructField("true_track", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
    StructField("geo_altitude_meters", DoubleType(), True),
    StructField("squawk", StringType(), True),
    StructField("position_source", IntegerType(), True),
    StructField("snapshot_timestamp", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("processing_time_ms", LongType(), True),
    StructField("cycle", IntegerType(), True),
])


# ═══════════════════════════════════════════════════════════════════════════
# INFLUXDB WRITER (with batching & retry)
# ═══════════════════════════════════════════════════════════════════════════

class InfluxDBWriter:
    """Robust InfluxDB writer with connection pooling, batching, and retry."""

    def __init__(self, config: Config = Config):
        self.config = config
        self._client: Optional[InfluxDBClient] = None
        self._write_api = None
        self._connected = False

    def connect(self) -> "InfluxDBWriter":
        """Initialize connection with retry."""
        try:
            self._client = InfluxDBClient(
                url=self.config.INFLUX_URL,
                token=self.config.INFLUX_TOKEN,
                org=self.config.INFLUX_ORG,
                timeout=self.config.INFLUX_TIMEOUT,
            )
            # Test connection
            health = self._client.health()
            if health.status == "pass":
                self._write_api = self._client.write_api(
                    write_options=WriteOptions(
                        batch_size=self.config.INFLUX_BATCH_SIZE,
                        flush_interval=self.config.INFLUX_FLUSH_INTERVAL,
                    )
                )
                self._connected = True
                logger.info(f"✅ Connected to InfluxDB at {self.config.INFLUX_URL}")
            else:
                raise ConnectionError(f"InfluxDB health check failed: {health.message}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to InfluxDB: {e}")
            raise
        return self

    @retry(
        max_retries=Config.INFLUX_MAX_RETRIES,
        delay=Config.INFLUX_RETRY_DELAY,
        exceptions=(InfluxDBError, ConnectionError, TimeoutError),
    )
    def write(self, points: List[Point]) -> None:
        """Write points with automatic retry."""
        if not points:
            logger.debug("No points to write")
            return
        if not self._connected or self._write_api is None:
            raise ConnectionError("InfluxDB not connected. Call connect() first.")

        try:
            self._write_api.write(bucket=self.config.INFLUX_BUCKET, record=points)
            logger.debug(f"✅ Wrote {len(points)} points to InfluxDB")
        except InfluxDBError as e:
            logger.error(f"❌ InfluxDB write error: {e}")
            raise

    def flush(self) -> None:
        """Force flush pending writes."""
        if self._write_api:
            try:
                self._write_api.flush()
            except Exception as e:
                logger.warning(f"⚠️  Flush error: {e}")

    def close(self) -> None:
        """Graceful shutdown."""
        if self._write_api:
            try:
                self._write_api.close()
            except Exception as e:
                logger.warning(f"⚠️  Error closing write API: {e}")
        if self._client:
            try:
                self._client.close()
                logger.info("🔒 InfluxDB connection closed")
            except Exception as e:
                logger.warning(f"⚠️  Error closing client: {e}")
        self._connected = False

    def __enter__(self) -> "InfluxDBWriter":
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


# ═══════════════════════════════════════════════════════════════════════════
# POINT BUILDERS
# ═══════════════════════════════════════════════════════════════════════════

class PointBuilder:
    """Builds InfluxDB points from flight data with validation."""

    @staticmethod
    def flight_detail(flight: Dict, timestamp: datetime) -> Optional[Point]:
        """Build a single flight detail point."""
        try:
            icao24 = safe_str(flight.get("icao24"), "unknown")
            callsign = safe_str(flight.get("callsign"), "UNKNOWN")
            # Extract airline from callsign (first 3 chars)
            airline = callsign[:3] if len(callsign) >= 3 and callsign != "UNKNOWN" else "Unknown"
            country = safe_str(flight.get("origin_country"), "Unknown")
            on_ground = bool(flight.get("on_ground", False))

            point = (
                Point("flight_details")
                .tag("icao24", icao24)
                .tag("callsign", callsign)
                .tag("country", country)
                .tag("airline", airline)
                .tag("on_ground", str(on_ground))
                .field("on_ground_num", 1.0 if on_ground else 0.0)
            )

            # Add optional numeric fields
            fields = {
                "altitude_m": safe_float(flight.get("altitude_meters")),
                "altitude_ft": safe_float(flight.get("altitude_feet")),
                "velocity_kmh": safe_float(flight.get("velocity_kmh")),
                "true_track": safe_float(flight.get("true_track")),
                "vertical_rate": safe_float(flight.get("vertical_rate")),
                "longitude": safe_float(flight.get("longitude")),
                "latitude": safe_float(flight.get("latitude")),
                "geo_altitude_m": safe_float(flight.get("geo_altitude_meters")),
            }

            for field_name, value in fields.items():
                if value is not None:
                    point = point.field(field_name, value)

            # Squawk as string field
            squawk = safe_str(flight.get("squawk"))
            if squawk:
                point = point.field("squawk", squawk)

            return point.time(timestamp)
        except Exception as e:
            logger.warning(f"⚠️  Failed to build flight detail point: {e}")
            return None

    @staticmethod
    def pipeline_summary(
        total: int,
        in_air: int,
        avg_alt: float,
        avg_vel: float,
        max_vel: float,
        timestamp: datetime,
    ) -> Point:
        return (
            Point("pipeline_summary")
            .field("total_flights", float(total))
            .field("in_air", float(in_air))
            .field("on_ground", float(total - in_air))
            .field("avg_altitude_meters", round(avg_alt, 1))
            .field("avg_velocity_kmh", round(avg_vel, 1))
            .field("max_velocity_kmh", round(max_vel, 1))
            .time(timestamp)
        )

    @staticmethod
    def vertical_rate_stats(climbing: int, descending: int, level: int, timestamp: datetime) -> Point:
        return (
            Point("vertical_rate_stats")
            .field("Climbing", float(climbing))
            .field("Descending", float(descending))
            .field("Level", float(level))
            .time(timestamp)
        )

    @staticmethod
    def altitude_bands(low: int, mid: int, high: int, timestamp: datetime) -> Point:
        return (
            Point("altitude_bands")
            .field("Low (< 10K ft)", float(low))
            .field("Medium (10K-30K ft)", float(mid))
            .field("High (> 30K ft)", float(high))
            .time(timestamp)
        )

    @staticmethod
    def arab_world_summary(total: int, in_air: int, timestamp: datetime) -> Point:
        return (
            Point("arab_world_summary")
            .field("total_flights", float(total))
            .field("in_air", float(in_air))
            .time(timestamp)
        )

    @staticmethod
    def arab_country_metrics(
        country: str, count: int, in_air: int, avg_alt: float, timestamp: datetime
    ) -> Point:
        return (
            Point("arab_country_metrics")
            .tag("country", country)
            .field("flight_count", float(count))
            .field("in_air", float(in_air))
            .field("avg_altitude_meters", round(avg_alt, 1))
            .time(timestamp)
        )
    
    @staticmethod
    def country_metrics(
        country: str, count: int, timestamp: datetime
    ) -> Point:
        return (
            Point("country_metrics")
            .tag("country", country)
            .field("flight_count", float(count))
            .time(timestamp)
        )

    @staticmethod
    def egypt_flight_detail(flight: Dict, timestamp: datetime) -> Optional[Point]:
        """Build a single Egypt flight detail point with dedicated measurement."""
        try:
            icao24 = safe_str(flight.get("icao24"), "unknown")
            callsign = safe_str(flight.get("callsign"), "UNKNOWN")
            airline = callsign[:3] if len(callsign) >= 3 and callsign != "UNKNOWN" else "Unknown"
            country = safe_str(flight.get("origin_country"), "Unknown")
            on_ground = bool(flight.get("on_ground", False))

            point = (
                Point("egypt_flight_detail")
                .tag("icao24", icao24)
                .tag("callsign", callsign)
                .tag("country", country)
                .tag("airline", airline)
                .tag("on_ground", str(on_ground))
                .field("on_ground_num", 1.0 if on_ground else 0.0)
            )

            fields = {
                "altitude_m": safe_float(flight.get("altitude_meters")),
                "altitude_ft": safe_float(flight.get("altitude_feet")),
                "velocity_kmh": safe_float(flight.get("velocity_kmh")),
                "true_track": safe_float(flight.get("true_track")),
                "vertical_rate": safe_float(flight.get("vertical_rate")),
                "longitude": safe_float(flight.get("longitude")),
                "latitude": safe_float(flight.get("latitude")),
                "geo_altitude_m": safe_float(flight.get("geo_altitude_meters")),
            }

            for field_name, value in fields.items():
                if value is not None:
                    point = point.field(field_name, value)

            squawk = safe_str(flight.get("squawk"))
            if squawk:
                point = point.field("squawk", squawk)

            return point.time(timestamp)
        except Exception as e:
            logger.warning(f"⚠️  Failed to build egypt flight detail point: {e}")
            return None

    @staticmethod
    def egypt_summary(total: int, in_air: int, timestamp: datetime) -> Point:
        return (
            Point("egypt_summary")
            .field("total_flights", float(total))
            .field("in_air", float(in_air))
            .time(timestamp)
        )

    @staticmethod
    def heartbeat(timestamp: datetime) -> Point:
        return (
            Point("pipeline_summary")
            .field("total_flights", 0.0)
            .field("in_air", 0.0)
            .field("on_ground", 0.0)
            .field("avg_altitude_meters", 0.0)
            .field("avg_velocity_kmh", 0.0)
            .field("max_velocity_kmh", 0.0)
            .time(timestamp)
        )


# ═══════════════════════════════════════════════════════════════════════════
# BATCH PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════

class BatchProcessor:
    """Processes a single micro-batch of flight data."""

    def __init__(self, config: Config = Config):
        self.config = config
        self.builder = PointBuilder()

    def parse_flights(self, rows: List) -> List[Dict]:
        """Parse JSON flight records with error handling."""
        flights = []
        for row in rows:
            try:
                if hasattr(row, "value"):
                    raw = row.value
                else:
                    raw = str(row)
                flight = json.loads(raw)
                if isinstance(flight, dict):
                    flights.append(flight)
            except (json.JSONDecodeError, TypeError, AttributeError) as e:
                logger.warning(f"⚠️  Skipping malformed record: {e}")
                continue
        return flights

    def compute_metrics(self, flights: List[Dict]) -> Dict:
        """Compute all metrics from parsed flights."""
        total = len(flights)
        in_air = 0
        altitudes = []
        velocities = []
        countries: Dict[str, int] = {}
        arab_flights = []
        egypt_flights = []
        # Egypt-specific distribution counters
        eg_climbing = eg_descending = eg_level = 0
        eg_low_alt = eg_mid_alt = eg_high_alt = 0

        for flight in flights:
            try:
                on_ground = bool(flight.get("on_ground", False))
                if not on_ground:
                    in_air += 1
                    alt = safe_float(flight.get("altitude_meters"))
                    if alt is not None:
                        altitudes.append(alt)
                    vel = safe_float(flight.get("velocity_kmh"))
                    if vel is not None:
                        velocities.append(vel)

                country = safe_str(flight.get("origin_country"), "Unknown")
                countries[country] = countries.get(country, 0) + 1

                if country in self.config.ARAB_COUNTRIES:
                    arab_flights.append(flight)
                if country == self.config.EGYPT_COUNTRY_NAME:
                    egypt_flights.append(flight)

                    # Vertical rate — Egypt only
                    v_rate = safe_float(flight.get("vertical_rate"), 0)
                    if v_rate is not None:
                        if v_rate > 1:
                            eg_climbing += 1
                        elif v_rate < -1:
                            eg_descending += 1
                        else:
                            eg_level += 1

                    # Altitude bands — Egypt only
                    alt_ft = safe_float(flight.get("altitude_feet"))
                    if alt_ft is not None:
                        if alt_ft < 10000:
                            eg_low_alt += 1
                        elif alt_ft < 30000:
                            eg_mid_alt += 1
                        else:
                            eg_high_alt += 1

            except Exception as e:
                logger.warning(f"⚠️  Error processing flight record: {e}")
                continue

        # Filter supersonic outliers
        valid_velocities = [v for v in velocities if v <= self.config.SUPERSONIC_THRESHOLD]

        return {
            "total": total,
            "in_air": in_air,
            "avg_alt": sum(altitudes) / len(altitudes) if altitudes else 0.0,
            "avg_vel": sum(valid_velocities) / len(valid_velocities) if valid_velocities else 0.0,
            "max_vel": max(valid_velocities) if valid_velocities else 0.0,
            "countries": countries,
            "arab_flights": arab_flights,
            "egypt_flights": egypt_flights,
            "climbing": eg_climbing,
            "descending": eg_descending,
            "level": eg_level,
            "low_alt": eg_low_alt,
            "mid_alt": eg_mid_alt,
            "high_alt": eg_high_alt,
        }

    def build_points(self, flights: List[Dict], metrics: Dict, timestamp: datetime) -> List[Point]:
        """Build all InfluxDB points for the batch."""
        points = []

        # Individual flight details
        for flight in flights:
            point = self.builder.flight_detail(flight, timestamp)
            if point:
                points.append(point)

        if not points:
            return points

        # Pipeline summary
        points.append(
            self.builder.pipeline_summary(
                metrics["total"],
                metrics["in_air"],
                metrics["avg_alt"],
                metrics["avg_vel"],
                metrics["max_vel"],
                timestamp,
            )
        )

        # Vertical rate stats
        points.append(
            self.builder.vertical_rate_stats(
                metrics["climbing"], metrics["descending"], metrics["level"], timestamp
            )
        )

        # Altitude bands
        points.append(
            self.builder.altitude_bands(
                metrics["low_alt"], metrics["mid_alt"], metrics["high_alt"], timestamp
            )
        )

        # Arab world summary
        arab_total = len(metrics["arab_flights"])
        arab_in_air = sum(
            1 for f in metrics["arab_flights"] if not bool(f.get("on_ground", False))
        )
        points.append(self.builder.arab_world_summary(arab_total, arab_in_air, timestamp))

        # Per-country Arab metrics
        arab_by_country: Dict[str, Dict] = {}
        for f in metrics["arab_flights"]:
            c = safe_str(f.get("origin_country"), "Unknown")
            if c not in arab_by_country:
                arab_by_country[c] = {"count": 0, "in_air": 0, "alts": []}
            arab_by_country[c]["count"] += 1
            if not bool(f.get("on_ground", False)):
                arab_by_country[c]["in_air"] += 1
            alt = safe_float(f.get("altitude_meters"))
            if alt is not None:
                arab_by_country[c]["alts"].append(alt)

        for country, data in arab_by_country.items():
            avg_alt = sum(data["alts"]) / len(data["alts"]) if data["alts"] else 0.0
            points.append(
                self.builder.arab_country_metrics(
                    country, data["count"], data["in_air"], avg_alt, timestamp
                )
            )
        
        # All countries metrics for Top 10
        all_by_country: Dict[str, int] = {}
        for f in flights:
            c = safe_str(f.get("origin_country"), "Unknown")
            if c not in all_by_country:
                all_by_country[c] = 0
            all_by_country[c] += 1
        
        for country, count in all_by_country.items():
            points.append(self.builder.country_metrics(country, count, timestamp))

        # Egypt summary
        egypt_total = len(metrics["egypt_flights"])
        egypt_in_air = sum(
            1 for f in metrics["egypt_flights"] if not bool(f.get("on_ground", False))
        )
        points.append(self.builder.egypt_summary(egypt_total, egypt_in_air, timestamp))
        
        # Egypt flight details
        for flight in metrics["egypt_flights"]:
            egypt_point = self.builder.egypt_flight_detail(flight, timestamp)
            if egypt_point:
                points.append(egypt_point)

        return points

    def process(self, rows: List, batch_id: int) -> Tuple[List[Point], Dict]:
        """Process a batch: parse → filter by latest cycle → compute → build points (NO deduplication, EXACTLY matches Producer)."""
        logger.info(f"\n{'='*80}")
        logger.info(f"📦 Batch #{batch_id} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*80}")

        if not rows:
            logger.info("⚠️  Empty batch – no data, skipping heartbeat")
            return [], {"total": 0}

        flights = self.parse_flights(rows)
        if not flights:
            logger.warning("⚠️  No valid flights parsed – skipping heartbeat")
            return [], {"total": 0}

        logger.info(f"✅ {len(flights)} valid records parsed (NO deduplication)")
        
        # Filter to only keep latest cycle (to match Producer's current cycle)
        cycles = [f.get("cycle") for f in flights if f.get("cycle") is not None]
        if cycles:
            latest_cycle = max(cycles)
            logger.info(f"🔄 Latest cycle in batch: {latest_cycle}")
            filtered_flights = [f for f in flights if f.get("cycle") == latest_cycle]
            logger.info(f"✅ Filtered to {len(filtered_flights)} flights from latest cycle (EXACTLY like Producer)")
        else:
            logger.warning("⚠️  No cycle numbers found in batch, using all flights")
            filtered_flights = flights
        
        # NO deduplication - use ALL filtered flights (EXACTLY like Producer)
        unique_flights = filtered_flights
        
        metrics = self.compute_metrics(unique_flights)
        
        # Sync with producer host clock via processing_time_ms to fix timezone/VM clock drift
        timestamp = None
        for f in unique_flights:
            p_time = f.get("processing_time_ms")
            if p_time:
                try:
                    timestamp = datetime.fromtimestamp(p_time / 1000.0, tz=timezone.utc)
                    break
                except Exception:
                    pass
        if not timestamp:
            timestamp = datetime.now(timezone.utc)

        points = self.build_points(unique_flights, metrics, timestamp)

        # Log summary
        logger.info(f"\n📊 Batch Summary:")
        logger.info(f"   • Total flights     : {metrics['total']}")
        logger.info(f"   • In Air            : {metrics['in_air']}  |  On Ground: {metrics['total'] - metrics['in_air']}")
        logger.info(f"   • Countries tracked : {len(metrics['countries'])}")
        if metrics.get("avg_alt"):
            logger.info(f"   • Avg altitude      : {metrics['avg_alt']:.0f} m")
        if metrics.get("max_vel"):
            logger.info(f"   • Max speed         : {metrics['max_vel']:.0f} km/h")
        logger.info(f"   • Arab world        : {len(metrics['arab_flights'])} flights  |  Egypt: {len(metrics['egypt_flights'])}")
        top3 = sorted(metrics["countries"].items(), key=lambda x: x[1], reverse=True)[:3]
        logger.info(f"   • Top countries     : {', '.join(f'{c}({n})' for c, n in top3)}")

        return points, metrics


# ═══════════════════════════════════════════════════════════════════════════
# STREAMING HANDLER
# ═══════════════════════════════════════════════════════════════════════════

def create_streaming_handler(config: Config = Config):
    """Factory for the foreachBatch handler with shared state."""
    processor = BatchProcessor(config)
    writer: Optional[InfluxDBWriter] = None
    # ── Staleness watchdog ────────────────────────────────────────────────
    # Tracks the wall-clock time of the last successful InfluxDB write.
    # If a batch fires but produces no points AND too much time has passed
    # since the last good write, we surface a loud WARNING so operators
    # know the pipeline is 'running but stale' — no exception, just silence.
    last_write_time: List[float] = [time.time()]   # list so nonlocal isn't needed
    last_stale_warn: List[float] = [0.0]

    def handler(batch_df: DataFrame, batch_id: int) -> None:
        nonlocal writer

        try:
            # Check for empty micro-batch without printing banners
            if batch_df.rdd.isEmpty():
                elapsed = time.time() - last_write_time[0]
                if elapsed > config.STALE_THRESHOLD_SECONDS:
                    now = time.time()
                    if now - last_stale_warn[0] >= 30.0:  # throttle warning to once every 30s
                        logger.warning(
                            f"🚨 STALE PIPELINE DETECTED | Batch #{batch_id} | "
                            f"No data written for {elapsed:.0f}s "
                            f"(threshold={config.STALE_THRESHOLD_SECONDS}s). "
                            "Stream is running but producing no output. "
                            "Check: Kafka lag, upstream producer, and InfluxDB health."
                        )
                        last_stale_warn[0] = now
                return

            rows = batch_df.collect()
            points, metrics = processor.process(rows, batch_id)

            if not points:
                # Fallback staleness check if rows existed but produced no valid points
                elapsed = time.time() - last_write_time[0]
                if elapsed > config.STALE_THRESHOLD_SECONDS:
                    now = time.time()
                    if now - last_stale_warn[0] >= 30.0:
                        logger.warning(
                            f"🚨 STALE PIPELINE DETECTED | Batch #{batch_id} | "
                            f"No data written for {elapsed:.0f}s "
                            f"(threshold={config.STALE_THRESHOLD_SECONDS}s). "
                            "Stream is running but producing no output. "
                            "Check: Kafka lag, upstream producer, and InfluxDB health."
                        )
                        last_stale_warn[0] = now
                return

            # Lazy init writer (survives Spark restarts)
            if writer is None or not writer._connected:
                writer = InfluxDBWriter(config).connect()

            writer.write(points)
            writer.flush()
            last_write_time[0] = time.time()   # ✅ update staleness clock
            logger.info(f"✅ Wrote {len(points)} points to InfluxDB")

        except Exception as e:
            logger.error(f"❌ Batch #{batch_id} failed: {e}")
            traceback.print_exc()
            # Don't kill the stream; Spark will retry
            raise

    return handler


def run_streaming_mode(config: Config = Config) -> None:
    """Run the Spark Streaming pipeline with robust error handling."""
    config.validate()

    logger.info("\n" + "="*80)
    logger.info("🚀 SPARK STREAMING: Kafka → InfluxDB")
    logger.info("="*80)
    logger.info(f"   Kafka topic : {config.KAFKA_TOPIC}")
    logger.info(f"   InfluxDB    : {config.INFLUX_URL}  bucket={config.INFLUX_BUCKET}")
    logger.info("="*80 + "\n")

    spark = (
        SparkSession.builder
        .appName("FlightStreaming_KafkaToInfluxDB")
        .config("spark.sql.streaming.checkpointLocation", config.STREAMING_CHECKPOINT_LOCATION)
        .config("spark.sql.streaming.minBatchesToRetain", "5")
        .config("spark.sql.streaming.pollingDelay", "100ms")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Skip Kafka test for now (we know it's working)
    logger.info("📡 Skipping Kafka connection test...")

    logger.info("📡 Starting streaming query...")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", config.KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", str(config.KAFKA_FAIL_ON_DATA_LOSS).lower())
        .option("maxOffsetsPerTrigger", config.KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as value")

    streaming_handler = create_streaming_handler(config)

    query = (
        parsed_df
        .writeStream
        .foreachBatch(streaming_handler)
        .trigger(processingTime=config.STREAMING_TRIGGER_INTERVAL)
        .outputMode("append")
        .option("checkpointLocation", config.STREAMING_CHECKPOINT_LOCATION)
        .start()
    )

    logger.info("✅ Stream query started successfully")
    logger.info("   Press Ctrl+C to stop gracefully\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("\n🛑 Stopping stream (Ctrl+C detected)...")
        query.stop()
        logger.info("✅ Stream stopped gracefully")
    finally:
        spark.stop()


# ═══════════════════════════════════════════════════════════════════════════
# BATCH MODE
# ═══════════════════════════════════════════════════════════════════════════

class FlightAnalytics:
    """Batch analytics with robust error handling and performance optimization."""

    def __init__(self, log_file_path: str, config: Config = Config):
        self.log_file_path = log_file_path
        self.config = config
        self.spark: Optional[SparkSession] = None
        self.writer: Optional[InfluxDBWriter] = None

    def initialize_spark(self) -> None:
        """Initialize Spark with optimized settings."""
        logger.info("🚀 Initializing Spark Session...")
        self.spark = (
            SparkSession.builder
            .appName("FlightAnalytics_Batch")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark initialized")

    def initialize_influx(self) -> None:
        """Initialize InfluxDB writer."""
        logger.info("📊 Connecting to InfluxDB...")
        self.writer = InfluxDBWriter(self.config).connect()
        logger.info("✅ InfluxDB connected")

    def load_flight_data(self) -> DataFrame:
        """Load flight data with schema enforcement and validation."""
        logger.info(f"📂 Loading data from: {self.log_file_path}")

        if not os.path.exists(self.log_file_path):
            raise FileNotFoundError(f"Log file not found: {self.log_file_path}")

        # Try JSON Lines with schema first
        try:
            df = self.spark.read.json(self.log_file_path, schema=FLIGHT_SCHEMA)
        except Exception:
            # Fallback: infer schema
            df = self.spark.read.json(self.log_file_path)

        count = df.count()
        if count == 0:
            raise ValueError("No data found in the log file")

        # Add computed columns
        df = (
            df.withColumn("altitude_km", col("altitude_meters") / 1000)
            .withColumn("velocity_kts", col("velocity_ms") * 1.944)
            .withColumn("is_high_altitude", col("altitude_feet") > 30000)
            .withColumn("is_fast", col("velocity_kmh") > 700)
            .withColumn("is_egypt", col("origin_country") == self.config.EGYPT_COUNTRY_NAME)
            .withColumn("timestamp", current_timestamp())
        )

        logger.info(f"✅ Loaded {count} flight records")
        return df

    def calculate_statistics(self, df: DataFrame) -> Dict:
        """Calculate comprehensive statistics with null-safety."""
        logger.info("\n📈 Calculating statistics...")
        stats = {}
        total = df.count()

        # Use cached dataframe for repeated operations
        df_cached = df.cache()

        # General stats
        flights_on_ground = df_cached.filter(col("on_ground") == True).count()
        flights_airborne = total - flights_on_ground

        stats["general"] = {
            "total_flights": total,
            "flights_airborne": flights_airborne,
            "flights_on_ground": flights_on_ground,
            "airborne_percentage": round(flights_airborne / total * 100, 2) if total > 0 else 0,
        }

        # Egypt stats
        egypt_df = df_cached.filter(col("origin_country") == self.config.EGYPT_COUNTRY_NAME)
        egypt_count = egypt_df.count()

        if egypt_count > 0:
            egypt_airborne = egypt_df.filter(col("on_ground") == False).count()
            egypt_stats_row = (
                egypt_df.filter(col("altitude_meters").isNotNull())
                .agg(avg("altitude_meters").alias("avg_alt"), spark_max("altitude_meters").alias("max_alt"))
                .collect()[0]
            )
            egypt_vel_row = (
                egypt_df.filter(col("velocity_kmh").isNotNull())
                .agg(avg("velocity_kmh").alias("avg_vel"), spark_max("velocity_kmh").alias("max_vel"))
                .collect()[0]
            )
            stats["egypt"] = {
                "total_flights": egypt_count,
                "flights_airborne": egypt_airborne,
                "flights_on_ground": egypt_count - egypt_airborne,
                "percentage_of_total": round(egypt_count / total * 100, 2),
                "avg_altitude_m": round(egypt_stats_row["avg_alt"] or 0, 2),
                "max_altitude_m": round(egypt_stats_row["max_alt"] or 0, 2),
                "avg_velocity_kmh": round(egypt_vel_row["avg_vel"] or 0, 2),
                "max_velocity_kmh": round(egypt_vel_row["max_vel"] or 0, 2),
            }
        else:
            stats["egypt"] = {k: 0 for k in [
                "total_flights", "flights_airborne", "flights_on_ground",
                "percentage_of_total", "avg_altitude_m", "max_altitude_m",
                "avg_velocity_kmh", "max_velocity_kmh"
            ]}

        # Altitude stats
        alt_stats = (
            df_cached.filter(col("altitude_meters").isNotNull())
            .agg(
                avg("altitude_meters").alias("avg"),
                spark_max("altitude_meters").alias("max"),
                spark_min("altitude_meters").alias("min"),
                stddev("altitude_meters").alias("stddev"),
            )
            .collect()[0]
        )
        stats["altitude"] = {
            "avg_meters": round(alt_stats["avg"] or 0, 2),
            "max_meters": round(alt_stats["max"] or 0, 2),
            "min_meters": round(alt_stats["min"] or 0, 2),
            "stddev_meters": round(alt_stats["stddev"] or 0, 2),
            "avg_feet": round((alt_stats["avg"] or 0) * 3.28084, 2),
            "max_feet": round((alt_stats["max"] or 0) * 3.28084, 2),
        }

        # Velocity stats
        vel_stats = (
            df_cached.filter(col("velocity_kmh").isNotNull())
            .agg(
                avg("velocity_kmh").alias("avg"),
                spark_max("velocity_kmh").alias("max"),
                spark_min("velocity_kmh").alias("min"),
                stddev("velocity_kmh").alias("stddev"),
            )
            .collect()[0]
        )
        stats["velocity"] = {
            "avg_kmh": round(vel_stats["avg"] or 0, 2),
            "max_kmh": round(vel_stats["max"] or 0, 2),
            "min_kmh": round(vel_stats["min"] or 0, 2),
            "stddev_kmh": round(vel_stats["stddev"] or 0, 2),
        }

        # Country stats (top 10)
        country_stats = (
            df_cached.groupBy("origin_country")
            .agg(
                count("*").alias("flight_count"),
                avg("velocity_kmh").alias("avg_speed"),
                avg("altitude_meters").alias("avg_altitude"),
            )
            .orderBy(desc("flight_count"))
            .limit(10)
            .collect()
        )
        stats["countries"] = [
            {
                "country": row["origin_country"] or "Unknown",
                "count": row["flight_count"],
                "avg_speed_kmh": round(row["avg_speed"] or 0, 2),
                "avg_altitude_m": round(row["avg_altitude"] or 0, 2),
            }
            for row in country_stats
        ]

        # Altitude distribution
        alt_dist = (
            df_cached.filter(col("altitude_feet").isNotNull())
            .select(
                when(col("altitude_feet") < 10000, "Low (< 10K ft)")
                .when(col("altitude_feet") < 30000, "Medium (10K-30K ft)")
                .otherwise("High (> 30K ft)")
                .alias("category")
            )
            .groupBy("category")
            .count()
            .collect()
        )
        stats["altitude_distribution"] = {row["category"]: row["count"] for row in alt_dist}

        # Speed distribution
        speed_dist = (
            df_cached.filter(col("velocity_kmh").isNotNull())
            .select(
                when(col("velocity_kmh") < 200, "Slow (< 200 km/h)")
                .when(col("velocity_kmh") < 700, "Normal (200-700 km/h)")
                .otherwise("Fast (> 700 km/h)")
                .alias("category")
            )
            .groupBy("category")
            .count()
            .collect()
        )
        stats["speed_distribution"] = {row["category"]: row["count"] for row in speed_dist}

        # Vertical movement
        vert_dist = (
            df_cached.filter(col("vertical_rate").isNotNull())
            .select(
                when(col("vertical_rate") > 1, "Climbing")
                .when(col("vertical_rate") < -1, "Descending")
                .otherwise("Level")
                .alias("movement")
            )
            .groupBy("movement")
            .count()
            .collect()
        )
        stats["vertical_movement"] = {row["movement"]: row["count"] for row in vert_dist}

        # Fastest 10
        fastest = (
            df_cached.filter(col("velocity_kmh").isNotNull())
            .orderBy(desc("velocity_kmh"))
            .limit(10)
            .select("callsign", "origin_country", "velocity_kmh", "altitude_feet")
            .collect()
        )
        stats["fastest_flights"] = [
            {
                "callsign": row["callsign"] or "UNKNOWN",
                "country": row["origin_country"] or "Unknown",
                "speed_kmh": round(row["velocity_kmh"] or 0, 2),
                "altitude_ft": round(row["altitude_feet"] or 0, 2),
            }
            for row in fastest
        ]

        # Highest 10
        highest = (
            df_cached.filter(col("altitude_meters").isNotNull())
            .orderBy(desc("altitude_meters"))
            .limit(10)
            .select("callsign", "origin_country", "altitude_meters", "velocity_kmh")
            .collect()
        )
        stats["highest_flights"] = [
            {
                "callsign": row["callsign"] or "UNKNOWN",
                "country": row["origin_country"] or "Unknown",
                "altitude_m": round(row["altitude_meters"] or 0, 2),
                "altitude_ft": round((row["altitude_meters"] or 0) * 3.28084, 2),
                "speed_kmh": round(row["velocity_kmh"] or 0, 2),
            }
            for row in highest
        ]

        # Geographic
        geo_stats = (
            df_cached.filter(col("longitude").isNotNull() & col("latitude").isNotNull())
            .agg(
                avg("longitude").alias("avg_lon"),
                avg("latitude").alias("avg_lat"),
                spark_min("longitude").alias("min_lon"),
                spark_max("longitude").alias("max_lon"),
                spark_min("latitude").alias("min_lat"),
                spark_max("latitude").alias("max_lat"),
            )
            .collect()[0]
        )
        stats["geographic"] = {
            "center_longitude": round(geo_stats["avg_lon"] or 0, 4),
            "center_latitude": round(geo_stats["avg_lat"] or 0, 4),
            "longitude_range": [
                round(geo_stats["min_lon"] or 0, 4),
                round(geo_stats["max_lon"] or 0, 4),
            ],
            "latitude_range": [
                round(geo_stats["min_lat"] or 0, 4),
                round(geo_stats["max_lat"] or 0, 4),
            ],
        }

        df_cached.unpersist()
        logger.info("✅ Statistics calculated")
        return stats

    def send_to_influxdb(self, stats: Dict, df: DataFrame) -> None:
        """Send statistics to InfluxDB with batching."""
        logger.info("\n📤 Sending data to InfluxDB...")
        if self.writer is None:
            raise RuntimeError("InfluxDB writer not initialized")

        timestamp = datetime.now(timezone.utc)
        points = []

        # Helper to add tagged points
        def add_tagged_points(measurement: str, category: str, data: Dict):
            for key, value in data.items():
                try:
                    points.append(
                        Point(measurement)
                        .tag("category", category)
                        .tag("metric", key)
                        .field("value", float(value))
                        .time(timestamp)
                    )
                except (ValueError, TypeError):
                    points.append(
                        Point(measurement)
                        .tag("category", category)
                        .tag("metric", key)
                        .field("value", str(value))
                        .time(timestamp)
                    )

        add_tagged_points("flight_statistics", "general", stats["general"])
        add_tagged_points("egypt_statistics", "egypt", stats["egypt"])
        add_tagged_points("flight_statistics", "altitude", stats["altitude"])
        add_tagged_points("flight_statistics", "velocity", stats["velocity"])

        # Country stats
        for country_data in stats["countries"]:
            points.append(
                Point("country_statistics")
                .tag("country", country_data["country"])
                .field("flight_count", country_data["count"])
                .field("avg_speed_kmh", float(country_data["avg_speed_kmh"]))
                .field("avg_altitude_m", float(country_data["avg_altitude_m"]))
                .time(timestamp)
            )

        # Distributions
        for category, count in stats["altitude_distribution"].items():
            points.append(
                Point("altitude_distribution")
                .tag("category", category)
                .field("count", count)
                .time(timestamp)
            )

        for category, count in stats["speed_distribution"].items():
            points.append(
                Point("speed_distribution")
                .tag("category", category)
                .field("count", count)
                .time(timestamp)
            )

        for movement, count in stats["vertical_movement"].items():
            points.append(
                Point("vertical_movement")
                .tag("movement", movement)
                .field("count", count)
                .time(timestamp)
            )

        # Fastest flights
        for flight in stats["fastest_flights"]:
            points.append(
                Point("fastest_flights")
                .tag("callsign", flight["callsign"])
                .tag("country", flight["country"])
                .field("speed_kmh", float(flight["speed_kmh"]))
                .field("altitude_ft", float(flight["altitude_ft"]))
                .time(timestamp)
            )

        # Highest flights
        for flight in stats["highest_flights"]:
            points.append(
                Point("highest_flights")
                .tag("callsign", flight["callsign"])
                .tag("country", flight["country"])
                .field("altitude_m", float(flight["altitude_m"]))
                .field("altitude_ft", float(flight["altitude_ft"]))
                .field("speed_kmh", float(flight["speed_kmh"]))
                .time(timestamp)
            )

        # Geographic center
        points.append(
            Point("geographic_center")
            .field("longitude", float(stats["geographic"]["center_longitude"]))
            .field("latitude", float(stats["geographic"]["center_latitude"]))
            .time(timestamp)
        )

        # Sample individual flights (first 100)
        sample = df.limit(100).collect()
        for row in sample:
            try:
                point = (
                    Point("flight_details")
                    .tag("icao24", (row["icao24"] or "unknown"))
                    .tag("callsign", (row["callsign"] or "UNKNOWN"))
                    .tag("country", (row["origin_country"] or "Unknown"))
                    .tag("on_ground", str(row["on_ground"]))
                )
                for field in ["altitude_meters", "altitude_feet", "velocity_kmh", "true_track", "vertical_rate", "longitude", "latitude"]:
                    if row[field] is not None:
                        point = point.field(field, float(row[field]))
                points.append(point.time(timestamp))
            except Exception as e:
                logger.warning(f"⚠️  Skipping sample flight point: {e}")

        logger.info(f"  Sending {len(points)} data points...")
        self.writer.write(points)
        self.writer.flush()
        logger.info("✅ Data sent successfully")

    def print_summary(self, stats: Dict) -> None:
        """Print formatted summary."""
        logger.info("\n" + "="*80)
        logger.info("📊 COMPREHENSIVE STATISTICS SUMMARY")
        logger.info("="*80)

        logger.info(f"\n🌍 General Statistics:")
        logger.info(f"  ├─ Total Flights: {stats['general']['total_flights']}")
        logger.info(f"  ├─ Airborne: {stats['general']['flights_airborne']} ({stats['general']['airborne_percentage']}%)")
        logger.info(f"  └─ On Ground: {stats['general']['flights_on_ground']}")

        if stats["egypt"]["total_flights"] > 0:
            logger.info(f"\n🇪🇬 Egypt Statistics:")
            logger.info(f"  ├─ Total: {stats['egypt']['total_flights']} ({stats['egypt']['percentage_of_total']}%)")
            logger.info(f"  ├─ Airborne: {stats['egypt']['flights_airborne']}")
            logger.info(f"  ├─ Avg Altitude: {stats['egypt']['avg_altitude_m']:,.2f} m")
            logger.info(f"  └─ Avg Speed: {stats['egypt']['avg_velocity_kmh']:,.2f} km/h")

        logger.info(f"\n✈️ Altitude: avg={stats['altitude']['avg_meters']:,.2f}m, max={stats['altitude']['max_meters']:,.2f}m")
        logger.info(f"⚡ Velocity: avg={stats['velocity']['avg_kmh']:,.2f}km/h, max={stats['velocity']['max_kmh']:,.2f}km/h")

        logger.info(f"\n🗺️ Top 5 Countries:")
        for i, country in enumerate(stats["countries"][:5], 1):
            logger.info(f"  {i}. {country['country']}: {country['count']} flights")

        logger.info("\n" + "="*80)
        logger.info("✅ Analysis Completed Successfully!")
        logger.info("="*80 + "\n")

    def run(self) -> bool:
        """Run complete batch pipeline with cleanup."""
        try:
            logger.info("\n" + "="*80)
            logger.info("🎯 STARTING FLIGHT DATA ANALYTICS (BATCH MODE)")
            logger.info("="*80 + "\n")

            self.initialize_spark()
            self.initialize_influx()
            df = self.load_flight_data()
            stats = self.calculate_statistics(df)
            self.send_to_influxdb(stats, df)
            self.print_summary(stats)
            return True

        except Exception as e:
            logger.error(f"\n❌ Error: {e}")
            traceback.print_exc()
            return False

        finally:
            if self.writer:
                try:
                    self.writer.close()
                except Exception:
                    pass
            if self.spark:
                try:
                    self.spark.stop()
                except Exception:
                    pass


def resolve_log_file() -> str:
    """Auto-detect latest log file."""
    search_paths = [
        "/opt/spark-apps/flights_cycle-*.log*",
        "/opt/spark-apps/cycle_*.log*",
        "./flights_cycle-*.log*",
        "./cycle_*.log*",
    ]
    candidates = []
    for pattern in search_paths:
        candidates.extend(glob.glob(pattern))

    if candidates:
        log_file = max(candidates, key=os.path.getmtime)
        logger.info(f"🔍 Auto-detected latest log: {os.path.basename(log_file)}")
        return log_file

    default = "/opt/spark-apps/flights_cycle-001_00-00.log"
    logger.warning(f"⚠️  No log files found, using default: {default}")
    return default


def run_batch_mode(log_file: str) -> None:
    """Run batch mode with proper exit codes."""
    analytics = FlightAnalytics(log_file)
    success = analytics.run()
    sys.exit(0 if success else 1)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Flight Analytics Pipeline (Streaming or Batch)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Streaming mode (default)
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 flight_analytics.py

  # Batch mode with auto-detection
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 flight_analytics.py --batch

  # Batch mode with specific file
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 flight_analytics.py --batch /path/to/flights.log
        """,
    )
    parser.add_argument(
        "--batch",
        metavar="FILE",
        nargs="?",
        const="auto",
        help="Run in batch mode (auto-detect or specify log file)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("🔍 Verbose logging enabled")

    if args.batch is not None:
        if args.batch == "auto":
            log_file = resolve_log_file()
        else:
            log_file = args.batch
            if not os.path.exists(log_file):
                logger.error(f"❌ Log file not found: {log_file}")
                sys.exit(1)
        logger.info(f"\n📁 Data File: {log_file}\n")
        run_batch_mode(log_file)
    else:
        run_streaming_mode()


if __name__ == "__main__":
    main()
