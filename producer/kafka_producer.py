import os
import time
import json
import logging
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("eonet-producer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "vm1:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "nasa-eonet-raw")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
REPLAY_DAYS = int(os.getenv("REPLAY_HISTORICAL_DAYS", "30"))

EONET_URL = "https://eonet.gsfc.nasa.gov/api/v3/events"
POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

# Caches to prevent duplicate messages and respect API rate limits
seen_record_ids = set()
weather_cache = {}


def normalize_utc(iso_str: str | None) -> str | None:
    if not iso_str:
        return None
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_nasa_power(lat: float, lon: float, event_time_utc: str) -> dict:
    """Query NASA POWER daily point weather with 0.5-degree spatial + date caching."""
    date_key = event_time_utc[:10].replace("-", "")  # YYYYMMDD
    cache_key = (round(lat * 2) / 2, round(lon * 2) / 2, date_key)
    if cache_key in weather_cache:
        return weather_cache[cache_key]

    default_weather = {
        "temp_2m_c": None,
        "humidity_2m_pct": None,
        "wind_10m_ms": None,
        "precip_mm_day": None,
    }
    params = {
        "parameters": "T2M,RH2M,WS10M,PRECTOTCORR",
        "community": "AG",
        "longitude": round(lon, 4),
        "latitude": round(lat, 4),
        "start": date_key,
        "end": date_key,
        "format": "JSON",
        "time-standard": "UTC",
    }
    try:
        resp = requests.get(POWER_URL, params=params, timeout=12)
        if resp.status_code == 200:
            p = resp.json().get("properties", {}).get("parameter", {})
            def clean(val):
                return None if val is None or val <= -900 else float(val)

            result = {
                "temp_2m_c": clean(p.get("T2M", {}).get(date_key)),
                "humidity_2m_pct": clean(p.get("RH2M", {}).get(date_key)),
                "wind_10m_ms": clean(p.get("WS10M", {}).get(date_key)),
                "precip_mm_day": clean(p.get("PRECTOTCORR", {}).get(date_key)),
            }
            weather_cache[cache_key] = result
            return result
    except Exception as exc:
        logger.warning(f"NASA POWER lookup skipped for {cache_key}: {exc}")

    weather_cache[cache_key] = default_weather
    return default_weather


def create_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
            )
            logger.info(f"Connected to Kafka at {BOOTSTRAP_SERVERS}")
            return producer
        except Exception as exc:
            logger.error(f"Kafka unavailable ({exc}); retrying in 5s...")
            time.sleep(5)


def poll_and_publish(producer: KafkaProducer, status: str = "open", days: int | None = None, limit: int = 200):
    params = {"status": status, "limit": limit}
    if days:
        params["days"] = days

    resp = requests.get(EONET_URL, params=params, timeout=20)
    resp.raise_for_status()
    events = resp.json().get("events", [])
    sent_count = 0

    for ev in events:
        event_id = ev.get("id")
        title = ev.get("title", "Unknown")
        categories = ev.get("categories", [])
        category_id = categories[0]["id"] if categories else "unknown"
        sources = ev.get("sources", [])
        source_id = sources[0]["id"] if sources else None
        closed_utc = normalize_utc(ev.get("closed"))
        ev_status = "closed" if closed_utc else "open"

        geometries = ev.get("geometry", [])
        if not event_id or not geometries:
            continue

        # Sort geometries chronologically to find first_seen_utc
        valid_geos = [g for g in geometries if g.get("date") and g.get("coordinates")]
        if not valid_geos:
            continue
        valid_geos.sort(key=lambda g: g["date"])
        first_seen_utc = normalize_utc(valid_geos[0]["date"])

        for geo in valid_geos:
            event_time_utc = normalize_utc(geo["date"])
            record_id = f"{event_id}_{event_time_utc}"
            if record_id in seen_record_ids:
                continue

            coords = geo["coordinates"]
            # Handle Point [lon, lat] or Polygon [[ [lon, lat], ... ]]
            if isinstance(coords[0], (int, float)):
                lon, lat = float(coords[0]), float(coords[1])
            else:
                lon, lat = float(coords[0][0][0]), float(coords[0][0][1])

            weather = fetch_nasa_power(lat, lon, event_time_utc)
            ingested_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            payload = {
                "schema_version": "1.0",
                "record_id": record_id,
                "event_id": event_id,
                "title": title,
                "category_id": category_id,
                "source_id": source_id,
                "status": ev_status,
                "event_time_utc": event_time_utc,
                "first_seen_utc": first_seen_utc,
                "closed_time_utc": closed_utc,
                "ingested_at_utc": ingested_at_utc,
                "longitude": lon,
                "latitude": lat,
                "magnitude_value": float(geo["magnitudeValue"]) if geo.get("magnitudeValue") is not None else None,
                "magnitude_unit": geo.get("magnitudeUnit"),
                **weather,
            }

            producer.send(TOPIC, key=event_id, value=payload)
            seen_record_ids.add(record_id)
            sent_count += 1

    producer.flush()
    logger.info(f"Poll status={status}: published {sent_count} new records to '{TOPIC}'.")


def main():
    producer = create_producer()
    backoff = 5

    # One-time historical replay on startup so Kafka & HDFS immediately have rich training data
    try:
        logger.info(f"Running initial historical replay (last {REPLAY_DAYS} days closed events)...")
        poll_and_publish(producer, status="closed", days=REPLAY_DAYS, limit=300)
    except Exception as exc:
        logger.warning(f"Initial historical replay warning: {exc}")

    # Continuous live polling loop
    while True:
        try:
            poll_and_publish(producer, status="open", limit=200)
            backoff = 5
            time.sleep(POLL_INTERVAL)
        except Exception as exc:
            logger.error(f"Polling error: {exc}. Backing off for {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)


if __name__ == "__main__":
    main()
