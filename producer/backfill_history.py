import os
import json
import time
from datetime import datetime, timedelta, timezone
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "master:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nasa-eonet-raw")
CACHE_PATH = os.path.join(os.path.dirname(__file__), ".weather_cache.json")

EONET_URL = "https://eonet.gsfc.nasa.gov/api/v3/events"
POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"


def load_cache() -> dict:
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(cache: dict) -> None:
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f)


WEATHER_CACHE = load_cache()


def snap_half_degree(val: float) -> float:
    return round(val * 2.0) / 2.0


def fetch_power_weather(lat: float, lon: float, iso_date: str) -> dict:
    ymd = iso_date[:10].replace("-", "")
    cell_key = f"{snap_half_degree(lat)}_{snap_half_degree(lon)}_{ymd}"

    if cell_key in WEATHER_CACHE:
        return WEATHER_CACHE[cell_key]

    params = {
        "parameters": "T2M,RH2M,WS10M,PRECTOTCORR",
        "community": "RE",
        "longitude": round(lon, 4),
        "latitude": round(lat, 4),
        "start": ymd,
        "end": ymd,
        "format": "JSON",
    }
    empty = {"temp_2m_c": None, "humidity_2m_pct": None, "wind_10m_ms": None, "precip_mm_day": None}

    for attempt in range(3):
        try:
            resp = requests.get(POWER_URL, params=params, timeout=20)
            if resp.status_code == 429:
                wait_s = 15 * (attempt + 1)
                print(f"[RateLimit 429] Pausing {wait_s}s for NASA POWER...")
                time.sleep(wait_s)
                continue
            resp.raise_for_status()
            props = resp.json()["properties"]["parameter"]

            def clean(v):
                return None if v in (-999, -999.0, None) else float(v)

            result = {
                "temp_2m_c": clean(props["T2M"].get(ymd)),
                "humidity_2m_pct": clean(props["RH2M"].get(ymd)),
                "wind_10m_ms": clean(props["WS10M"].get(ymd)),
                "precip_mm_day": clean(props["PRECTOTCORR"].get(ymd)),
            }
            WEATHER_CACHE[cell_key] = result
            time.sleep(0.55)
            return result
        except Exception:
            time.sleep(2 * (attempt + 1))

    WEATHER_CACHE[cell_key] = empty
    return empty


def fetch_eonet_window(start_str: str, end_str: str) -> list:
    params = {"status": "closed", "start": start_str, "end": end_str, "limit": 2000}
    resp = requests.get(EONET_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("events", [])


def main(total_days: int = 1460, chunk_days: int = 90) -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    end_dt = datetime.now(timezone.utc).date()
    start_dt = end_dt - timedelta(days=total_days)

    all_events = {}
    cursor = start_dt
    while cursor < end_dt:
        win_end = min(cursor + timedelta(days=chunk_days), end_dt)
        batch = fetch_eonet_window(cursor.isoformat(), win_end.isoformat())
        for ev in batch:
            all_events[ev["id"]] = ev
        print(f"Window {cursor} -> {win_end}: fetched {len(batch)} events (unique total: {len(all_events)})")
        cursor = win_end + timedelta(days=1)
        time.sleep(0.5)

    events_list = list(all_events.values())
    print(f"\nStarting enrichment & Kafka publish for {len(events_list)} unique 4-year closed events...")

    sent = 0
    for ev in events_list:
        geoms = ev.get("geometry", [])
        if not geoms:
            continue
        first_g = geoms[0]
        coords = first_g.get("coordinates", [])
        if not isinstance(coords, list) or len(coords) < 2 or not isinstance(coords[0], (int, float)):
            continue

        lon, lat = float(coords[0]), float(coords[1])
        event_time = first_g.get("date")
        closed_time = ev.get("closed")
        if not event_time or not closed_time:
            continue

        weather = fetch_power_weather(lat, lon, event_time)
        record = {
            "schema_version": "1.0",
            "record_id": f"{ev['id']}_{event_time}",
            "event_id": ev["id"],
            "title": ev.get("title"),
            "category_id": ev["categories"][0]["id"] if ev.get("categories") else "unknown",
            "source_id": ev["sources"][0]["id"] if ev.get("sources") else "EONET",
            "status": "closed",
            "event_time_utc": event_time,
            "first_seen_utc": event_time,
            "closed_time_utc": closed_time,
            "ingested_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "longitude": lon,
            "latitude": lat,
            "magnitude_value": float(first_g["magnitudeValue"]) if first_g.get("magnitudeValue") is not None else None,
            "magnitude_unit": first_g.get("magnitudeUnit"),
            **weather,
        }
        producer.send(KAFKA_TOPIC, key=ev["id"], value=record)
        sent += 1

        if sent % 100 == 0:
            producer.flush()
            save_cache(WEATHER_CACHE)
            print(f"Published {sent}/{len(events_list)} | Unique NASA POWER calls cached: {len(WEATHER_CACHE)}")

    producer.flush()
    save_cache(WEATHER_CACHE)
    print(f"\nDone! Published {sent} 4-year events ({len(WEATHER_CACHE)} cached NASA POWER calls).")


if __name__ == "__main__":
    main()
