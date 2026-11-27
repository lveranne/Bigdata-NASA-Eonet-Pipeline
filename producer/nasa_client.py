import os
import time
import requests
from datetime import datetime, timezone

EONET_URL = "https://eonet.gsfc.nasa.gov/api/v3/events"
POWER_DAILY_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

# Local weather cache {(round(lat,1), round(lon,1), YYYYMMDD): dict} to respect NASA POWER rate limits
_weather_cache = {}


def fetch_power_weather(lat: float, lon: float, event_dt: datetime) -> dict:
    date_str = event_dt.strftime("%Y%m%d")
    cache_key = (round(lat, 1), round(lon, 1), date_str)
    if cache_key in _weather_cache:
        return _weather_cache[cache_key]

    params = {
        "parameters": "T2M,RH2M,WS10M,PRECTOTCORR",
        "community": "RE",
        "longitude": round(lon, 4),
        "latitude": round(lat, 4),
        "start": date_str,
        "end": date_str,
        "format": "JSON",
    }
    default_wx = {
        "temp_2m_c": None,
        "humidity_2m_pct": None,
        "wind_10m_ms": None,
        "precip_mm_day": None,
    }
    for attempt in range(3):
        try:
            resp = requests.get(POWER_DAILY_URL, params=params, timeout=15)
            if resp.status_code == 200:
                p = resp.json().get("properties", {}).get("parameter", {})
                def clean(val):
                    return None if val is None or val <= -900 else float(val)
                wx = {
                    "temp_2m_c": clean(p.get("T2M", {}).get(date_str)),
                    "humidity_2m_pct": clean(p.get("RH2M", {}).get(date_str)),
                    "wind_10m_ms": clean(p.get("WS10M", {}).get(date_str)),
                    "precip_mm_day": clean(p.get("PRECTOTCORR", {}).get(date_str)),
                }
                _weather_cache[cache_key] = wx
                return wx
            elif resp.status_code == 429:
                time.sleep(2 ** attempt)
        except Exception:
            time.sleep(1)

    _weather_cache[cache_key] = default_wx
    return default_wx


def parse_iso_utc(ts_str: str) -> datetime:
    if not ts_str:
        return None
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc)


def build_event_messages(event: dict, enrich_weather: bool = True, latest_only: bool = False) -> list:
    """Converts a raw NASA EONET v3 event into Checkpoint 1 Kafka messages."""
    event_id = event["id"]
    title = event.get("title", "")
    categories = event.get("categories", [])
    category_id = categories[0]["id"] if categories else "unknown"
    sources = event.get("sources", [])
    source_id = sources[0]["id"] if sources else "NASA"
    closed_raw = event.get("closed")
    closed_dt = parse_iso_utc(closed_raw)
    closed_time_utc = closed_dt.strftime("%Y-%m-%dT%H:%M:%SZ") if closed_dt else None

    geometries = sorted(event.get("geometry", []), key=lambda g: g.get("date", ""))
    if not geometries:
        return []

    first_dt = parse_iso_utc(geometries[0]["date"])
    first_seen_utc = first_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    selected_geos = [geometries[-1]] if latest_only else geometries
    messages = []
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for idx, geo in enumerate(selected_geos):
        obs_dt = parse_iso_utc(geo["date"])
        event_time_utc = obs_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        coords = geo.get("coordinates", [0.0, 0.0])
        # Handle Polygon vs Point coordinates safely
        if isinstance(coords[0], list):
            lon, lat = float(coords[0][0][0]), float(coords[0][0][1])
        else:
            lon, lat = float(coords[0]), float(coords[1])

        is_last_geo = (geo == geometries[-1])
        status = "closed" if (closed_time_utc and is_last_geo) else "open"

        wx = fetch_power_weather(lat, lon, obs_dt) if enrich_weather else {
            "temp_2m_c": None, "humidity_2m_pct": None, "wind_10m_ms": None, "precip_mm_day": None
        }

        mag_val = geo.get("magnitudeValue")
        messages.append({
            "schema_version": "1.0",
            "record_id": f"{event_id}_{event_time_utc}_{status}",
            "event_id": event_id,
            "title": title,
            "category_id": category_id,
            "source_id": source_id,
            "status": status,
            "event_time_utc": event_time_utc,
            "first_seen_utc": first_seen_utc,
            "closed_time_utc": closed_time_utc if status == "closed" else None,
            "ingested_at_utc": now_utc,
            "longitude": lon,
            "latitude": lat,
            "magnitude_value": float(mag_val) if mag_val is not None else None,
            "magnitude_unit": geo.get("magnitudeUnit"),
            **wx,
        })
    return messages
