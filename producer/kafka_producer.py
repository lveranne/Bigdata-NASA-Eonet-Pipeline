import os
import json
import time
import signal
import requests
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
from nasa_client import EONET_URL, build_event_messages, parse_iso_utc

load_dotenv(os.path.expanduser("~/Bigdata-NASA-Eonet-Pipeline/.env"))

CUTOFF_DT = datetime(2026, 11, 27, 0, 0, 0, tzinfo=timezone.utc)
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "master:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "nasa-eonet-raw")
NASA_API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY")
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "60"))
STATE_FILE = os.path.expanduser("~/Bigdata-NASA-Eonet-Pipeline/producer/.seen_records.json")

running = True


def handle_sigterm(signum, frame):
    global running
    running = False


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


def log_lifecycle(event_type: str, details: dict):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "worker"),
            port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME", "eonet_db"),
            user=os.getenv("DB_USER", "eonet_user"),
            password=os.getenv("DB_PASSWORD", "eonet_pass"),
        )
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO eonet_lifecycle_events (event_ts, component, event_type, model_version, details)
                VALUES (NOW(), 'kafka_producer', %s, 'n/a', %s::jsonb)
                """,
                (event_type, json.dumps(details)),
            )
        conn.close()
    except Exception as e:
        print(f"[WARN] Lifecycle log failed: {e}")


def load_seen() -> set:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_seen(seen: set):
    with open(STATE_FILE, "w") as f:
        json.dump(list(seen)[-50000:], f)


def poll_stream(producer: KafkaProducer, seen: set):
    headers_params = {}
    if NASA_API_KEY and NASA_API_KEY != "DEMO_KEY":
        headers_params["api_key"] = NASA_API_KEY

    open_sent, closed_sent = 0, 0

    # 1. Fetch LIVE OPEN events for real-time speed-layer forecasting
    r_open = requests.get(EONET_URL, params={"status": "open", "limit": 100, **headers_params}, timeout=30)
    r_open.raise_for_status()
    for ev in r_open.json().get("events", []):
        for msg in build_event_messages(ev, enrich_weather=True, latest_only=True):
            if msg["record_id"] not in seen:
                producer.send(TOPIC, key=msg["event_id"], value=msg)
                seen.add(msg["record_id"])
                open_sent += 1

    # 2. Synchronize NEWLY CLOSED events (>= 2026-11-27 test set) to evaluate prediction errors
    r_closed = requests.get(EONET_URL, params={"status": "closed", "days": 14, "limit": 100, **headers_params}, timeout=30)
    r_closed.raise_for_status()
    for ev in r_closed.json().get("events", []):
        closed_dt = parse_iso_utc(ev.get("closed"))
        if not closed_dt or closed_dt < CUTOFF_DT:
            continue
        for msg in build_event_messages(ev, enrich_weather=True, latest_only=True):
            if msg["record_id"] not in seen:
                producer.send(TOPIC, key=msg["event_id"], value=msg)
                seen.add(msg["record_id"])
                closed_sent += 1

    producer.flush()
    save_seen(seen)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Published open={open_sent}, newly_closed_test={closed_sent}")


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    seen = load_seen()
    log_lifecycle("service_start", {"topic": TOPIC, "poll_interval_sec": POLL_INTERVAL_SEC})
    backoff = 5

    while running:
        try:
            poll_stream(producer, seen)
            backoff = 5
            for _ in range(POLL_INTERVAL_SEC):
                if not running:
                    break
                time.sleep(1)
        except Exception as e:
            print(f"[ERROR] Poll failed ({e}); retrying in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)

    log_lifecycle("service_stop", {"reason": "SIGTERM/SIGINT"})
    producer.close()


if __name__ == "__main__":
    main()
