import os
import json
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
from nasa_client import EONET_URL, build_event_messages, parse_iso_utc

load_dotenv(os.path.expanduser("~/Bigdata-NASA-Eonet-Pipeline/.env"))

CUTOFF_DT = datetime(2026, 11, 27, 0, 0, 0, tzinfo=timezone.utc)
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "master:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "nasa-eonet-raw")
NASA_API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY")


def run_backfill(days: int = 1825, max_events: int = 3000, enrich_weather: bool = True):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=50,
    )

    params = {"status": "closed", "days": days, "limit": max_events}
    if NASA_API_KEY and NASA_API_KEY != "DEMO_KEY":
        params["api_key"] = NASA_API_KEY

    print(f"[BACKFILL] Requesting up to {max_events} closed EONET events over {days} days (< {CUTOFF_DT.isoformat()})...")
    resp = requests.get(EONET_URL, params=params, timeout=60)
    resp.raise_for_status()
    events = resp.json().get("events", [])
    print(f"[BACKFILL] Downloaded {len(events)} raw closed events from NASA EONET.")

    sent_msgs = 0
    kept_events = 0
    for ev in events:
        closed_dt = parse_iso_utc(ev.get("closed"))
        # Strictly enforce training split (< 2026-11-27)
        if not closed_dt or closed_dt >= CUTOFF_DT:
            continue

        # Build initial open observation + final closed observation for trajectory & duration training
        msgs = build_event_messages(ev, enrich_weather=enrich_weather, latest_only=False)
        if not msgs:
            continue
        selected = [msgs[0], msgs[-1]] if len(msgs) > 1 else [msgs[-1]]
        for m in selected:
            producer.send(TOPIC, key=m["event_id"], value=m)
            sent_msgs += 1

        kept_events += 1
        if kept_events % 100 == 0:
            producer.flush()
            print(f"[BACKFILL] Processed {kept_events} historical events ({sent_msgs} Kafka messages)...")
            time.sleep(0.2)

    producer.flush()
    producer.close()
    print(f"[BACKFILL COMPLETE] Published {sent_msgs} messages from {kept_events} closed training events.")


if __name__ == "__main__":
    run_backfill(days=1825, max_events=3000, enrich_weather=True)
