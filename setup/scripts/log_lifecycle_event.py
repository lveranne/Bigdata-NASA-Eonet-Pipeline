import argparse
import os
from datetime import datetime, timezone
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

TS_HOST = os.getenv("TIMESCALE_HOST", "worker")
TS_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
TS_DB = os.getenv("TIMESCALE_DB", "eonet_db")
TS_USER = os.getenv("TIMESCALE_USER", "eonet_user")
TS_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "eonet_pass")


def log_event(event_type: str, component: str, title: str, details: str = "", tags: str = "") -> None:
    conn = psycopg2.connect(
        host=TS_HOST, port=TS_PORT, dbname=TS_DB, user=TS_USER, password=TS_PASSWORD
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO eonet_lifecycle_events (
                    event_time_utc, event_type, component, title, details, tags
                ) VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (datetime.now(timezone.utc), event_type, component, title, details, tags),
            )
        conn.commit()
        print(f"[Lifecycle Logged] {event_type} | {component}: {title}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log a Grafana lifecycle annotation to TimescaleDB")
    parser.add_argument("--type", required=True, choices=["MODEL_RETRAIN", "SERVICE_RESTART", "DEPLOYMENT"])
    parser.add_argument("--component", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--details", default="")
    parser.add_argument("--tags", default="")
    args = parser.parse_args()
    log_event(args.type, args.component, args.title, args.details, args.tags)
