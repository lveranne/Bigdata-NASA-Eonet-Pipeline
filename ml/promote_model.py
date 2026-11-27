import argparse
import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/Bigdata-NASA-Eonet-Pipeline/.env"))

parser = argparse.ArgumentParser(description="Promote a shadow model to production Champion in TimescaleDB")
parser.add_argument("--model", choices=["v1_baseline", "v2_gbt", "v3_xgboost"], help="Model version to promote to champion")
parser.add_argument("--reason", default="Manual champion promotion after shadow evaluation", help="Audit reason logged to Grafana")
parser.add_argument("--days", type=int, default=14, help="Lookback window in days for shadow scorecard")
args = parser.parse_args()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "worker"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "eonet_db"),
    user=os.getenv("DB_USER", "eonet_user"),
    password=os.getenv("DB_PASSWORD", "eonet_pass"),
)

with conn, conn.cursor() as cur:
    print(f"\n=== Shadow Benchmark Scorecard (Last {args.days} Days on Closed Stream Events) ===")
    cur.execute(
        """
        SELECT r.model_version, r.deployment_role,
               COUNT(h.abs_error_hours) AS closed_n,
               ROUND(AVG(h.abs_error_hours)::numeric, 2) AS mae_h,
               ROUND(SQRT(AVG(h.squared_error_hours))::numeric, 2) AS rmse_h
        FROM eonet_model_registry r
        LEFT JOIN eonet_prediction_history h
          ON r.model_version = h.model_version
         AND h.status = 'closed'
         AND h.data_split = 'test'
         AND h.scored_at_utc >= NOW() - (%s || ' days')::interval
        GROUP BY 1, 2
        ORDER BY mae_h NULLS LAST, r.deployment_role;
        """,
        (args.days,),
    )
    for row in cur.fetchall():
        print(f"  {row[0]:12s} | role={row[1]:8s} | closed_n={row[2]:4d} | MAE={str(row[3]):>8s}h | RMSE={str(row[4]):>8s}h")

    if args.model:
        cur.execute("SELECT model_version FROM eonet_model_registry WHERE deployment_role = 'champion';")
        prev = cur.fetchone()
        prev_champion = prev[0] if prev else "none"

        if prev_champion == args.model:
            print(f"\n[NO-OP] {args.model} is already the active production champion.")
        else:
            cur.execute("UPDATE eonet_model_registry SET deployment_role = 'shadow' WHERE deployment_role = 'champion';")
            cur.execute(
                """
                UPDATE eonet_model_registry
                SET deployment_role = 'champion', promoted_at_utc = NOW(), promotion_notes = %s
                WHERE model_version = %s;
                """,
                (args.reason, args.model),
            )
            cur.execute(
                """
                INSERT INTO eonet_lifecycle_events (event_ts, component, event_type, model_version, details)
                VALUES (NOW(), 'model_registry', 'champion_promoted', %s, %s::jsonb);
                """,
                (args.model, json.dumps({"previous_champion": prev_champion, "new_champion": args.model, "reason": args.reason})),
            )
            print(f"\n[PROMOTED] {prev_champion} (shadow) <--- {args.model} (CHAMPION)")
            print("Logged 'champion_promoted' lifecycle annotation to TimescaleDB & Grafana.")

conn.close()
