import json
import os
import time
from pathlib import Path
from datetime import datetime

import psycopg

DB_DSN = os.getenv("DB_DSN")
IN_DIR = os.getenv("IN_DIR", "/data")
DB_CONNECT_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "20"))
DB_CONNECT_RETRY_DELAY = float(os.getenv("DB_CONNECT_RETRY_DELAY", "1.5"))

DDL = """
CREATE TABLE IF NOT EXISTS garmin_raw (
  id bigserial PRIMARY KEY,
  day date NOT NULL,
  source text NOT NULL,         -- summary/sleep/stress/heart_rate/hrv/activities
  fetched_at timestamptz NOT NULL DEFAULT now(),
  payload jsonb NOT NULL,
  UNIQUE(day, source)
);

CREATE INDEX IF NOT EXISTS garmin_raw_day_idx ON garmin_raw(day);
CREATE INDEX IF NOT EXISTS garmin_raw_source_idx ON garmin_raw(source);
"""

def upsert(conn, day: str, source: str, payload: dict):
    conn.execute(
        """
        INSERT INTO garmin_raw(day, source, payload)
        VALUES (%s, %s, %s::jsonb)
        ON CONFLICT(day, source)
        DO UPDATE SET payload = EXCLUDED.payload, fetched_at = now()
        """,
        (day, source, json.dumps(payload)),
    )

def connect_with_retry(dsn: str) -> psycopg.Connection:
    last_error = None
    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            return psycopg.connect(dsn)
        except psycopg.OperationalError as e:
            last_error = e
            if attempt == DB_CONNECT_RETRIES:
                break
            print(
                f"DB not ready (attempt {attempt}/{DB_CONNECT_RETRIES}), retrying in {DB_CONNECT_RETRY_DELAY}s..."
            )
            time.sleep(DB_CONNECT_RETRY_DELAY)
    raise last_error  # type: ignore[misc]

def main():
    if not DB_DSN:
        raise SystemExit("Missing DB_DSN")

    base = Path(IN_DIR)
    if not base.exists():
        raise SystemExit(f"IN_DIR not found: {IN_DIR}")

    with connect_with_retry(DB_DSN) as conn:
        conn.execute(DDL)

        # ожидаем структуру /data/YYYY-MM-DD/*.json
        for day_dir in sorted([p for p in base.iterdir() if p.is_dir()]):
            day = day_dir.name
            # грубая проверка формата даты
            try:
                datetime.strptime(day, "%Y-%m-%d")
            except ValueError:
                continue

            for file in sorted(day_dir.glob("*.json")):
                source = file.stem  # summary, sleep, stress, heart_rate, hrv, activities_0_20
                try:
                    payload = json.loads(file.read_text(encoding="utf-8"))
                except Exception as e:
                    payload = {"_error": "json_parse", "message": str(e)}

                upsert(conn, day, source, payload)

        conn.commit()

    print("Loaded raw JSON into Postgres: garmin_raw")

if __name__ == "__main__":
    main()
