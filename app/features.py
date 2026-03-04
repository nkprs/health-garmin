import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg

DB_DSN = os.getenv("DB_DSN")
IN_DIR = os.getenv("IN_DIR", "/data")
DB_CONNECT_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "20"))
DB_CONNECT_RETRY_DELAY = float(os.getenv("DB_CONNECT_RETRY_DELAY", "1.5"))

DDL = """
CREATE TABLE IF NOT EXISTS daily_features (
  day date PRIMARY KEY,
  sleep_total_sec integer,
  sleep_deep_sec integer,
  sleep_rem_sec integer,
  sleep_efficiency numeric(5,2),
  sleep_score numeric(5,2),
  resting_hr integer,
  hrv_night numeric(10,3),
  stress_avg numeric(10,3),
  stress_max numeric(10,3),
  body_battery integer,
  steps integer,
  active_kcal numeric(10,2),
  training_load numeric(10,3),
  computed_at timestamptz NOT NULL DEFAULT now()
);
"""


def to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def to_int(value: Any) -> int | None:
    num = to_float(value)
    if num is None:
        return None
    return int(num)


def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def get_nested(data: Any, *path: str) -> Any:
    cur = data
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        if cur is None:
            return None
    return cur


def walk_pairs(data: Any):
    if isinstance(data, dict):
        for key, value in data.items():
            yield key, value
            yield from walk_pairs(value)
    elif isinstance(data, list):
        for item in data:
            yield from walk_pairs(item)


def find_first_numeric_by_keys(
    data: Any,
    candidate_keys: set[str],
    min_value: float | None = None,
    max_value: float | None = None,
) -> float | None:
    if data is None:
        return None
    lookup = {k.lower() for k in candidate_keys}
    for key, value in walk_pairs(data):
        if str(key).lower() not in lookup:
            continue
        num = to_float(value)
        if num is None:
            continue
        if min_value is not None and num < min_value:
            continue
        if max_value is not None and num > max_value:
            continue
        return num
    return None


def extract_sleep(sleep: Any) -> dict[str, Any]:
    sleep_total = to_int(get_nested(sleep, "dailySleepDTO", "sleepTimeSeconds"))
    deep = to_int(get_nested(sleep, "dailySleepDTO", "deepSleepSeconds"))
    rem = to_int(get_nested(sleep, "dailySleepDTO", "remSleepSeconds"))

    efficiency = to_float(get_nested(sleep, "dailySleepDTO", "sleepEfficiency"))
    if efficiency is None:
        efficiency = find_first_numeric_by_keys(
            sleep,
            {"sleepEfficiency", "sleepQualityScore", "overallSleepQuality"},
            min_value=0,
            max_value=100,
        )
    if efficiency is None and sleep_total is not None:
        awake = to_float(get_nested(sleep, "dailySleepDTO", "awakeSleepSeconds"))
        if awake is not None and (sleep_total + awake) > 0:
            efficiency = (sleep_total / (sleep_total + awake)) * 100.0

    sleep_score = to_float(get_nested(sleep, "dailySleepDTO", "sleepScore"))
    if sleep_score is None:
        sleep_score = find_first_numeric_by_keys(
            sleep,
            {"sleepScore", "overallSleepScore", "overallScore"},
            min_value=0,
            max_value=100,
        )

    return {
        "sleep_total_sec": sleep_total,
        "sleep_deep_sec": deep,
        "sleep_rem_sec": rem,
        "sleep_efficiency": round(efficiency, 2) if efficiency is not None else None,
        "sleep_score": round(sleep_score, 2) if sleep_score is not None else None,
    }


def extract_hrv_night(hrv: Any) -> float | None:
    known_paths = [
        ("lastNightAvg",),
        ("lastNightAverage",),
        ("hrvSummary", "lastNightAvg"),
        ("hrvSummary", "lastNightAverage"),
        ("overnightAvg",),
        ("nightlyAvg",),
    ]
    for path in known_paths:
        val = to_float(get_nested(hrv, *path))
        if val is not None:
            return val

    return find_first_numeric_by_keys(
        hrv,
        {"lastNightAvg", "lastNightAverage", "overnightAvg", "nightlyAvg", "avgHrv", "averageHrv"},
    )


def extract_body_battery_from_stress(stress: Any) -> int | None:
    arr = get_nested(stress, "bodyBatteryValuesArray")
    if not isinstance(arr, list):
        return None
    for row in reversed(arr):
        if isinstance(row, list) and len(row) >= 3:
            lvl = to_int(row[2])
            if lvl is not None:
                return lvl
    return None


def parse_activity_day(activity: dict[str, Any]) -> str | None:
    for key in ("startTimeLocal", "startTimeGMT"):
        raw = activity.get(key)
        if not isinstance(raw, str):
            continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(raw[:19], fmt).date().isoformat()
            except ValueError:
                continue

    ts = to_float(activity.get("beginTimestamp"))
    if ts is not None:
        return datetime.utcfromtimestamp(ts / 1000.0).date().isoformat()
    return None


def aggregate_activities(day: str, activities: Any) -> dict[str, Any]:
    result = {
        "matched": False,
        "training_load": 0.0,
        "steps": 0,
        "active_kcal": 0.0,
    }
    if not isinstance(activities, list):
        return result

    for item in activities:
        if not isinstance(item, dict):
            continue
        if parse_activity_day(item) != day:
            continue

        result["matched"] = True

        tl = to_float(item.get("activityTrainingLoad"))
        if tl is None:
            tl = to_float(item.get("trainingLoad"))
        if tl is not None:
            result["training_load"] += tl

        steps = to_int(item.get("steps"))
        if steps is not None:
            result["steps"] += steps

        cal = to_float(item.get("calories"))
        bmr = to_float(item.get("bmrCalories"))
        if cal is not None:
            if bmr is not None and cal >= bmr:
                result["active_kcal"] += cal - bmr
            else:
                result["active_kcal"] += cal

    return result


def build_daily_features(day: str, summary: Any, sleep: Any, stress: Any, hrv: Any, activities: Any) -> dict[str, Any]:
    activity_agg = aggregate_activities(day, activities)
    sleep_features = extract_sleep(sleep)

    resting_hr = to_int(get_nested(summary, "restingHeartRate"))
    if resting_hr is None:
        resting_hr = to_int(get_nested(summary, "lastSevenDaysAvgRestingHeartRate"))

    stress_avg = to_float(get_nested(stress, "avgStressLevel"))
    if stress_avg is None:
        stress_avg = to_float(get_nested(summary, "averageStressLevel"))

    stress_max = to_float(get_nested(stress, "maxStressLevel"))
    if stress_max is None:
        stress_max = to_float(get_nested(summary, "maxStressLevel"))

    body_battery = None
    for key in (
        "bodyBatteryMostRecentValue",
        "bodyBatteryAtWakeTime",
        "bodyBatteryHighestValue",
        "bodyBatteryLowestValue",
    ):
        body_battery = to_int(get_nested(summary, key))
        if body_battery is not None:
            break
    if body_battery is None:
        body_battery = extract_body_battery_from_stress(stress)

    steps = to_int(get_nested(summary, "totalSteps"))
    if steps is None and activity_agg["matched"]:
        steps = activity_agg["steps"]

    active_kcal = to_float(get_nested(summary, "activeKilocalories"))
    if active_kcal is None:
        active_kcal = to_float(get_nested(summary, "wellnessActiveKilocalories"))
    if active_kcal is None and activity_agg["matched"]:
        active_kcal = activity_agg["active_kcal"]

    training_load = None
    if activity_agg["matched"]:
        training_load = activity_agg["training_load"]
    if training_load is None:
        training_load = to_float(get_nested(summary, "trainingLoad"))

    return {
        **sleep_features,
        "resting_hr": resting_hr,
        "hrv_night": extract_hrv_night(hrv),
        "stress_avg": stress_avg,
        "stress_max": stress_max,
        "body_battery": body_battery,
        "steps": steps,
        "active_kcal": round(active_kcal, 2) if active_kcal is not None else None,
        "training_load": round(training_load, 3) if training_load is not None else None,
    }


def upsert_daily_features(conn: psycopg.Connection, day: str, features: dict[str, Any]):
    conn.execute(
        """
        INSERT INTO daily_features(
            day,
            sleep_total_sec,
            sleep_deep_sec,
            sleep_rem_sec,
            sleep_efficiency,
            sleep_score,
            resting_hr,
            hrv_night,
            stress_avg,
            stress_max,
            body_battery,
            steps,
            active_kcal,
            training_load
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(day)
        DO UPDATE SET
            sleep_total_sec = EXCLUDED.sleep_total_sec,
            sleep_deep_sec = EXCLUDED.sleep_deep_sec,
            sleep_rem_sec = EXCLUDED.sleep_rem_sec,
            sleep_efficiency = EXCLUDED.sleep_efficiency,
            sleep_score = EXCLUDED.sleep_score,
            resting_hr = EXCLUDED.resting_hr,
            hrv_night = EXCLUDED.hrv_night,
            stress_avg = EXCLUDED.stress_avg,
            stress_max = EXCLUDED.stress_max,
            body_battery = EXCLUDED.body_battery,
            steps = EXCLUDED.steps,
            active_kcal = EXCLUDED.active_kcal,
            training_load = EXCLUDED.training_load,
            computed_at = now()
        """,
        (
            day,
            features["sleep_total_sec"],
            features["sleep_deep_sec"],
            features["sleep_rem_sec"],
            features["sleep_efficiency"],
            features["sleep_score"],
            features["resting_hr"],
            features["hrv_night"],
            features["stress_avg"],
            features["stress_max"],
            features["body_battery"],
            features["steps"],
            features["active_kcal"],
            features["training_load"],
        ),
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

    loaded = 0
    with connect_with_retry(DB_DSN) as conn:
        conn.execute(DDL)

        for day_dir in sorted([p for p in base.iterdir() if p.is_dir()]):
            day = day_dir.name
            try:
                datetime.strptime(day, "%Y-%m-%d")
            except ValueError:
                continue

            summary = read_json(day_dir / "summary.json")
            sleep = read_json(day_dir / "sleep.json")
            stress = read_json(day_dir / "stress.json")
            hrv = read_json(day_dir / "hrv.json")
            activities = read_json(day_dir / "activities_0_20.json")

            features = build_daily_features(day, summary, sleep, stress, hrv, activities)
            upsert_daily_features(conn, day, features)
            loaded += 1

        conn.commit()

    print(f"Loaded daily features into Postgres: daily_features ({loaded} days)")


if __name__ == "__main__":
    main()
