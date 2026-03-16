import json
import os
from datetime import date, timedelta
from dateutil.parser import isoparse

from garminconnect import Garmin

OUT_DIR = os.getenv("OUT_DIR", "/data")


def fetch_heart_rate(client: Garmin, day: str):
    # The library has exposed heart-rate endpoints under slightly different names
    # across releases, so try known variants in order.
    for method_name in ("get_heart_rates", "get_heart_rate_data", "get_heart_rate"):
        method = getattr(client, method_name, None)
        if callable(method):
            return method(day)
    raise AttributeError("Garmin client has no supported heart-rate method")

def dump_json(path: str, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def safe_call(fn, label: str):
    try:
        return fn()
    except Exception as e:
        return {"_error": label, "message": str(e)}

def main():
    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    if not email or not password:
        raise SystemExit("Missing GARMIN_EMAIL or GARMIN_PASSWORD in env")

    client = Garmin(email, password)
    client.login()

    # Какие даты выгружаем
    forced = os.getenv("DATE")
    days_back = int(os.getenv("DAYS_BACK", "1"))  # по умолчанию: только сегодня

    if forced:
        start = isoparse(forced).date()
        dates = [start]
    else:
        today = date.today()
        dates = [today - timedelta(days=i) for i in range(days_back)]

    for d in dates:
        ds = d.isoformat()
        base = os.path.join(OUT_DIR, ds)

        # Daily summary
        summary = safe_call(lambda: client.get_user_summary(ds), "get_user_summary")
        dump_json(os.path.join(base, "summary.json"), summary)

        # Sleep
        sleep = safe_call(lambda: client.get_sleep_data(ds), "get_sleep_data")
        dump_json(os.path.join(base, "sleep.json"), sleep)

        # Stress
        stress = safe_call(lambda: client.get_stress_data(ds), "get_stress_data")
        dump_json(os.path.join(base, "stress.json"), stress)

        # Heart rate
        heart_rate = safe_call(lambda: fetch_heart_rate(client, ds), "get_heart_rates")
        dump_json(os.path.join(base, "heart_rate.json"), heart_rate)

        # HRV (может не отдаваться на некоторых аккаунтах/днях)
        hrv = safe_call(lambda: client.get_hrv_data(ds), "get_hrv_data")
        dump_json(os.path.join(base, "hrv.json"), hrv)

        # Activities list (последние 20)
        acts = safe_call(lambda: client.get_activities(0, 20), "get_activities")
        dump_json(os.path.join(base, "activities_0_20.json"), acts)

    print(f"Done. Files are in {OUT_DIR}")

if __name__ == "__main__":
    main()
