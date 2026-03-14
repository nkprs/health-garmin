import json
import os
import random
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg
import requests
from psycopg import sql
from psycopg.rows import dict_row

DB_DSN = os.getenv("DB_DSN")
DB_CONNECT_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "20"))
DB_CONNECT_RETRY_DELAY = float(os.getenv("DB_CONNECT_RETRY_DELAY", "1.5"))
TARGET_DATE = os.getenv("TARGET_DATE")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_API_URL = os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/responses")
OPENAI_TIMEOUT_SEC = int(os.getenv("OPENAI_TIMEOUT_SEC", "60"))
OPENAI_DRY_RUN = os.getenv("OPENAI_DRY_RUN", "0") == "1"
OPENAI_MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "5"))
OPENAI_RETRY_BASE_SEC = float(os.getenv("OPENAI_RETRY_BASE_SEC", "2.0"))
OPENAI_RETRY_MAX_SEC = float(os.getenv("OPENAI_RETRY_MAX_SEC", "60.0"))
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_HTTP_REFERER = os.getenv("OPENROUTER_HTTP_REFERER", "http://localhost")
OPENROUTER_APP_NAME = os.getenv("OPENROUTER_APP_NAME", "garmin-export")

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
RESEND_API_BASE = os.getenv("RESEND_API_BASE", "https://api.resend.com")
RESEND_FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL")
RESEND_TO_EMAIL = os.getenv("RESEND_TO_EMAIL")
RESEND_TIMEOUT_SEC = int(os.getenv("RESEND_TIMEOUT_SEC", "20"))
EMAIL_SUBJECT_PREFIX = os.getenv("EMAIL_SUBJECT_PREFIX", "[Garmin Brief]")

NUMERIC_FEATURE_KEYS = [
    "sleep_total_sec",
    "sleep_deep_sec",
    "sleep_rem_sec",
    "sleep_efficiency",
    "sleep_score",
    "resting_hr",
    "hrv_night",
    "stress_avg",
    "stress_max",
    "body_battery",
    "steps",
    "active_kcal",
    "training_load",
]

DELTA_PCT_THRESHOLDS = {
    "resting_hr": 8.0,
    "hrv_night": 12.0,
    "stress_avg": 15.0,
    "stress_max": 15.0,
    "sleep_total_sec": 15.0,
    "sleep_efficiency": 8.0,
    "sleep_score": 10.0,
    "body_battery": 20.0,
    "steps": 35.0,
    "training_load": 35.0,
    "active_kcal": 35.0,
}

SYSTEM_PROMPT = """Ты — персональный помощник по восстановлению для биохакинга.
Входные данные: daily_features за сегодня, baseline за 14 дней, дельты/аномалии и 3 последние тренировки.
Сформируй ответ только в JSON с полями:
- summary_lines: массив из 5-7 коротких строк
- recommendations: массив объектов {area, action, reason, confidence}
- red_flags: массив объектов {flag, probability, why}
- questions: массив из 1-3 уточняющих вопросов
- confidence_overall: число от 0 до 1
Требования:
- Не ставь диагнозы и не используй категоричные медицинские утверждения.
- Если данных мало, явно скажи это.
- Рекомендации должны быть практичными на ближайшие 24 часа.
"""

DDL = """
CREATE TABLE IF NOT EXISTS daily_briefs (
  id bigserial PRIMARY KEY,
  day date NOT NULL,
  model text NOT NULL,
  prompt_payload jsonb NOT NULL,
  brief_json jsonb NOT NULL,
  raw_response jsonb,
  generated_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS daily_briefs_day_idx ON daily_briefs(day);
CREATE INDEX IF NOT EXISTS daily_briefs_generated_at_idx ON daily_briefs(generated_at DESC);
"""

MIGRATION_SQL = """
DO $$
DECLARE
    existing_pk text;
BEGIN
    IF to_regclass('public.daily_briefs') IS NULL THEN
        RETURN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'daily_briefs'
          AND column_name = 'id'
    ) THEN
        ALTER TABLE public.daily_briefs ADD COLUMN id bigserial;
    END IF;

    SELECT conname INTO existing_pk
    FROM pg_constraint
    WHERE conrelid = 'public.daily_briefs'::regclass
      AND contype = 'p'
    LIMIT 1;

    IF existing_pk IS NOT NULL THEN
        EXECUTE format('ALTER TABLE public.daily_briefs DROP CONSTRAINT %I', existing_pk);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.daily_briefs'::regclass
          AND contype = 'p'
    ) THEN
        ALTER TABLE public.daily_briefs ADD PRIMARY KEY (id);
    END IF;

    ALTER TABLE public.daily_briefs ALTER COLUMN day SET NOT NULL;
    ALTER TABLE public.daily_briefs ALTER COLUMN model SET NOT NULL;
    ALTER TABLE public.daily_briefs ALTER COLUMN prompt_payload SET NOT NULL;
    ALTER TABLE public.daily_briefs ALTER COLUMN brief_json SET NOT NULL;
    ALTER TABLE public.daily_briefs ALTER COLUMN generated_at SET DEFAULT now();
    ALTER TABLE public.daily_briefs ALTER COLUMN updated_at SET DEFAULT now();
END $$;
"""


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


def to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
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


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def parse_date(raw: str) -> str:
    return datetime.strptime(raw, "%Y-%m-%d").date().isoformat()


def fetch_target_day(conn: psycopg.Connection) -> str:
    if TARGET_DATE:
        return parse_date(TARGET_DATE)
    with conn.cursor() as cur:
        cur.execute("SELECT max(day)::text FROM daily_features")
        row = cur.fetchone()
    if not row or not row[0]:
        raise SystemExit("No rows in daily_features. Run features.py first.")
    return row[0]


def fetch_today_features(conn: psycopg.Connection, day: str) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM daily_features WHERE day = %s", (day,))
        row = cur.fetchone()
    if not row:
        raise SystemExit(f"daily_features row not found for {day}")
    out = dict(row)
    out["day"] = day
    out.pop("computed_at", None)
    return to_jsonable(out)


def fetch_baseline(conn: psycopg.Connection, day: str) -> dict[str, Any]:
    select_metrics = sql.SQL(",\n      ").join(
        sql.SQL("avg({field})::float as {alias}").format(
            field=sql.Identifier(key),
            alias=sql.Identifier(key),
        )
        for key in NUMERIC_FEATURE_KEYS
    )
    query = sql.SQL(
        """
    SELECT
      count(*)::int AS baseline_days,
      {select_metrics}
    FROM daily_features
    WHERE day < %s
      AND day >= (%s::date - interval '14 day')
    """
    ).format(select_metrics=select_metrics)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, (day, day))
        row = cur.fetchone()
    if not row:
        return {"baseline_days": 0}
    return to_jsonable(dict(row))


def parse_activity_dt(activity: dict[str, Any]) -> datetime | None:
    for key in ("startTimeLocal", "startTimeGMT"):
        raw = activity.get(key)
        if not isinstance(raw, str):
            continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(raw[:19], fmt)
            except ValueError:
                continue
    ts = to_float(activity.get("beginTimestamp"))
    if ts is not None:
        return datetime.utcfromtimestamp(ts / 1000.0)
    return None


def fetch_recent_trainings(conn: psycopg.Connection, day: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.garmin_raw')::text")
        table_name_row = cur.fetchone()
    if not table_name_row or not table_name_row[0]:
        return []

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT payload
            FROM garmin_raw
            WHERE source = 'activities_0_20'
              AND day <= %s
            ORDER BY day DESC
            LIMIT 30
            """,
            (day,),
        )
        rows = cur.fetchall()

    dedupe: dict[int, dict[str, Any]] = {}
    for row in rows:
        payload = row.get("payload")
        if not isinstance(payload, list):
            continue
        for activity in payload:
            if not isinstance(activity, dict):
                continue
            activity_id = activity.get("activityId")
            if not isinstance(activity_id, int):
                continue
            if activity_id in dedupe:
                continue

            dt = parse_activity_dt(activity)
            if dt is None:
                continue
            dedupe[activity_id] = {
                "activity_id": activity_id,
                "start_time": dt.isoformat(sep=" "),
                "activity_name": activity.get("activityName"),
                "activity_type": (activity.get("activityType") or {}).get("typeKey"),
                "duration_sec": to_float(activity.get("duration")),
                "distance_m": to_float(activity.get("distance")),
                "avg_hr": to_float(activity.get("averageHR")),
                "calories": to_float(activity.get("calories")),
                "training_load": to_float(activity.get("activityTrainingLoad") or activity.get("trainingLoad")),
            }

    trainings = list(dedupe.values())
    trainings.sort(key=lambda x: x["start_time"], reverse=True)
    return trainings[:3]


def compute_deltas(today: dict[str, Any], baseline: dict[str, Any]) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    for key in NUMERIC_FEATURE_KEYS:
        today_val = to_float(today.get(key))
        base_val = to_float(baseline.get(key))
        if today_val is None or base_val is None:
            continue

        delta_abs = today_val - base_val
        delta_pct = None
        if abs(base_val) > 1e-9:
            delta_pct = (delta_abs / base_val) * 100.0

        threshold = DELTA_PCT_THRESHOLDS.get(key, 20.0)
        is_anomaly = False
        if delta_pct is not None:
            is_anomaly = abs(delta_pct) >= threshold
        else:
            is_anomaly = abs(delta_abs) > 0

        if not is_anomaly:
            continue

        severity = "high" if (delta_pct is not None and abs(delta_pct) >= threshold * 1.5) else "medium"
        anomalies.append(
            {
                "metric": key,
                "today": round(today_val, 3),
                "baseline14": round(base_val, 3),
                "delta_abs": round(delta_abs, 3),
                "delta_pct": round(delta_pct, 2) if delta_pct is not None else None,
                "severity": severity,
                "direction": "up" if delta_abs > 0 else "down",
            }
        )

    anomalies.sort(key=lambda x: abs(x.get("delta_pct") or x["delta_abs"]), reverse=True)
    return anomalies


def extract_json_text_from_openai_response(data: dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str) and data["output_text"].strip():
        return data["output_text"]

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        msg = (choices[0] or {}).get("message") or {}
        content = msg.get("content")
        if isinstance(content, str) and content.strip():
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for chunk in content:
                if not isinstance(chunk, dict):
                    continue
                txt = chunk.get("text")
                if isinstance(txt, str):
                    parts.append(txt)
            if parts:
                return "\n".join(parts)

    output = data.get("output")
    if isinstance(output, list):
        parts: list[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for chunk in content:
                if not isinstance(chunk, dict):
                    continue
                txt = chunk.get("text")
                if isinstance(txt, str):
                    parts.append(txt)
        if parts:
            return "\n".join(parts)

    raise RuntimeError("OpenAI response did not contain text output")


def parse_brief_json(text: str) -> dict[str, Any]:
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        parsed = json.loads(text[start : end + 1])
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError("Failed to parse JSON brief from OpenAI output")


def normalize_brief(payload: dict[str, Any]) -> dict[str, Any]:
    summary_lines = payload.get("summary_lines")
    if not isinstance(summary_lines, list):
        summary_lines = []
    summary_lines = [str(x).strip() for x in summary_lines if str(x).strip()]

    recommendations = payload.get("recommendations")
    if not isinstance(recommendations, list):
        recommendations = []

    red_flags = payload.get("red_flags")
    if not isinstance(red_flags, list):
        red_flags = []

    questions = payload.get("questions")
    if not isinstance(questions, list):
        questions = []
    questions = [str(x).strip() for x in questions if str(x).strip()][:3]

    confidence_overall = to_float(payload.get("confidence_overall"))
    if confidence_overall is None:
        confidence_overall = 0.5
    confidence_overall = max(0.0, min(1.0, confidence_overall))

    return {
        "summary_lines": summary_lines[:7],
        "recommendations": recommendations,
        "red_flags": red_flags,
        "questions": questions,
        "confidence_overall": round(confidence_overall, 3),
    }


def build_error_brief(message: str) -> dict[str, Any]:
    return {
        "summary_lines": [
            "Не удалось сгенерировать GPT-brief автоматически.",
            f"Причина: {message}",
            "Повтори запуск позже или проверь настройки OpenAI/API.",
        ],
        "recommendations": [],
        "red_flags": [],
        "questions": ["Есть ли субъективные симптомы усталости или недосыпа сегодня?"],
        "confidence_overall": 0.1,
    }


def parse_retry_after(headers: requests.structures.CaseInsensitiveDict[str]) -> float | None:
    raw = headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def extract_openai_error(response: requests.Response) -> tuple[str, str | None, str | None]:
    try:
        payload = response.json()
    except ValueError:
        text = response.text.strip()
        return (text[:500] if text else "Unknown API error"), None, None

    if isinstance(payload, dict):
        err = payload.get("error")
        if isinstance(err, dict):
            msg = err.get("message")
            err_type = err.get("type")
            err_code = err.get("code")
            return (str(msg) if msg else "Unknown API error"), (
                str(err_type) if err_type else None
            ), (str(err_code) if err_code else None)
    return json.dumps(payload, ensure_ascii=False)[:500], None, None


def call_openai(prompt_payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if OPENAI_DRY_RUN:
        stub = {
            "summary_lines": [
                "Мало исторических данных: выводы предварительные.",
                "Основной фокус на восстановление и стабильный сон.",
                "Стресс выше базовой линии — контролируй интенсивность нагрузки.",
                "Низкая активность сегодня может быть уместна как recovery day.",
                "Нужны уточнения по самочувствию и признакам усталости.",
            ],
            "recommendations": [
                {"area": "нагрузка", "action": "легкая активность 20-40 минут", "reason": "поддержать восстановление", "confidence": 0.62},
                {"area": "сон", "action": "целевое окно сна не менее 7.5 часов", "reason": "стабилизация HRV/стресса", "confidence": 0.66},
            ],
            "red_flags": [
                {"flag": "недовосстановление", "probability": 0.41, "why": "стресс выше baseline при ограниченных данных"},
            ],
            "questions": [
                "Какое качество сна было субъективно этой ночью?",
                "Есть ли признаки начинающейся простуды или боли в горле?",
            ],
            "confidence_overall": 0.58,
        }
        return normalize_brief(stub), {"dry_run": True}

    is_openrouter = "openrouter.ai" in OPENAI_API_URL.lower()
    api_key = (OPENROUTER_API_KEY or OPENAI_API_KEY) if is_openrouter else OPENAI_API_KEY
    if not api_key:
        if is_openrouter:
            raise SystemExit("Missing OPENROUTER_API_KEY (or OPENAI_API_KEY) for OpenRouter")
        raise SystemExit("Missing OPENAI_API_KEY")

    user_prompt = json.dumps(prompt_payload, ensure_ascii=False, indent=2)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if is_openrouter:
        if OPENROUTER_HTTP_REFERER:
            headers["HTTP-Referer"] = OPENROUTER_HTTP_REFERER
        if OPENROUTER_APP_NAME:
            headers["X-Title"] = OPENROUTER_APP_NAME

    if OPENAI_API_URL.rstrip("/").endswith("/chat/completions"):
        req_body = {
            "model": OPENAI_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
        }
    else:
        req_body = {
            "model": OPENAI_MODEL,
            "input": [
                {"role": "system", "content": [{"type": "input_text", "text": SYSTEM_PROMPT}]},
                {"role": "user", "content": [{"type": "input_text", "text": user_prompt}]},
            ],
            "temperature": 0.2,
        }

    last_exc: Exception | None = None
    for attempt in range(1, OPENAI_MAX_RETRIES + 1):
        try:
            response = requests.post(
                OPENAI_API_URL,
                headers=headers,
                json=req_body,
                timeout=OPENAI_TIMEOUT_SEC,
            )
        except requests.RequestException as e:
            last_exc = e
            if attempt >= OPENAI_MAX_RETRIES:
                break
            wait_sec = min(
                OPENAI_RETRY_MAX_SEC,
                OPENAI_RETRY_BASE_SEC * (2 ** (attempt - 1)),
            ) * (1 + random.uniform(0.0, 0.25))
            print(
                f"OpenAI request error on attempt {attempt}/{OPENAI_MAX_RETRIES}: {type(e).__name__}. "
                f"Retrying in {wait_sec:.1f}s..."
            )
            time.sleep(wait_sec)
            continue

        if response.ok:
            raw = response.json()
            text = extract_json_text_from_openai_response(raw)
            parsed = parse_brief_json(text)
            return normalize_brief(parsed), raw

        err_msg, err_type, err_code = extract_openai_error(response)
        status = response.status_code

        should_retry = status == 429 or status >= 500
        # 429 with exhausted billing/quota should fail fast.
        if status == 429 and err_code == "insufficient_quota":
            should_retry = False

        if should_retry and attempt < OPENAI_MAX_RETRIES:
            retry_after = parse_retry_after(response.headers)
            if retry_after is not None:
                wait_sec = retry_after
            else:
                wait_sec = min(
                    OPENAI_RETRY_MAX_SEC,
                    OPENAI_RETRY_BASE_SEC * (2 ** (attempt - 1)),
                ) * (1 + random.uniform(0.0, 0.25))
            print(
                f"OpenAI API {status} on attempt {attempt}/{OPENAI_MAX_RETRIES} "
                f"(type={err_type}, code={err_code}). Retrying in {wait_sec:.1f}s..."
            )
            time.sleep(wait_sec)
            continue

        raise RuntimeError(
            f"OpenAI API error {status}: {err_msg}"
            + (f" (type={err_type})" if err_type else "")
            + (f" (code={err_code})" if err_code else "")
        )

    raise RuntimeError(f"OpenAI request failed after retries: {type(last_exc).__name__}: {last_exc}")


def build_email_body(
    day: str,
    brief_id: int,
    model: str,
    brief_json: dict[str, Any],
    raw_response: dict[str, Any],
) -> str:
    lines: list[str] = []
    lines.append(f"Daily Brief #{brief_id} | {day}")
    lines.append(f"Model: {model}")

    usage = raw_response.get("usage") if isinstance(raw_response, dict) else None
    if isinstance(usage, dict):
        total_tokens = usage.get("total_tokens")
        cost = usage.get("cost")
        usage_line = "Usage:"
        if total_tokens is not None:
            usage_line += f" tokens={total_tokens}"
        if cost is not None:
            usage_line += f", cost={cost}"
        lines.append(usage_line)

    summary = brief_json.get("summary_lines")
    if isinstance(summary, list) and summary:
        lines.append("")
        lines.append("Summary:")
        for item in summary:
            lines.append(f"- {item}")

    recs = brief_json.get("recommendations")
    if isinstance(recs, list) and recs:
        lines.append("")
        lines.append("Recommendations:")
        for rec in recs:
            if not isinstance(rec, dict):
                continue
            area = rec.get("area") or "general"
            action = rec.get("action") or ""
            reason = rec.get("reason") or ""
            lines.append(f"- [{area}] {action}")
            if reason:
                lines.append(f"  reason: {reason}")

    flags = brief_json.get("red_flags")
    if isinstance(flags, list) and flags:
        lines.append("")
        lines.append("Red flags:")
        for flag in flags:
            if not isinstance(flag, dict):
                continue
            name = flag.get("flag") or "unknown"
            prob = flag.get("probability")
            why = flag.get("why") or ""
            prob_txt = f" ({prob})" if prob is not None else ""
            lines.append(f"- {name}{prob_txt}")
            if why:
                lines.append(f"  why: {why}")

    questions = brief_json.get("questions")
    if isinstance(questions, list) and questions:
        lines.append("")
        lines.append("Questions:")
        for q in questions:
            lines.append(f"- {q}")

    conf = brief_json.get("confidence_overall")
    if conf is not None:
        lines.append("")
        lines.append(f"Confidence overall: {conf}")

    return "\n".join(lines)


def send_brief_to_email(
    day: str,
    brief_id: int,
    model: str,
    brief_json: dict[str, Any],
    raw_response: dict[str, Any],
) -> bool:
    if not RESEND_API_KEY or not RESEND_FROM_EMAIL or not RESEND_TO_EMAIL:
        return False

    recipients = [item.strip() for item in RESEND_TO_EMAIL.split(",") if item.strip()]
    response = requests.post(
        f"{RESEND_API_BASE.rstrip('/')}/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": RESEND_FROM_EMAIL,
            "to": recipients,
            "subject": f"{EMAIL_SUBJECT_PREFIX} {day} brief #{brief_id}",
            "text": build_email_body(day, brief_id, model, brief_json, raw_response),
        },
        timeout=RESEND_TIMEOUT_SEC,
    )
    if not response.ok:
        details = response.text.strip()
        try:
            payload = response.json()
            if isinstance(payload, dict):
                details = str(payload.get("message") or payload)
        except ValueError:
            pass
        raise RuntimeError(f"Resend API error {response.status_code}: {details}")
    return True


def store_brief(
    conn: psycopg.Connection,
    day: str,
    prompt_payload: dict[str, Any],
    brief_json: dict[str, Any],
    raw_response: dict[str, Any],
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_briefs(day, model, prompt_payload, brief_json, raw_response)
            VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
            RETURNING id
            """,
            (
                day,
                OPENAI_MODEL,
                json.dumps(to_jsonable(prompt_payload), ensure_ascii=False),
                json.dumps(to_jsonable(brief_json), ensure_ascii=False),
                json.dumps(to_jsonable(raw_response), ensure_ascii=False),
            ),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        raise RuntimeError("Failed to insert daily_briefs row")
    return int(row[0])


def main():
    if not DB_DSN:
        raise SystemExit("Missing DB_DSN")

    with connect_with_retry(DB_DSN) as conn:
        conn.execute(DDL)
        conn.execute(MIGRATION_SQL)

        day = fetch_target_day(conn)
        today = fetch_today_features(conn, day)
        baseline = fetch_baseline(conn, day)
        deltas = compute_deltas(today, baseline)
        recent_trainings = fetch_recent_trainings(conn, day)

        prompt_payload = {
            "day": day,
            "today_features": today,
            "baseline_14d": baseline,
            "deltas_and_anomalies": deltas,
            "recent_trainings_3": recent_trainings,
            "notes": {
                "baseline_window_days": 14,
                "baseline_days_used": baseline.get("baseline_days", 0),
            },
        }

        try:
            brief_json, raw_response = call_openai(prompt_payload)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            brief_json = build_error_brief(err)
            raw_response = {"error": err}
        brief_id = store_brief(conn, day, prompt_payload, brief_json, raw_response)
        conn.commit()

    try:
        sent = send_brief_to_email(day, brief_id, OPENAI_MODEL, brief_json, raw_response)
        if sent:
            print(f"Email sent for brief #{brief_id}")
        else:
            print("Email send skipped: RESEND_API_KEY, RESEND_FROM_EMAIL or RESEND_TO_EMAIL is not set")
    except Exception as e:
        print(f"Email send failed: {type(e).__name__}: {e}")

    print(f"Generated GPT brief #{brief_id} for {day} into Postgres table daily_briefs")


if __name__ == "__main__":
    main()
