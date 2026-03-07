# Garmin Export Pipeline

ETL-пайплайн для Garmin Connect:
1. выгрузка JSON (`summary`, `sleep`, `stress`, `hrv`, `activities_0_20`) в `data/YYYY-MM-DD`
2. загрузка raw JSON в Postgres (`garmin_raw`)
3. расчет daily features для анализа (`daily_features`)
4. генерация ежедневного GPT-brief (`daily_briefs`)
   Каждый запуск `brief` сохраняется отдельной записью (append-only).

## 1. Требования

- Docker + Docker Compose
- (опционально) Python 3.14 для локального запуска без Docker

## 2. Переменные окружения

Создай `.env` в корне проекта:

```env
GARMIN_EMAIL=your_email
GARMIN_PASSWORD=your_password
OPENAI_API_KEY=your_openai_api_key
TZ=Europe/Moscow
DAYS_BACK=3
OPENAI_MAX_RETRIES=5
OPENAI_RETRY_BASE_SEC=2
OPENAI_RETRY_MAX_SEC=60
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
TELEGRAM_TIMEOUT_SEC=20
```

Файл `.env` уже добавлен в `.gitignore`.

Для OpenRouter можно добавить (рекомендуется для текущего проекта):

```env
OPENAI_API_URL=https://openrouter.ai/api/v1/chat/completions
OPENROUTER_API_KEY=your_openrouter_key
OPENAI_MODEL=openai/gpt-4.1-mini
OPENROUTER_HTTP_REFERER=http://localhost
OPENROUTER_APP_NAME=garmin-export
```

Примечание: если используешь OpenRouter, ключ можно хранить в `OPENROUTER_API_KEY`.
Скрипт также поддерживает fallback на `OPENAI_API_KEY`.
Telegram-отправка включается автоматически, если заданы `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`.

## 3. Запуск через Docker (рекомендуется)

Рабочая директория:

```bash
cd /Users/nkprs/Documents/health/garmin-export
```

### Шаг A. Выгрузка Garmin JSON

```bash
docker compose up --build --abort-on-container-exit garmin_export
```

Количество дней задается через `.env` переменную `DAYS_BACK` (если не задана, используется `3`).

Для разового запуска можно переопределить:

```bash
DAYS_BACK=7 docker compose up --build --abort-on-container-exit garmin_export
```

Для конкретной даты:

```bash
DATE=2026-03-04 docker compose up --build --abort-on-container-exit garmin_export
```

### Шаг B. Загрузка raw JSON в Postgres

```bash
docker compose up --build --abort-on-container-exit loader
```

### Шаг C. Расчет daily features

```bash
docker compose up --build --abort-on-container-exit features
```

### Шаг D. GPT daily brief

Dry run (без вызова OpenAI API):

```bash
OPENAI_DRY_RUN=1 docker compose up --build --abort-on-container-exit brief
```

Боевой запуск (реальный вызов OpenAI):

```bash
OPENAI_DRY_RUN=0 docker compose up --build --abort-on-container-exit brief
```

При `429`/`5xx` сервис делает автоматические ретраи с экспоненциальной паузой.
После успешной генерации brief отправляется в Telegram (если Telegram env настроены).

Для конкретной даты:

```bash
TARGET_DATE=2026-03-04 OPENAI_DRY_RUN=0 docker compose up --build --abort-on-container-exit brief
```

## 4. Полезные проверки в Postgres

Проверить raw:

```bash
docker compose exec -T db psql -U bio -d bio -c "select day, source, fetched_at from garmin_raw order by day desc, source;"
```

Проверить daily features:

```bash
docker compose exec -T db psql -U bio -d bio -c "select * from daily_features order by day desc;"
```

Проверить GPT brief:

```bash
docker compose exec -T db psql -U bio -d bio -c "select id, day, model, generated_at from daily_briefs order by id desc limit 20;"
```

## 5. Локальный запуск через venv (опционально)

```bash
cd /Users/nkprs/Documents/health/garmin-export
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Запуск скриптов локально:

```bash
python app/main.py
python app/load_to_pg.py
python app/features.py
python app/brief.py
```

Важно: для `load_to_pg.py`, `features.py`, `brief.py` нужен доступный Postgres и корректный `DB_DSN`.

## 6. Структура данных

- `garmin_raw`: сырые payload по дням и источникам
- `daily_features`: агрегированные признаки для аналитики
- `daily_briefs`: append-only журнал запусков LLM-анализа (prompt + JSON-brief + raw response)

## 7. Telegram Bot Notes

- Для личного чата с ботом `TELEGRAM_CHAT_ID` обычно положительный числовой id.
- Для группы/супергруппы id обычно начинается с `-100...`.
- Бот должен иметь право отправлять сообщения в этот чат.

## 8. Безопасность секретов

- `docker compose config` печатает итоговый конфиг с подставленными переменными из `.env`.
- Поэтому значения вроде `OPENAI_API_KEY` могут попасть в лог терминала/CI.
- Не запускай `docker compose config` в публичных логах и скриншотах.
- Если ключ уже засветился, перевыпусти его в OpenAI и обнови `.env`.

## 9. Ошибка 429 OpenAI

Если видишь `429 Too Many Requests`, обычно это:

- превышен rate limit (слишком частые запросы за короткий интервал),
- или квота/billing (`insufficient_quota`).

Что уже реализовано в проекте:

- `brief.py` автоматически ретраит `429`/`5xx` с backoff,
- учитывает `Retry-After`, если он есть в ответе API.

Что проверить:

- активен ли billing у API-ключа,
- есть ли лимиты RPM/TPM на выбранную модель,
- не запущено ли несколько `brief`-процессов одновременно.
