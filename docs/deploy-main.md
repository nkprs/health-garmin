# Автодеплой `main` (минимальный)

Этот вариант делает только одно: при пуше в `main` обновляет код на сервере до `origin/main`.

## 1. Что уже добавлено в репозиторий

- Workflow: `.github/workflows/deploy-main.yml`
- Триггеры:
  - `push` в `main`
  - ручной запуск `workflow_dispatch`

## 2. GitHub Secrets (Repository Settings -> Secrets and variables -> Actions)

Нужно добавить:

- `DEPLOY_HOST` — IP/домен сервера
- `DEPLOY_PORT` — SSH порт (опционально, по умолчанию `22`)
- `DEPLOY_USER` — SSH пользователь
- `DEPLOY_PATH` — путь к проекту на сервере (например `/opt/garmin-export`)
- `DEPLOY_SSH_KEY` — приватный SSH ключ (лучше отдельный deploy key)

## 3. Подготовка сервера

1. Клонировать этот репозиторий на сервер в `DEPLOY_PATH`.
2. Убедиться, что на сервере есть доступ на `git pull` из `origin`.
3. Убедиться, что у `DEPLOY_USER` есть права на директорию проекта.

## 4. Что выполняется на сервере

Workflow выполняет:

```bash
cd "$DEPLOY_PATH"
git fetch origin main
git checkout main
git pull --ff-only origin main
```

## 5. Что можно добавить следующим шагом

После обновления кода можно добавить рестарт приложения, например:

```bash
docker compose up -d --build
```
