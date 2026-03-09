# Автодеплой `main` через self-hosted runner

Теперь деплой выполняется не по SSH из GitHub-hosted runner, а прямо на вашем сервере через self-hosted runner.

## 1. Workflow

Файл: `.github/workflows/deploy-main.yml`

Триггеры:

- `push` в `main`
- ручной запуск `workflow_dispatch`

Runner:

- `runs-on: [self-hosted, homelab]`

## 2. Что делает job

Workflow `deploy` выполняет отдельные шаги:

```bash
cd /home/woolf/apps/your-project
git fetch origin main
git reset --hard origin/main
docker compose up -d db pgadmin
docker compose up --build --abort-on-container-exit --exit-code-from garmin_export garmin_export
docker compose up --build --abort-on-container-exit --exit-code-from loader loader
docker compose up --build --abort-on-container-exit --exit-code-from features features
docker compose up --build --abort-on-container-exit --exit-code-from brief brief
```

`db` и `pgadmin` поднимаются отдельно и могут жить параллельно с batch-частью.
`garmin_export`, `loader`, `features`, `brief` выполняются последовательно, поэтому `brief` берет уже актуальные `daily_features`.

## 3. Что важно проверить на сервере

- Runner зарегистрирован в этом репозитории и имеет label `homelab`.
- В директории `/home/woolf/apps/your-project` уже есть git-репозиторий проекта.
- Пользователь runner имеет права на `git` и `docker compose`.

## 4. Настройка пути проекта

Если путь отличается, поменяйте строку `cd /home/woolf/apps/your-project` в workflow.
