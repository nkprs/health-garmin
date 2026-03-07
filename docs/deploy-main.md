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

```bash
cd /home/woolf/apps/your-project
git fetch origin main
git reset --hard origin/main
docker compose up -d --build
```

## 3. Что важно проверить на сервере

- Runner зарегистрирован в этом репозитории и имеет label `homelab`.
- В директории `/home/woolf/apps/your-project` уже есть git-репозиторий проекта.
- Пользователь runner имеет права на `git` и `docker compose`.

## 4. Настройка пути проекта

Если путь отличается, поменяйте строку `cd /home/woolf/apps/your-project` в workflow.
