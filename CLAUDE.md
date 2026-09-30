# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

Early-stage data pipeline for dengue data (Rio de Janeiro) built on Apache Airflow 3.3.2 + Postgres 16, run via Docker Compose. Stage 1 is done: the stack runs and `dags/teste_conexao.py` (single task, `SELECT 1` against the `dengue_rj` warehouse via `psycopg2`) runs successfully. There is no other application code, test suite, linter, or README yet; `plugins/` is empty. Comments in `docker-compose.yaml` are in Portuguese; keep to that language for consistency.

## Commands

```
cp .env.example .env            # then change POSTGRES_PASSWORD (.env is gitignored); optionally set AIRFLOW_JWT_SECRET
docker compose up airflow-init  # runs `airflow db migrate` (one-shot)
docker compose up -d            # postgres + api-server, scheduler, dag-processor, triggerer
docker compose logs -f airflow-scheduler
docker compose down             # add -v to also drop the dengue_pgdata volume
```

Airflow UI: http://localhost:8080 (user `airflow`, via SimpleAuthManager; the generated password is printed in the api-server logs). Postgres is exposed on localhost:5432.

Test a single DAG/task without the scheduler:
`docker compose run --rm airflow-scheduler dags test <dag_id>` (the service `command` is overridden by the args).
Trigger a real run: `docker compose exec airflow-scheduler airflow dags trigger <dag_id>`; check parse errors with `docker compose exec airflow-dag-processor airflow dags list-import-errors`. DAGs start paused.

## Architecture

- `docker-compose.yaml` defines a `postgres` service that serves as both the pipeline's data warehouse (`POSTGRES_DB`, default `dengue_rj`) and Airflow's metadata DB.
- All Airflow services share one env block via the YAML anchor `&airflow-env` / `*airflow-env`; change Airflow config there once. Executor is `LocalExecutor` (no worker/redis).
- Services: `airflow-init` (db migrate), `api-server` (UI, :8080), `scheduler`, `dag-processor` (parses `/opt/airflow/dags`), `triggerer`. Airflow 3 splits the DAG processor from the scheduler, so DAG parse errors show up in `dag-processor` logs.
- Host `./dags` and `./logs` are bind-mounted into `/opt/airflow/`. `./plugins` exists but is not mounted yet.

## Gotchas

- `&airflow-env` passes `POSTGRES_USER/PASSWORD/DB` to every Airflow container so DAG code can reach the warehouse via `os.environ` (host `postgres`).
- With `LocalExecutor`, tasks run inside the scheduler container and call the Execution API, so `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` must point to `http://airflow-api-server:8080/execution/` (the default `localhost` gives `Connection refused` and the task fails before running, with an empty task log; the cause is in the scheduler logs).
- `AIRFLOW__API_AUTH__JWT_SECRET` is fixed in `&airflow-env` (from `AIRFLOW_JWT_SECRET`, with a dev-only default) so all services sign/verify the same tokens. `FERNET_KEY` is still unset.
- The metadata DB is named `airflow` on the same Postgres, but `POSTGRES_DB` only creates `dengue_rj`; the `airflow` DB was created manually (`docker compose exec postgres createdb -U $POSTGRES_USER airflow`). A fresh `dengue_pgdata` volume needs this again before `airflow-init` succeeds.
- `psycopg2` is in the stock image, but other dependencies (e.g. pandas) are not; a custom Dockerfile or `_PIP_ADDITIONAL_REQUIREMENTS` will be needed.
- `.gitignore` ignores `.env`, `data/`, `logs/` and `__pycache__/`.
