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

## Etapas 2 e 3 (extração; fora do Docker)

- Código em `src/pipeline_dengue/` (rodar com `PYTHONPATH=src`, venv em `.venv`, env do `.env`): `extract.py`/`load.py` (SINAN, 301.847 linhas em `raw.sinan_dengue`) e `extract_sih.py`/`load_sih.py` (SIH-RD, `python -m pipeline_dengue.load_sih` faz o backfill de 2024).
- DDL versionado em `sql/` (`001` tabelas raw, `002` coluna `competencia` do SIH); aplicar manualmente: `docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB' < sql/00X.sql`. Não há runner de migração.
- PySUS 2.11.3 no Windows: `pysus.ftp.*` devolve vazio sem erro (o filtro de origem compara caminhos com `/` e o catálogo traz `\`). Usar `pysus.sih(...)` (depreciada). `source="origin"` também devolve 0.
- SIH: carga por mês (DELETE + INSERT da `competencia`). Filtra CID A90/A91 e `IDENT=1` na extração (`IDENT=5` repete `N_AIH`). O arquivo "RJ" é por estabelecimento, não por residência. `data_internacao` pode ser anterior à competência (mín. 2023-10-25).
- **Lacuna conhecida: SIH-RD RJ 2024-07.** `RDRJ2407.dbc` existe no FTP do DATASUS mas não no catálogo do PySUS; `raw.sih_dengue` tem 11 meses (7.932 linhas). O backfill registra a lacuna e segue; recarregar quando o catálogo atualizar.
- População (`extract_pop.py`/`load_pop.py`, `pysus.ibge(ano, group="POPT")`): `raw.populacao_ibge` tem RJ 2020-2025 (92 municípios/ano). O espelho do PySUS está errado para 2022 e 2023 (só PR/RN, 399/167 linhas): nesses anos lê `POPTRJ<aa>.dbf` do zip no FTP; a extração falha se não vier 92 municípios. O código tem 7 dígitos (SINAN/SIH têm 6): nos joins usar `left(municipio, 6)`. 2022 e 2023 têm o mesmo total (16.055.174, o valor do Censo 2022), como vem na fonte.
- Qualidade do SINAN (não tratada ainda): 3 linhas com município de outra UF, 1 nula e 1 com código `330000` (sem população correspondente).
