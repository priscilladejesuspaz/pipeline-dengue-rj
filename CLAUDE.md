# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

Early-stage data pipeline for dengue data (Rio de Janeiro) built on Apache Airflow 3.3.2 + Postgres 16, run via Docker Compose. Stage 1 (stack up, test DAG) and stages 2-3 (SINAN/SIH/população extraction, run by hand outside Docker) are done. Stage 4, orchestration, is in progress: `dags/dag_sinan_dengue.py`, `dags/dag_populacao_ibge.py` and `dags/dag_sih_dengue.py` wrap the extract/load code as real Airflow DAGs, each with a custom image (see below). All three DAGs were run end to end in Docker (população 6 anos x 92 municípios; SINAN 301.847 linhas; SIH 11 meses + 2024-07 `skipped`). There is no other application code, test suite, or linter yet; `plugins/` is empty. Comments in `docker-compose.yaml`/`Dockerfile` are in Portuguese; keep to that language for consistency.

## Commands

```
cp .env.example .env            # then change POSTGRES_PASSWORD (.env is gitignored); optionally set AIRFLOW_JWT_SECRET
docker compose build            # builds the custom image (Dockerfile) used by all airflow-* services
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
- All five `airflow-*` services now `build: .` (the `Dockerfile`) instead of pulling `apache/airflow:3.3.2` directly. Host `./dags`, `./logs`, `./src` and `./data` are bind-mounted into `/opt/airflow/`.
- `src/pipeline_dengue/` ships inside every Airflow container (`./src:/opt/airflow/src`, `ENV PYTHONPATH=/opt/airflow/src` in the Dockerfile), so DAGs import it with `from pipeline_dengue.extract import ...` etc., same as running it locally with `PYTHONPATH=src`.

### PySUS vs. Airflow dependency conflict (why there's a custom image)

PySUS 2.11.3 pins versions that collide with what Airflow 3.3.2 pins in its own constraints file (`constraints-3.3.2/constraints-3.12.txt`): `pandas<3.0` vs. Airflow's `pandas==3.0.5`, `typer<0.25` vs. `typer==0.27.2` (Airflow's own CLI uses `typer`), `python-dateutil==2.8.2` vs. `==2.9.0.post0`. Installing PySUS straight into the main Airflow environment would downgrade packages Airflow itself depends on. The `pyreaddbc` C-extension PySUS needs is *not* the problem — it ships a `manylinux` wheel for cp312, no compiler needed.

Resolved with two separate installs in the `Dockerfile`, not one:
1. **Main environment** (`airflow` user): `pandas` + `pyarrow` + `httpx`, installed *with* Airflow's own constraints file. Needed because `extract_*.py`/`load_*.py` import these at module top level, and the `dag-processor` imports the module when parsing the DAG — even though the actual PySUS call happens in a different interpreter (`pysus` itself is imported lazily, inside the functions, so it's *not* needed here).
2. **`/opt/pysus-venv`**: isolated venv with only `pysus==2.11.3`, installed *without* any constraints — gets whatever versions it wants. Tasks that actually call `pysus.*` (the `extrair_*` functions) run here via `@task.external_python(python="/opt/pysus-venv/bin/python")`. Tasks that only call `carregar_*` (pandas + psycopg2, no PySUS) run as normal `@task` in the main environment.

`ExternalPythonOperator` pickles the task callable by module reference and re-imports it in the target interpreter, so the DAG-side wrapper functions must be plain functions (no Airflow context) and `pipeline_dengue` must be importable in both environments — handled by `PYTHONPATH=/opt/airflow/src` applying to every interpreter in the container, including `/opt/pysus-venv/bin/python`.

### DAG pattern: SIH's known gap (2024-07)

`dag_sih_dengue.py` maps one task pair per month. The `extrair` task (runs in `pysus-venv`, no Airflow installed there) can't raise `AirflowSkipException` itself, so on `ArquivoIndisponivel` it returns a plain dict (`{"caminho": None, "motivo": ...}`) instead. The downstream `carregar` task (main env, Airflow available) raises `AirflowSkipException` when it sees `caminho is None` — that month's task shows as **skipped**, not failed, and doesn't fail the DAG run. Same chained dynamic-task-mapping pattern (`carregar.expand(info=extrair.expand(mes=MESES))`) is used in `dag_populacao_ibge.py`, without the skip handling (no gap expected in POPT 2020-2025; a bad year should fail loudly).

## Gotchas

- `&airflow-env` passes `POSTGRES_USER/PASSWORD/DB/HOST` to every Airflow container so DAG code can reach the warehouse via `os.environ` (host `postgres`; `POSTGRES_HOST=postgres` added for `pipeline_dengue.load._conectar()`, which defaults to `localhost`).
- With `LocalExecutor`, tasks run inside the scheduler container and call the Execution API, so `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` must point to `http://airflow-api-server:8080/execution/` (the default `localhost` gives `Connection refused` and the task fails before running, with an empty task log; the cause is in the scheduler logs).
- `AIRFLOW__API_AUTH__JWT_SECRET` is fixed in `&airflow-env` (from `AIRFLOW_JWT_SECRET`, with a dev-only default) so all services sign/verify the same tokens. `FERNET_KEY` is still unset.
- The metadata DB is named `airflow` on the same Postgres, but `POSTGRES_DB` only creates `dengue_rj`; the `airflow` DB was created manually (`docker compose exec postgres createdb -U $POSTGRES_USER airflow`). A fresh `dengue_pgdata` volume needs this again before `airflow-init` succeeds.
- `psycopg2` is in the stock image; `pandas`/`pyarrow`/`httpx` are now installed in the custom image's main env (see above) — no more need for `_PIP_ADDITIONAL_REQUIREMENTS`.
- Use `@task.external_python`, NOT `from airflow.providers.standard.decorators.external_python import external_python_task`: o Airflow reenvia o código da função para a venv e só remove o decorador na forma `@task.external_python`; com o import do provider a venv quebra com `NameError: external_python_task is not defined`.
- Nada do escopo do módulo da DAG (constantes, imports do topo) pode aparecer no corpo nem na assinatura (valores padrão) de uma `@task.external_python`: só o código da função vai para a venv, então `ano: int = ANO` dá `NameError: name 'ANO' is not defined`. Use literais e importe dentro da função.
- O PySUS guarda config num DuckDB (`~/pysus/config.db`) que aceita um processo por vez: tasks mapeadas que chamam o PySUS precisam de `max_active_tis_per_dag=1` (já em `dag_populacao_ibge` e `dag_sih_dengue`), senão falham com `Conflicting lock`.
- `/opt` é do root: o Dockerfile cria `/opt/pysus-venv` como root e entrega ao usuário `airflow` antes de criar a venv.
- `.gitignore` ignores `.env`, `data/`, `logs/` and `__pycache__/`.

## Etapas 2 e 3 (extração; fora do Docker)

- Código em `src/pipeline_dengue/` (rodar com `PYTHONPATH=src`, venv em `.venv`, env do `.env`): `extract.py`/`load.py` (SINAN, 301.847 linhas em `raw.sinan_dengue`) e `extract_sih.py`/`load_sih.py` (SIH-RD, `python -m pipeline_dengue.load_sih` faz o backfill de 2024).
- DDL versionado em `sql/` (`001` tabelas raw, `002` coluna `competencia` do SIH); aplicar manualmente: `docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB' < sql/00X.sql`. Não há runner de migração.
- PySUS 2.11.3 no Windows: `pysus.ftp.*` devolve vazio sem erro (o filtro de origem compara caminhos com `/` e o catálogo traz `\`). Usar `pysus.sih(...)` (depreciada). `source="origin"` também devolve 0.
- SIH: carga por mês (DELETE + INSERT da `competencia`). Filtra CID A90/A91 e `IDENT=1` na extração (`IDENT=5` repete `N_AIH`). O arquivo "RJ" é por estabelecimento, não por residência. `data_internacao` pode ser anterior à competência (mín. 2023-10-25).
- **Lacuna conhecida: SIH-RD RJ 2024-07.** `RDRJ2407.dbc` existe no FTP do DATASUS mas não no catálogo do PySUS; `raw.sih_dengue` tem 11 meses (7.932 linhas). O backfill registra a lacuna e segue; recarregar quando o catálogo atualizar. Na DAG (`dag_sih_dengue.py`), esse mês fica `skipped`, não `failed` — ver seção de arquitetura acima.
- População (`extract_pop.py`/`load_pop.py`, `pysus.ibge(ano, group="POPT")`): `raw.populacao_ibge` tem RJ 2020-2025 (92 municípios/ano). O espelho do PySUS está errado para 2022 e 2023 (só PR/RN, 399/167 linhas): nesses anos lê `POPTRJ<aa>.dbf` do zip no FTP; a extração falha se não vier 92 municípios. O código tem 7 dígitos (SINAN/SIH têm 6): nos joins usar `left(municipio, 6)`. 2022 e 2023 têm o mesmo total (16.055.174, o valor do Censo 2022), como vem na fonte.
- Qualidade do SINAN (não tratada ainda): 3 linhas com município de outra UF, 1 nula e 1 com código `330000` (sem população correspondente).
- Staging/marts (`sql/003_staging_marts.sql`, `transform.py`, `dag_marts_dengue`; full refresh, rodar após as DAGs raw): `marts.fato_dengue_mensal_municipio` tem 1.026 linhas (2023-12 a 2024-12). 40 linhas não têm população: são internações de residentes de outras UFs (35 municípios, 47 internações), porque `stg_sih_dengue` não filtra município `33*` como o SINAN faz; a taxa por 100k fica nula nelas. Decisão pendente: filtrar ou manter.
