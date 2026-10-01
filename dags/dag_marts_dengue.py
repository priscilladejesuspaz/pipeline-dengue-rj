"""DAG: staging + marts — cruza SINAN x SIH x população, grão mensal por município (RJ).

Full refresh em cada task (TRUNCATE + INSERT, ver transform.py): staging/marts não
acumulam histórico próprio, são sempre reconstruídos a partir do raw corrente. Roda
inteiramente no ambiente principal do Airflow — nenhuma task aqui chama PySUS, só SQL via
psycopg2, então não precisa de ExternalPythonOperator.

Assume que raw.sinan_dengue/raw.sih_dengue/raw.populacao_ibge já estão carregados
(dag_sinan_dengue, dag_sih_dengue, dag_populacao_ibge) — não dispara essas DAGs sozinha.
"""
import logging
from datetime import datetime

from airflow.sdk import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="dag_marts_dengue",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["staging", "marts"],
)
def dag_marts_dengue():
    @task
    def stg_sinan() -> int:
        from pipeline_dengue.transform import construir_stg_sinan

        linhas = construir_stg_sinan()
        log.info("staging.stg_sinan_dengue: %s linhas", linhas)
        return linhas

    @task
    def stg_sih() -> int:
        from pipeline_dengue.transform import construir_stg_sih

        linhas = construir_stg_sih()
        log.info("staging.stg_sih_dengue: %s linhas", linhas)
        return linhas

    @task
    def stg_populacao() -> int:
        from pipeline_dengue.transform import construir_stg_populacao

        linhas = construir_stg_populacao()
        log.info("staging.stg_populacao: %s linhas", linhas)
        return linhas

    @task
    def fato_mensal(*_deps) -> int:
        from pipeline_dengue.transform import construir_fato_mensal

        linhas = construir_fato_mensal()
        log.info("marts.fato_dengue_mensal_municipio: %s linhas", linhas)
        return linhas

    fato_mensal(stg_sinan(), stg_sih(), stg_populacao())


dag_marts_dengue()
