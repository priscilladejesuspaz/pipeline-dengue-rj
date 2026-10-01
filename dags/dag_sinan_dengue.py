"""DAG: extração (SINAN) + carga de notificações de dengue, UF de residência = RJ.

A extração roda na venv isolada do PySUS (`/opt/pysus-venv`, ver Dockerfile) porque os
pins do PySUS colidem com os que o Airflow 3.3.2 fixa no ambiente principal (pandas,
typer, python-dateutil). A carga roda no ambiente principal: só precisa de
pandas/psycopg2, que não têm esse conflito.
"""
import logging
from datetime import datetime

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

PYSUS_PYTHON = "/opt/pysus-venv/bin/python"


@dag(
    dag_id="dag_sinan_dengue",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sinan", "extracao"],
)
def dag_sinan_dengue():
    @task.external_python(python=PYSUS_PYTHON)
    def extrair(ano: int = 2024, uf: str = "RJ") -> str:
        from pipeline_dengue.extract import extrair_dengue

        caminho = extrair_dengue(ano=ano, uf=uf)
        return str(caminho)

    @task
    def carregar(caminho: str) -> int:
        from pathlib import Path

        from pipeline_dengue.load import carregar as carregar_parquet

        linhas = carregar_parquet(Path(caminho))
        log.info("raw.sinan_dengue: %s linhas", linhas)
        return linhas

    carregar(extrair())


dag_sinan_dengue()
