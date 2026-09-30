import logging
import os
from datetime import datetime

import psycopg2
from airflow.sdk import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="teste_conexao",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["teste", "etapa-1"],
)
def teste_conexao():
    @task
    def select_1():
        conn = psycopg2.connect(
            host="postgres",
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                resultado = cur.fetchone()[0]
        finally:
            conn.close()
        log.info("SELECT 1 retornou %s", resultado)
        if resultado != 1:
            raise ValueError(f"Retorno inesperado: {resultado}")

    select_1()


teste_conexao()
