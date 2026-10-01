"""DAG: extração (SIH-RD/PySUS) + carga de internações por dengue, UF=RJ, ano 2024.

Uma task mapeada por mês, extração + carga encadeadas (ver dag_populacao_ibge.py sobre o
padrão de dynamic task mapping em cadeia). Mês sem arquivo no catálogo do PySUS (lacuna
conhecida: 2024-07) não falha a DAG:

- `extrair` roda na venv isolada do PySUS, que não tem o Airflow instalado (é por isso
  que ela existe), então não pode levantar AirflowSkipException diretamente — devolve um
  dict com `caminho=None` e o motivo.
- `carregar` roda no ambiente principal (tem Airflow) e, ao ver `caminho=None`, levanta
  AirflowSkipException: a task daquele mês fica "skipped", não "failed", e não derruba a
  DAG run.
"""
import logging
from datetime import datetime

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

PYSUS_PYTHON = "/opt/pysus-venv/bin/python"
MESES = list(range(1, 13))


@dag(
    dag_id="dag_sih_dengue",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sih", "extracao"],
)
def dag_sih_dengue():
    @task.external_python(python=PYSUS_PYTHON, max_active_tis_per_dag=1)  # DuckDB do PySUS: 1 por vez
    def extrair(mes: int, ano: int = 2024, uf: str = "RJ") -> dict:
        from pipeline_dengue.extract_sih import ArquivoIndisponivel, extrair_sih_mes

        try:
            caminho = extrair_sih_mes(ano=ano, mes=mes, uf=uf)
        except ArquivoIndisponivel as e:
            return {"ano": ano, "mes": mes, "caminho": None, "motivo": str(e)}
        return {"ano": ano, "mes": mes, "caminho": str(caminho), "motivo": None}

    @task
    def carregar(info: dict) -> int:
        from pathlib import Path

        from airflow.exceptions import AirflowSkipException

        from pipeline_dengue.load_sih import carregar_sih_mes

        if info["caminho"] is None:
            raise AirflowSkipException(
                f"SIH-RD RJ {info['ano']}-{info['mes']:02d}: {info['motivo']}"
            )
        linhas = carregar_sih_mes(Path(info["caminho"]), info["ano"], info["mes"])
        log.info("raw.sih_dengue %s-%02d: %s linhas", info["ano"], info["mes"], linhas)
        return linhas

    carregar.expand(info=extrair.expand(mes=MESES))


dag_sih_dengue()
