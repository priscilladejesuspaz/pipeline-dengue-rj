"""DAG: extração (IBGE/PySUS, grupo POPT) + carga de população por município, UF=RJ.

Uma task mapeada por ano (2020-2025), extração + carga encadeadas via dynamic task
mapping (`carregar.expand(caminho=extrair.expand(ano=ANOS))`): cada índice do mapeamento
de `extrair` casa com o mesmo índice em `carregar`, então não precisa zipar ano+caminho
manualmente. Diferente do SIH, aqui não há lacuna esperada — ano sem os 92 municípios do
RJ falha a task mesmo (`extrair_populacao` levanta RuntimeError), não é tratado como skip.
"""
import logging
from datetime import datetime

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

PYSUS_PYTHON = "/opt/pysus-venv/bin/python"
ANOS = list(range(2020, 2026))


@dag(
    dag_id="dag_populacao_ibge",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["populacao", "ibge", "extracao"],
)
def dag_populacao_ibge():
    @task.external_python(python=PYSUS_PYTHON, max_active_tis_per_dag=1)  # DuckDB do PySUS: 1 por vez
    def extrair(ano: int, uf: str = "RJ") -> str:
        from pipeline_dengue.extract_pop import extrair_populacao

        caminho = extrair_populacao(ano=ano, uf=uf)
        return str(caminho)

    @task
    def carregar(caminho: str) -> int:
        from pathlib import Path

        from pipeline_dengue.load_pop import carregar_populacao

        linhas = carregar_populacao(Path(caminho))
        log.info("raw.populacao_ibge: %s linhas", linhas)
        return linhas

    carregar.expand(caminho=extrair.expand(ano=ANOS))


dag_populacao_ibge()
