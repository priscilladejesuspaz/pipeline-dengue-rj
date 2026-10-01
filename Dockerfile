# Imagem custom do Airflow com o PySUS isolado numa venv separada.
#
# O PySUS fixa versões que colidem com o que o Airflow 3.3.2 fixa no ambiente principal
# (constraints-3.3.2/constraints-3.12.txt): pandas<3.0 vs pandas==3.0.5, typer<0.25 vs
# typer==0.27.2, python-dateutil==2.8.2 vs ==2.9.0.post0. Instalar o PySUS direto no
# ambiente principal rebaixaria pacotes que o próprio Airflow usa (typer é a CLI dele).
#
# Solução: duas instalações.
#   1. Ambiente principal: só pandas + pyarrow + httpx, COM o constraints do Airflow.
#      Precisa disso porque os módulos de extract_*.py/load_*.py importam essas libs no
#      topo do arquivo (pysus é importado tardiamente, dentro das funções, então não
#      precisa estar aqui) — o dag-processor faz esse import ao interpretar as DAGs,
#      mesmo quando a task que de fato chama o PySUS roda em outro interpretador.
#   2. /opt/pysus-venv: venv isolada com o PySUS e suas próprias versões, SEM o
#      constraints do Airflow. As tasks que chamam pysus.* rodam aqui via
#      ExternalPythonOperator (python=/opt/pysus-venv/bin/python).
FROM apache/airflow:3.3.2

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt" \
    pandas pyarrow httpx

# /opt é do root: cria o diretório da venv e entrega ao usuário airflow.
USER root
RUN mkdir /opt/pysus-venv && chown airflow:root /opt/pysus-venv
USER airflow

RUN python -m venv /opt/pysus-venv && \
    /opt/pysus-venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/pysus-venv/bin/pip install --no-cache-dir pysus==2.11.3

ENV PYTHONPATH=/opt/airflow/src
