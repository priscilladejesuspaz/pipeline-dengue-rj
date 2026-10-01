"""Transformações staging/marts: cruza SINAN (notificação) x SIH (internação) x IBGE
(população), grão mensal por município, UF=RJ.

Full refresh (TRUNCATE + INSERT) a cada execução — os volumes são pequenos (~300k/8k/550
linhas) e staging/marts são sempre reconstruídos a partir do raw, não acumulam histórico
próprio. Não depende do PySUS; roda no ambiente principal do Airflow (só psycopg2).
"""
from pipeline_dengue.load import _conectar

SQL_STG_SINAN = """
    INSERT INTO staging.stg_sinan_dengue
        (numero_notificacao, municipio, ano, mes, classificacao_final, hospitalizacao, evolucao)
    SELECT
        numero_notificacao,
        left(municipio_residencia, 6),
        extract(year FROM data_notificacao)::int,
        extract(month FROM data_notificacao)::int,
        classificacao_final, hospitalizacao, evolucao
    FROM raw.sinan_dengue
    WHERE municipio_residencia IS NOT NULL
      AND left(municipio_residencia, 2) = '33'
      AND municipio_residencia <> '330000'
      AND data_notificacao IS NOT NULL
"""

SQL_STG_SIH = """
    INSERT INTO staging.stg_sih_dengue (numero_aih, municipio, ano, mes, valor_total_aih)
    SELECT
        numero_aih,
        left(municipio_residencia, 6),
        extract(year FROM competencia)::int,
        extract(month FROM competencia)::int,
        valor_total_aih
    FROM raw.sih_dengue
    WHERE municipio_residencia IS NOT NULL
"""

SQL_STG_POPULACAO = """
    INSERT INTO staging.stg_populacao (municipio, ano, populacao_estimada)
    SELECT left(municipio, 6), ano, populacao_estimada
    FROM raw.populacao_ibge
"""

SQL_FATO_MENSAL = """
    INSERT INTO marts.fato_dengue_mensal_municipio
        (municipio, ano, mes, notificacoes, internacoes, populacao_estimada,
         taxa_internacao_por_notificacao, taxa_internacao_por_100k)
    WITH notif AS (
        SELECT municipio, ano, mes, count(*) AS notificacoes
        FROM staging.stg_sinan_dengue GROUP BY 1, 2, 3
    ), inter AS (
        SELECT municipio, ano, mes, count(*) AS internacoes
        FROM staging.stg_sih_dengue GROUP BY 1, 2, 3
    )
    SELECT
        coalesce(notif.municipio, inter.municipio) AS municipio,
        coalesce(notif.ano, inter.ano) AS ano,
        coalesce(notif.mes, inter.mes) AS mes,
        coalesce(notif.notificacoes, 0),
        coalesce(inter.internacoes, 0),
        pop.populacao_estimada,
        CASE WHEN coalesce(notif.notificacoes, 0) > 0
             THEN coalesce(inter.internacoes, 0)::numeric / notif.notificacoes END,
        CASE WHEN pop.populacao_estimada > 0
             THEN coalesce(inter.internacoes, 0)::numeric / pop.populacao_estimada * 100000 END
    FROM notif
    FULL OUTER JOIN inter
        ON inter.municipio = notif.municipio AND inter.ano = notif.ano AND inter.mes = notif.mes
    LEFT JOIN staging.stg_populacao pop
        ON pop.municipio = coalesce(notif.municipio, inter.municipio)
       AND pop.ano = coalesce(notif.ano, inter.ano)
"""


def _refrescar(tabela: str, sql_insert: str) -> int:
    """TRUNCATE + INSERT numa transação só: a tabela nunca fica vazia por falha no meio."""
    with _conectar() as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE {tabela}")
        cur.execute(sql_insert)
        return cur.rowcount


def construir_stg_sinan() -> int:
    return _refrescar("staging.stg_sinan_dengue", SQL_STG_SINAN)


def construir_stg_sih() -> int:
    return _refrescar("staging.stg_sih_dengue", SQL_STG_SIH)


def construir_stg_populacao() -> int:
    return _refrescar("staging.stg_populacao", SQL_STG_POPULACAO)


def construir_fato_mensal() -> int:
    return _refrescar("marts.fato_dengue_mensal_municipio", SQL_FATO_MENSAL)
