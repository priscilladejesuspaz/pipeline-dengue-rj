"""Carga idempotente da população IBGE em raw.populacao_ibge (upsert por município + ano)."""
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

from pipeline_dengue.extract_pop import extrair_populacao
from pipeline_dengue.load import _conectar

COLUNAS_RAW = {"MUNIC_RES": "municipio", "ANO": "ano", "POPULACAO": "populacao_estimada"}
COLS = list(COLUNAS_RAW.values())


def carregar_populacao(parquet: Path) -> int:
    """Upsert por (municipio, ano): reprocessar atualiza, nunca duplica."""
    df = pd.read_parquet(parquet).rename(columns=COLUNAS_RAW)[COLS]
    df["ano"] = df["ano"].astype(int)
    df["populacao_estimada"] = pd.to_numeric(df["populacao_estimada"])
    sql = f"""
        INSERT INTO raw.populacao_ibge ({", ".join(COLS)}) VALUES %s
        ON CONFLICT (municipio, ano) DO UPDATE SET
        populacao_estimada = EXCLUDED.populacao_estimada, carga_em = now()
    """
    linhas = list(df.astype(object).itertuples(index=False, name=None))
    with _conectar() as conn, conn.cursor() as cur:
        execute_values(cur, sql, linhas)
    return len(linhas)


def backfill_populacao(anos=range(2020, 2026)) -> dict:
    """Extrai e carrega ano a ano. Falha no primeiro ano sem arquivo (não há lacuna esperada)."""
    carregados = {}
    for ano in anos:
        carregados[ano] = carregar_populacao(extrair_populacao(ano))
        print(f"{ano}: {carregados[ano]} linhas")
    print(f"total: {sum(carregados.values())} linhas em {len(carregados)} anos")
    return carregados


if __name__ == "__main__":
    backfill_populacao()
