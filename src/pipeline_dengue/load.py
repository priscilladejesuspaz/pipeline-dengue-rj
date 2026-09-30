"""Carga idempotente do Parquet de data/raw em raw.sinan_dengue."""
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from pipeline_dengue.extract import COLUNAS_RAW, extrair_dengue

COLS = ["numero_notificacao", *COLUNAS_RAW.values()]


def _conectar():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ.get("POSTGRES_DB", "dengue_rj"),
    )


def carregar(parquet: Path, lote: int = 10_000) -> int:
    """Upsert por numero_notificacao: reprocessar atualiza, nunca duplica."""
    df = pd.read_parquet(parquet).rename(columns=COLUNAS_RAW)[COLS]
    df = df.astype(object).where(df.notna(), None)  # NaN -> NULL
    sql = f"""
        INSERT INTO raw.sinan_dengue ({", ".join(COLS)}) VALUES %s
        ON CONFLICT (numero_notificacao) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in COLS[1:])}, carga_em = now()
    """
    with _conectar() as conn, conn.cursor() as cur:  # transação única: tudo ou nada
        linhas = list(df.itertuples(index=False, name=None))
        for i in range(0, len(linhas), lote):
            execute_values(cur, sql, linhas[i : i + lote])
    return len(linhas)


if __name__ == "__main__":
    print(carregar(extrair_dengue()), "linhas processadas")
