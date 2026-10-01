"""Carga mensal do Parquet do SIH em raw.sih_dengue, por partição (competência)."""
from datetime import date
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

from pipeline_dengue.extract_sih import ArquivoIndisponivel, extrair_sih_mes
from pipeline_dengue.load import _conectar

COLUNAS_RAW = {
    "N_AIH": "numero_aih",
    "MUNIC_RES": "municipio_residencia",
    "DT_INTER": "data_internacao",
    "DT_SAIDA": "data_saida",
    "DIAG_PRINC": "cid_principal",
    "VAL_TOT": "valor_total_aih",
}
COLS = [*COLUNAS_RAW.values(), "competencia"]


def carregar_sih_mes(parquet: Path, ano: int, mes: int) -> int:
    """Apaga e reinsere a competência inteira numa transação: reprocessar não duplica
    e remove o que deixou de existir na origem. Outros meses não são tocados."""
    df = pd.read_parquet(parquet).rename(columns=COLUNAS_RAW)[list(COLUNAS_RAW.values())]
    for c in ("data_internacao", "data_saida"):
        df[c] = pd.to_datetime(df[c], format="%Y%m%d", errors="coerce").dt.date
    df["valor_total_aih"] = pd.to_numeric(df["valor_total_aih"], errors="coerce")
    df["competencia"] = date(ano, mes, 1)
    df = df.astype(object).where(df.notna(), None)  # NaN/NaT -> NULL

    linhas = list(df.itertuples(index=False, name=None))
    with _conectar() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM raw.sih_dengue WHERE competencia = %s", (date(ano, mes, 1),))
        if linhas:
            execute_values(
                cur, f"INSERT INTO raw.sih_dengue ({', '.join(COLS)}) VALUES %s", linhas
            )
    return len(linhas)


def backfill_sih(ano: int = 2024, meses=range(1, 13)) -> dict:
    """Extrai e carrega mês a mês. Mês sem arquivo no catálogo vira lacuna, não aborta."""
    carregados, lacunas = {}, []
    for mes in meses:
        try:
            parquet = extrair_sih_mes(ano, mes)
        except ArquivoIndisponivel as e:
            print(f"LACUNA {ano}-{mes:02d}: {e}")
            lacunas.append(mes)
            continue
        carregados[mes] = carregar_sih_mes(parquet, ano, mes)
        print(f"{ano}-{mes:02d}: {carregados[mes]} linhas")
    print(f"total: {sum(carregados.values())} linhas em {len(carregados)} meses; lacunas: {lacunas}")
    return {"carregados": carregados, "lacunas": lacunas}


if __name__ == "__main__":
    backfill_sih()
