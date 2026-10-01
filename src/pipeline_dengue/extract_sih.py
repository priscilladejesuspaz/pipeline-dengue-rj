"""Extração das internações por dengue (SIH-RD, CID A90/A91) por mês, estabelecimentos do RJ.

Origem: espelho S3 do PySUS (grupo RD = "AIH Reduzida"), um Parquet por UF/mês.
Atenção: o arquivo "RJ" reúne internações em estabelecimentos do RJ, não por residência
(em jan/2024, 3 das 670 internações de dengue residem fora do RJ).

`pysus.ftp.sih` devolve vazio no Windows (o filtro de origem compara caminhos com "/" e o
catálogo traz "\\"), então usamos `pysus.sih`, depreciada mas funcional. Se ela for removida,
só `_baixar_mes` precisa mudar.
"""
import warnings
from pathlib import Path

import pandas as pd

DATA_RAW = Path("data/raw")
CIDS_DENGUE = ("A90", "A91")

# Só o que a carga usa, mais campos para reclassificar depois sem rebaixar o arquivo:
# DIAG_SECUN, IDENT/SEQUENCIA (tipo e ordem da AIH) e ANO_CMPT/MES_CMPT (competência).
COLUNAS_ORIGEM = [
    "N_AIH", "MUNIC_RES", "DT_INTER", "DT_SAIDA", "DIAG_PRINC", "VAL_TOT",
    "DIAG_SECUN", "IDENT", "SEQUENCIA", "ANO_CMPT", "MES_CMPT",
]


class ArquivoIndisponivel(RuntimeError):
    """O PySUS não tem o arquivo do mês (ex.: RDRJ2407 ausente do catálogo)."""


def _baixar_mes(ano: int, mes: int, uf: str) -> pd.DataFrame:
    import pysus

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # aviso de depreciação de pysus.sih
        return pysus.sih(
            uf, ano, mes, group="RD", as_dataframe=True, show_progress=False,
            columns=COLUNAS_ORIGEM,
        )


def extrair_sih_mes(ano: int, mes: int, uf: str = "RJ", destino: Path = DATA_RAW) -> Path:
    """Baixa o RD do mês, filtra dengue (CID principal) e AIH principal; salva Parquet."""
    df = _baixar_mes(ano, mes, uf)
    if df.empty:
        raise ArquivoIndisponivel(f"SIH-RD {uf} {ano}-{mes:02d}: nenhum arquivo no catálogo")

    # Filtra na extração: ~1% das linhas do arquivo mensal é dengue.
    df = df[df["DIAG_PRINC"].astype(str).str[:3].isin(CIDS_DENGUE)]
    # IDENT=1 é a AIH principal; IDENT=5 são continuações da mesma internação (longa
    # permanência) e repetem N_AIH, o que quebraria a PK e contaria a internação duas vezes.
    df = df[df["IDENT"] == "1"].reset_index(drop=True)

    repetidos = df["N_AIH"].duplicated().sum()
    if repetidos:
        raise RuntimeError(f"{repetidos} N_AIH repetidos em {uf} {ano}-{mes:02d} após IDENT=1")

    destino.mkdir(parents=True, exist_ok=True)
    saida = destino / f"sih_dengue_{uf.lower()}_{ano}_{mes:02d}.parquet"
    df.to_parquet(saida, index=False)
    return saida


if __name__ == "__main__":
    print(extrair_sih_mes(2024, 1))
