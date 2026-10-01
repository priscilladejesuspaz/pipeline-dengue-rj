"""Extração da população residente por município (IBGE, grupo POPT) do RJ, por ano.

Origem: espelho S3 do PySUS (`pysus.ibge`, grupo POPT), um Parquet nacional por ano.
O código do município vem com 7 dígitos (com dígito verificador), enquanto SINAN e SIH usam 6.
Mantemos o código como vem (raw é espelho da fonte); nos joins, usar `left(municipio, 6)`.

Como em extract_sih.py, usamos a chamada de topo (`pysus.ibge`) porque `pysus.ftp.*` devolve
vazio no Windows.

O espelho do PySUS está errado para 2022 e 2023: o zip do DATASUS desses anos traz um .dbf por
UF além do nacional, e o espelho converteu só o primeiro (PR em 2022, RN em 2023; 399 e 167
linhas). Quando a UF não vem no catálogo, lemos POPT<UF><aa>.dbf direto do zip no FTP.
"""
import ftplib
import io
import warnings
import zipfile
from pathlib import Path

import pandas as pd

DATA_RAW = Path("data/raw")
UF_PREFIXO = {"RJ": "33"}  # prefixo IBGE do código do município
N_MUNICIPIOS = {"RJ": 92}  # trava contra arquivo parcial como os de 2022/2023 do espelho
FTP_POPT = "/dissemin/publicos/IBGE/POPTCU"


def _baixar_ano(ano: int) -> pd.DataFrame:
    import pysus

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # aviso de depreciação de pysus.ibge
        return pysus.ibge(ano, group="POPT", as_dataframe=True, show_progress=False)


def _ler_dbf(conteudo: bytes) -> pd.DataFrame:
    """Lê um .dbf simples (campos de texto/numéricos em ASCII) sem dependência extra."""
    n_reg = int.from_bytes(conteudo[4:8], "little")
    tam_cab = int.from_bytes(conteudo[8:10], "little")
    tam_reg = int.from_bytes(conteudo[10:12], "little")
    campos, pos = [], 32
    while conteudo[pos] != 0x0D:
        campos.append((conteudo[pos : pos + 11].split(bytes(1))[0].decode(), conteudo[pos + 16]))
        pos += 32
    linhas = []
    for i in range(n_reg):
        reg = conteudo[tam_cab + i * tam_reg : tam_cab + (i + 1) * tam_reg]
        off, linha = 1, []  # 1º byte é a marca de exclusão
        for nome, tam in campos:
            linha.append(reg[off : off + tam].decode("latin-1").strip())
            off += tam
        linhas.append(linha)
    return pd.DataFrame(linhas, columns=[c[0] for c in campos])


def _ler_zip_ftp(ano: int, uf: str) -> pd.DataFrame:
    nome = f"POPTBR{ano % 100:02d}.zip"
    buf = io.BytesIO()
    with ftplib.FTP("ftp.datasus.gov.br", timeout=60) as ftp:
        ftp.login()
        ftp.voidcmd("TYPE I")
        ftp.retrbinary(f"RETR {FTP_POPT}/{nome}", buf.write)
    with zipfile.ZipFile(buf) as z:
        membro = f"POPT{uf}{ano % 100:02d}.dbf"
        if membro not in z.namelist():
            raise RuntimeError(f"{membro} não está em {nome}: {z.namelist()[:5]}...")
        return _ler_dbf(z.read(membro))


def extrair_populacao(ano: int, uf: str = "RJ", destino: Path = DATA_RAW) -> Path:
    """Obtém o POPT do ano para a UF (catálogo; FTP se o espelho vier parcial) e salva Parquet."""
    df = _baixar_ano(ano)
    if not df.empty:
        df = df[df["MUNIC_RES"].str.startswith(UF_PREFIXO[uf])]
    if df.empty:
        print(f"POPT {ano}: {uf} ausente no catálogo do PySUS; lendo do zip no FTP")
        df = _ler_zip_ftp(ano, uf)
    df = df.reset_index(drop=True)
    if len(df) != N_MUNICIPIOS[uf]:
        raise RuntimeError(f"POPT {ano}: {len(df)} municípios para {uf}, esperado {N_MUNICIPIOS[uf]}")
    if df["MUNIC_RES"].duplicated().any():
        raise RuntimeError(f"POPT {ano}: município repetido para UF={uf}")

    destino.mkdir(parents=True, exist_ok=True)
    saida = destino / f"populacao_ibge_{uf.lower()}_{ano}.parquet"
    df.to_parquet(saida, index=False)
    return saida


if __name__ == "__main__":
    print(extrair_populacao(2024))
