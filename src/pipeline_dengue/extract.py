"""Extração das notificações de dengue (SINAN) por UF de RESIDÊNCIA.

Origem: OpenDataSUS via PySUS (`pysus.saude.arboviroses`). A origem FTP
(`pysus.ftp.sinan`) devolveu lista vazia sem erro na versão 2.11.3, por isso não é usada.
O arquivo do Ministério é nacional (DENGBR<aa>.csv.zip); filtramos a UF ao ler.
"""
import hashlib
from pathlib import Path

import httpx
import pandas as pd

DATA_RAW = Path("data/raw")
UF_RESIDENCIA = {"RJ": "33"}  # código IBGE da UF -> coluna SG_UF (residência)

# Campos que a vigilância NÃO revisa depois; formam a chave sintética, já que este CSV
# não traz NU_NOTIFIC. Não incluir classificação, evolução, hospitalização ou datas de
# encerramento/óbito: mudariam a chave quando o registro fosse atualizado.
CAMPOS_CHAVE = [
    "TP_NOT", "ID_AGRAVO", "DT_NOTIFIC", "SEM_NOT", "NU_ANO", "SG_UF_NOT", "ID_MUNICIP",
    "ID_REGIONA", "ID_UNIDADE", "DT_SIN_PRI", "SEM_PRI", "ANO_NASC", "NU_IDADE_N", "CS_SEXO",
    "CS_GESTANT", "CS_RACA", "CS_ESCOL_N", "SG_UF", "ID_MN_RESI", "ID_RG_RESI", "ID_PAIS",
    "ID_OCUPA_N", "FEBRE", "MIALGIA", "CEFALEIA", "EXANTEMA", "VOMITO", "NAUSEA", "DOR_COSTAS",
    "CONJUNTVIT", "ARTRITE", "ARTRALGIA", "PETEQUIA_N", "LEUCOPENIA", "LACO", "DOR_RETRO",
    "DIABETES", "HEMATOLOG", "HEPATOPAT", "RENAL", "HIPERTENSA", "ACIDO_PEPT", "AUTO_IMUNE",
    "MUNICIPIO", "TPAUTOCTO", "COUFINF", "COPAISINF", "COMUNINF", "CLINC_CHIK", "MANI_HEMOR",
    "EPISTAXE", "GENGIVO", "METRO", "PETEQUIAS", "HEMATURA", "SANGRAM", "LACO_N",
    "PLAQ_MENOR", "DT_DIGITA",
]

# Colunas de origem -> colunas de raw.sinan_dengue
COLUNAS_RAW = {
    "ID_MN_RESI": "municipio_residencia",
    "DT_NOTIFIC": "data_notificacao",
    "DT_SIN_PRI": "data_sintomas",
    "CLASSI_FIN": "classificacao_final",
    "HOSPITALIZ": "hospitalizacao",
    "EVOLUCAO": "evolucao",
}


def _url_arquivo(ano: int) -> str:
    # Import tardio: o PySUS imprime um banner e cria diretório de cache ao ser importado.
    import pysus

    nome = f"DENGBR{ano % 100:02d}.csv.zip"
    # `year=` é ignorado por essa origem: lista todos os anos e escolhemos pelo nome.
    achados = [f for f in pysus.saude.arboviroses(download=False) if f.name == nome]
    if not achados:
        raise RuntimeError(f"{nome} não encontrado no OpenDataSUS via PySUS")
    return achados[0].path


def _baixar_zip(ano: int, destino: Path) -> Path:
    zip_path = destino / f"DENGBR{ano % 100:02d}.csv.zip"
    if zip_path.exists():
        return zip_path
    tmp = zip_path.with_suffix(".part")
    with httpx.stream("GET", _url_arquivo(ano), follow_redirects=True, timeout=None) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for bloco in r.iter_bytes(1 << 20):
                f.write(bloco)
    tmp.rename(zip_path)
    return zip_path


def gerar_chave(df: pd.DataFrame) -> pd.Series:
    """SHA1 dos campos estáveis: determinística, então reprocessar gera as mesmas chaves."""
    base = df[CAMPOS_CHAVE].fillna("").agg("|".join, axis=1)
    return base.map(lambda s: hashlib.sha1(s.encode()).hexdigest())


def extrair_dengue(ano: int = 2024, uf: str = "RJ", destino: Path = DATA_RAW) -> Path:
    """Baixa o CSV nacional, filtra a UF de residência e salva Parquet em data/raw."""
    destino.mkdir(parents=True, exist_ok=True)
    zip_path = _baixar_zip(ano, destino)

    pedacos = []
    for chunk in pd.read_csv(zip_path, dtype=str, encoding="latin-1", chunksize=500_000):
        pedacos.append(chunk[chunk["SG_UF"] == UF_RESIDENCIA[uf]])
    df = pd.concat(pedacos, ignore_index=True)
    if df.empty:
        raise RuntimeError(f"0 linhas para UF={uf} em {zip_path.name}; abortando")

    df["numero_notificacao"] = gerar_chave(df)
    # Notificações repetidas (mesmos campos estáveis) contam uma vez; ordem fixa p/ ser reprodutível.
    df = df.sort_values(["numero_notificacao", "DT_DIGITA"]).drop_duplicates(
        "numero_notificacao", keep="last"
    )

    saida = destino / f"sinan_dengue_{uf.lower()}_{ano}.parquet"
    df.to_parquet(saida, index=False)
    return saida


if __name__ == "__main__":
    print(extrair_dengue())
