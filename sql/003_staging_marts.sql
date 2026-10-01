-- Migração 003: schema staging/marts — cruzamento de notificação (SINAN) x internação
-- (SIH) x população (IBGE), grão mensal por município, UF=RJ.
-- Full refresh: as tabelas abaixo são sempre reconstruídas a partir do raw (TRUNCATE +
-- INSERT, ver pipeline_dengue/transform.py), não acumulam histórico próprio.

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Notificações válidas, com ano/mês derivado de data_notificacao e código de município
-- padronizado pra 6 dígitos (SINAN já vem com 6; o left() é só por simetria com stg_populacao).
-- Exclui as linhas sem município de RJ válido: 3 de outra UF, 1 nula, 1 com código 330000
-- (achado documentado no CLAUDE.md — qualidade do SINAN, regra Q1 do plano original).
CREATE TABLE IF NOT EXISTS staging.stg_sinan_dengue (
    numero_notificacao  text PRIMARY KEY,
    municipio           text NOT NULL,
    ano                 integer NOT NULL,
    mes                 integer NOT NULL,
    classificacao_final text,
    hospitalizacao      text,
    evolucao            text
);

-- Internações, com ano/mês derivado de competencia (não de data_internacao: a competência
-- é o mês do arquivo de origem, ver extract_sih.py).
CREATE TABLE IF NOT EXISTS staging.stg_sih_dengue (
    numero_aih      text PRIMARY KEY,
    municipio       text NOT NULL,
    ano             integer NOT NULL,
    mes             integer NOT NULL,
    valor_total_aih numeric
);

-- População por município/ano, código padronizado pra 6 dígitos (raw.populacao_ibge
-- traz 7 — ver extract_pop.py).
CREATE TABLE IF NOT EXISTS staging.stg_populacao (
    municipio          text NOT NULL,
    ano                integer NOT NULL,
    populacao_estimada numeric,
    PRIMARY KEY (municipio, ano)
);

-- Grão: município x ano x mês. FULL OUTER JOIN entre notificação e internação — um
-- município pode ter internação sem notificação registrada (ou vice-versa) nesse mês.
CREATE TABLE IF NOT EXISTS marts.fato_dengue_mensal_municipio (
    municipio                        text    NOT NULL,
    ano                              integer NOT NULL,
    mes                              integer NOT NULL,
    notificacoes                     integer NOT NULL DEFAULT 0,
    internacoes                      integer NOT NULL DEFAULT 0,
    populacao_estimada               numeric,
    taxa_internacao_por_notificacao  numeric,  -- internacoes / notificacoes
    taxa_internacao_por_100k         numeric,  -- internacoes / populacao_estimada * 100000
    PRIMARY KEY (municipio, ano, mes)
);
