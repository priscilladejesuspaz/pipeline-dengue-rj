-- Migração 001: schema raw e tabelas de entrada do pipeline.
-- Idempotente (IF NOT EXISTS): segura para rodar sobre o banco que já tem as tabelas,
-- que foram criadas manualmente e cujo DDL foi reconstruído a partir do banco real.

CREATE SCHEMA IF NOT EXISTS raw;

-- Notificações de dengue (SINAN). A chave é sintética (SHA1 de campos estáveis, ver extract.py).
CREATE TABLE IF NOT EXISTS raw.sinan_dengue (
    numero_notificacao   text PRIMARY KEY,
    municipio_residencia text,
    data_notificacao     date,
    data_sintomas        date,
    classificacao_final  text,
    hospitalizacao       text,
    evolucao             text,
    carga_em             timestamp DEFAULT now()
);

-- Internações por dengue (SIH/SUS).
CREATE TABLE IF NOT EXISTS raw.sih_dengue (
    numero_aih           text PRIMARY KEY,
    municipio_residencia text,
    data_internacao      date,
    data_saida           date,
    cid_principal        text,
    valor_total_aih      numeric,
    carga_em             timestamp DEFAULT now()
);

-- Estimativas de população (IBGE) por município e ano.
CREATE TABLE IF NOT EXISTS raw.populacao_ibge (
    municipio          text    NOT NULL,
    ano                integer NOT NULL,
    populacao_estimada numeric,
    carga_em           timestamp DEFAULT now(),
    PRIMARY KEY (municipio, ano)
);
