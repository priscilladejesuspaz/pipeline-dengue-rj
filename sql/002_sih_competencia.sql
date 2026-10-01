-- Migração 002: partição mensal em raw.sih_dengue.
-- `competencia` é o 1º dia do mês do arquivo SIH-RD de origem. A carga mensal apaga e
-- reinsere uma competência inteira, então reprocessar um mês toca só aquela partição.
-- Nula só para linhas anteriores a esta migração (a tabela estava vazia).

ALTER TABLE raw.sih_dengue ADD COLUMN IF NOT EXISTS competencia date;

CREATE INDEX IF NOT EXISTS sih_dengue_competencia_idx ON raw.sih_dengue (competencia);
