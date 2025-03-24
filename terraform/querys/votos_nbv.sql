CREATE TABLE silver.quantitativos_votacao_nulos_brancos_validos
WITH (
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://elections-silver-data/silver/quantitativos_votacao_nulos_brancos_validos/'
) AS
SELECT
  turno,
  uf,
  CASE
    WHEN uf IN ('AM', 'PA', 'RR', 'AP', 'RO', 'AC', 'TO') THEN 'Norte'
    WHEN uf IN ('MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA') THEN 'Nordeste'
    WHEN uf IN ('MT', 'MS', 'GO', 'DF') THEN 'Centro-Oeste'
    WHEN uf IN ('SP', 'RJ', 'ES', 'MG') THEN 'Sudeste'
    WHEN uf IN ('PR', 'RS', 'SC') THEN 'Sul'
    WHEN uf = 'ZZ' THEN 'Exterior'
    ELSE 'Não definido'
  END AS regiao,
  SUM(quantidade_aptos) AS total_eleitores_registrados,
  SUM(quantidade_comparecimento) AS total_comparecimento,
  SUM(quantidade_abstencoes) AS total_abstencoes,
  ROUND(100.0 * SUM(quantidade_abstencoes) / NULLIF(SUM(quantidade_aptos), 0), 2) AS taxa_abstencao_percentual,
  SUM(quantidade_votos_validos) AS total_votos_validos,
  SUM(quantidade_votos_nulos) AS total_votos_nulos,
  SUM(quantidade_votos_brancos) AS total_votos_brancos
FROM db_detalhe_votacao
WHERE ano_eleicao = 2022
GROUP BY turno, uf
ORDER BY regiao, uf, turno;
