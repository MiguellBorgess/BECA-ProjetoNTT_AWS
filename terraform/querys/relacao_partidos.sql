CREATE TABLE silver.partidosrecebemfinanciamentopublico
WITH (
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://elections-silver-data/silver/partidosrecebemfinanciamentopublico/'
) AS
--correlação entre os partidos que receberam mais financiamento público e o número de candidatos eleitos
 SELECT 
    f.partido, 
    CAST(SUM(f.valor_fp_campanha) AS DECIMAL(18, 2)) AS total_fp_campanha_formatado, 
    SUM(c.quantidade_candidatos_eleitos) AS total_candidatos_eleitos
FROM "silver"."db_fundo_partidario" AS f
JOIN "silver"."db_candidatos" AS c
ON f.partido = c.sigla_partido
WHERE c.ano_eleicao = f.ano_eleicao
GROUP BY f.partido
ORDER BY total_fp_campanha_formatado DESC;
