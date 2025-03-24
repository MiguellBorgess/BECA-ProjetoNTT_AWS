CREATE TABLE silver.recursosrecebidos_genero_raca
WITH (
  format = 'PARQUET',
  write_compression = 'SNAPPY',
  external_location = 's3://elections-silver-data/silver/recursosrecebidos_genero_raca/'
) AS
SELECT 
    c.cor_raca, 
    c.genero, 
    c.uf,  -- uf vindo de candidatos
    CAST(SUM(f.valor_fp_campanha) AS DECIMAL(18, 2)) AS total_fp_campanha_formatado, 
    CAST(SUM(f.recursos_declarados) AS DECIMAL(18, 2)) AS total_recursos_declarados_formatado
FROM "silver"."db_fundo_partidario" AS f
JOIN "silver"."db_candidatos" AS c
    ON f.partido = c.sigla_partido
    AND f.ano_eleicao = c.ano_eleicao
    -- opcional: AND f.cargo = c.cargo
WHERE c.ano_eleicao = 2022
GROUP BY c.cor_raca, c.genero, c.uf
ORDER BY total_fp_campanha_formatado DESC;
