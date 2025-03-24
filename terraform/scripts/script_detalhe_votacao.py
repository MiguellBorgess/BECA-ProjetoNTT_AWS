import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from awsglue.job import Job
import boto3

# Configuração do Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)

s3_input_path = "s3://elections-bronze-data/detalhe_votacao.csv"

# Esquema completo, incluindo colunas que serão removidas
schema = StructType([
    StructField("ano_eleicao", IntegerType(), True),
    StructField("regiao", StringType(), True),
    StructField("zona", IntegerType(), True),
    StructField("cargo", StringType(), True),
    StructField("codigo_municipio", IntegerType(), True),
    StructField("municipio", StringType(), True),
    StructField("turno", IntegerType(), True),
    StructField("uf", StringType(), True),
    StructField("quantidade_votos_brancos", IntegerType(), True),
    StructField("quantidade_votos_legenda_validos", IntegerType(), True),
    StructField("quantidade_comparecimento", IntegerType(), True),
    StructField("quantidade_votos_anulados", IntegerType(), True),
    StructField("quantidade_votos_nominais_validos", IntegerType(), True),
    StructField("quantidade_secoes_agregadas", IntegerType(), True),
    StructField("quantidade_votos_validos", IntegerType(), True),
    StructField("quantidade_aptos_totalizados", IntegerType(), True),
    StructField("quantidade_total_secoes", IntegerType(), True),
    StructField("quantidade_votos_nulos", IntegerType(), True),
    StructField("quantidade_votos_totais", IntegerType(), True),
    StructField("quantidade_aptos", IntegerType(), True),
    StructField("quantidade_secoes_principais", IntegerType(), True),
    StructField("quantidade_abstencoes", IntegerType(), True),
    StructField("quantidade_total_votos_legenda_validos", IntegerType(), True),
    StructField("quantidade_votos_anulados_separado", IntegerType(), True),  # Será removida
    StructField("quantidade_votos_nulos_tecnico", IntegerType(), True),
    StructField("quantidade_total_votos_anulados_subjudice", IntegerType(), True),
    StructField("quantidade_votos_concorrentes", IntegerType(), True),
    StructField("quantidade_total_votos_nulos", IntegerType(), True),
    StructField("quantidade_votos_nominais_anulados", IntegerType(), True),  # Será removida
    StructField("quantidade_votos_legenda_anulados", IntegerType(), True),  # Será removida
    StructField("quantidade_votos_nominais_anulados_subjudice", IntegerType(), True),  # Será removida
    StructField("quantidade_votos_legenda_anulados_subjudice", IntegerType(), True),  # Será removida
    StructField("quantidade_secoes_nao_instaladas", IntegerType(), True),
    StructField("quantidade_secoes_instaladas", IntegerType(), True),
    StructField("data_carga", TimestampType(), True)  # Será removida
])

# Aplicando o esquema ao DataFrame
data_frame = spark.read.csv(
    s3_input_path,
    header=True,
    schema=schema,
    sep=";",
    encoding="ISO-8859-1"
)

# Lista de colunas para remover (já renomeadas)
colunas_para_remover = [
    "quantidade_votos_anulados_separado",
    "quantidade_votos_nominais_anulados",
    "quantidade_votos_legenda_anulados",
    "quantidade_votos_nominais_anulados_subjudice",
    "quantidade_votos_legenda_anulados_subjudice",
    "data_carga"
]

# Removendo as colunas específicas
data_frame = data_frame.drop(*colunas_para_remover)

# Removendo dados duplicados
data_frame = data_frame.dropDuplicates()

# Substituindo valores nulos por "NA"
data_frame = data_frame.na.fill("NA")

# Convertendo para DynamicFrame
dyf = DynamicFrame.fromDF(data_frame, glueContext, "dyf")

# Salvando os dados no bucket S3
s3_output_path = "s3://elections-silver-data/output/db_detalhe_votacao"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
