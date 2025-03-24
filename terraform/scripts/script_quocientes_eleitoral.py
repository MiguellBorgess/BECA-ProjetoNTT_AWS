import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from awsglue.job import Job
import boto3

# Configuração do Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)

# Caminho para o arquivo de entrada no S3
s3_input_path = "s3://elections-bronze-data/quociente_eleitoral.csv"

# Esquema completo, incluindo colunas que serão removidas
schema = StructType([
    StructField("ano_eleicao", IntegerType(), True),
    StructField("regiao", StringType(), True),
    StructField("cargo", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("turno", IntegerType(), True),
    StructField("uf", StringType(), True),
    StructField("coligacao", StringType(), True),
    StructField("qtd_vagas_qe", IntegerType(), True),
    StructField("qtd_votos_legenda_qe", IntegerType(), True),
    StructField("qtd_votos_nominais_qe", IntegerType(), True),
    StructField("qtd_votos_validos_qe", IntegerType(), True),
    StructField("valor_qe", FloatType(), True),
    StructField("qtd_votos_legenda_qp", IntegerType(), True),
    StructField("qtd_votos_nominais_qp", IntegerType(), True),
    StructField("qtd_vagas_preenchidas_qp", IntegerType(), True),  # Será removida
    StructField("qtd_vagas_media_qp", IntegerType(), True),
    StructField("qtd_vagas_obtidas_qp", IntegerType(), True),  # Será removida
    StructField("qtd_votos_coligacao_qp", IntegerType(), True),  # Será removida
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
    "qtd_vagas_preenchidas_qp",
    "qtd_vagas_obtidas_qp",
    "qtd_votos_coligacao_qp",
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
s3_output_path = "s3://elections-silver-data/output/db_quociente_eleitoral"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
