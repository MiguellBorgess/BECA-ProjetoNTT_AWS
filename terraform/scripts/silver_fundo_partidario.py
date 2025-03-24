import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id, regexp_replace
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

import boto3

# Configuração do Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)

# Lendo o arquivo CSV bruto
s3_input_path = "s3://elections-bronze-data/fundo_partidario.csv"

# Definindo o esquema correspondente
schema = StructType([
    StructField("uf", StringType(), True),
    StructField("ano_eleicao", IntegerType(), True),
    StructField("cor_raca", StringType(), True),
    StructField("esfera_partidaria", StringType(), True),
    StructField("genero", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("partido", StringType(), True),
    StructField("quant_candidatos", IntegerType(), True),
    StructField("valor_fp_campanha", StringType(), True),
    StructField("recursos_declarados", StringType(), True),
    StructField("data_carga", TimestampType(), True)
])

# Lendo o arquivo CSV aplicando o esquema diretamente e definindo a codificação como ISO-8859-1
data_frame = spark.read.csv(
    s3_input_path,
    header=True,
    schema=schema,
    sep=";",
    encoding="ISO-8859-1"  # Define a codificação do arquivo
)

# Substituindo ',' por '.' e convertendo para DoubleType
data_frame = data_frame.withColumn(
    "valor_fp_campanha", 
    regexp_replace(col("valor_fp_campanha"), ",", ".").cast(DoubleType())
)

data_frame = data_frame.withColumn(
    "recursos_declarados", 
    regexp_replace(col("recursos_declarados"), ",", ".").cast(DoubleType())
)

# Deletando a colunas especificas
data_frame = data_frame.drop("municipio", "data_carga")

# Removendo dados duplicados
data_frame = data_frame.dropDuplicates()

# Adicionando a coluna id_registro
data_frame = data_frame.withColumn("id_registro", monotonically_increasing_id() + 1)

# Reorganizando as colunas para que id_registro seja a primeira
colunas = ["id_registro"] + [coluna for coluna in data_frame.columns if coluna != "id_registro"]
data_frame = data_frame.select(*colunas)

# Substituindo valores nulos por "NA"
#data_frame = data_frame.na.fill(0)

# Convertendo para DynamicFrame
dyf = DynamicFrame.fromDF(data_frame, glueContext, "dyf")

# Salvando os dados no bucket S3
s3_output_path = "s3://elections-silver-data/output/db_fundo_partidario"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()