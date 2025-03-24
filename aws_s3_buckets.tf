# Buckets S3 para armazenar os dados zip
resource "aws_s3_bucket" "bronze_data" {
  bucket = "elections-bronze-data"  # Replace with your desired bucket name
  acl    = "private"
}
# Processo para subir os arquivos csv
resource "aws_s3_bucket_object" "files" {
  for_each = fileset("C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Dataset", "**")  # Replace with the path to your local folder

  bucket = aws_s3_bucket.bronze_data.id
  key    = each.value
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Dataset/${each.value}"  # Replace with the path to your local folder
}

# Buckets S3 para armazenar os dados parcialmente transformados
resource "aws_s3_bucket" "silver_data" {
  bucket = "elections-silver-data"
  acl    = "private"
}

# Buckets S3 para armazenar os dados otimizados para análises (Star Schema)
resource "aws_s3_bucket" "gold_data" {
  bucket = "elections-gold-data"
  acl    = "private"
}
