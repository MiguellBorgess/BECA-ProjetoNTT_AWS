resource "aws_s3_bucket_object" "script_upload_candidato" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_candidato.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_candidato.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_comparecimento" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_comparecimento_abstencao.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_comparecimento_abstencao.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_deficientes" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_deficientes.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_deficientes.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_detalhe_votacao" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_detalhe_votacao.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_detalhe_votacao.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_eleitorado_capilaridade" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_eleitorado_eleicao_capilaridade.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_eleitorado_eleicao_capilaridade.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_eleitores_faltosos" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_eleitores_faltosos.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_eleitores_faltosos.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_filiado_eleicao" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_filiado_eleicao.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_filiado_eleicao.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_pesquisas_eleitorais" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_pesquisas_eleitorais.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_pesquisas_eleitorais.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_quocientes_eleitoral" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_quocientes_eleitoral.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_quocientes_eleitoral.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_vagas" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_vagas.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_vagas.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_votacao_candidatos" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_votacao_candidatos.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_votacao_candidatos.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_votacao_partida" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_votacao_partida.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_votacao_partida.py"  # Local path to the script
  acl    = "private"
}

resource "aws_s3_bucket_object" "script_upload_fundo_partidario" {
  bucket = aws_s3_bucket.bronze_data.bucket
  key    = "scripts/script_fundo_partidario.py"  # Path within the bucket
  source = "C:/Users/mperebor/OneDrive - NTT DATA EMEAL/Documents/PI/Beca25/scripts/script_fundo_partidario.py"  # Local path to the script
  acl    = "private"
}
