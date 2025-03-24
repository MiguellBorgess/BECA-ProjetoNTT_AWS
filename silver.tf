# Map com nomes e scripts para os jobs
locals {
  jobs = {
    candidato                    = "s3://elections-bronze-data/scripts/script_candidato.py"
    comparecimento_abstencao     = "s3://elections-bronze-data/scripts/script_comparecimento_abstencao.py"
    deficientes                  = "s3://elections-bronze-data/scripts/script_deficientes.py"
    detalhe_votacao              = "s3://elections-bronze-data/scripts/script_detalhe_votacao.py"
    eleitorado_eleicao_capilaridade = "s3://elections-bronze-data/scripts/script_eleitorado_eleicao_capilaridade.py"
    eleitores_faltosos           = "s3://elections-bronze-data/scripts/script_eleitores_faltosos.py"
    filiado_eleicao              = "s3://elections-bronze-data/scripts/script_filiado_eleicao.py"
    pesquisas_eleitorais         = "s3://elections-bronze-data/scripts/script_pesquisas_eleitorais.py"
    quocientes_eleitoral         = "s3://elections-bronze-data/scripts/script_quocientes_eleitoral.py"
    vagas                        = "s3://elections-bronze-data/scripts/script_vagas.py"
    votacao_candidatos           = "s3://elections-bronze-data/scripts/script_votacao_candidatos.py"
    votacao_partida              = "s3://elections-bronze-data/scripts/script_votacao_partida.py"
    fundo_partidario             = "s3://elections-bronze-data/scripts/script_fundo_partidario.py"
  }
}


# Criação dinâmica de jobs
resource "aws_glue_job" "silver_transform" {
  for_each = local.jobs

  name     = "silver_transform_${each.key}_job"
  role_arn = "arn:aws:iam::008376430615:role/GlueAccessRole"

  command {
    script_location = each.value
    name            = "glueetl"
    python_version  = "3"  # Specify Python 3
  }

  default_arguments = {
    "--TempDir"               = "s3://elections-silver-data/temp/"
    "--job-language"          = "python"
    "--enable-glue-datacatalog" = "true"
    "--job-bookmark-option"   = "job-bookmark-enable"
  }

  tags = {
    environment = "development"
    layer       = "silver"
    table       = each.key
  }
}
