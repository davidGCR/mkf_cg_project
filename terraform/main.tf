terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
    }
  }
}

provider "aws" {
  region  = "us-east-1"
  shared_config_files      = ["~/.aws/config"]
  shared_credentials_files = ["~/.aws/credentials"]
  profile                  = "default"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {} # data.aws_region.current.name

locals {
  account_id = data.aws_caller_identity.current.account_id
  region = data.aws_region.current.name
  tmp_cuotas_folder = "tmp_cuotas"
  s3_stage_bucket = "auna-dla${var.env}-stage-s3"
  s3_analytics_bucket = "auna-dla${var.env}-analytics-s3"
}

/* S3 bucket del proyecto mkf */
resource "aws_s3_bucket" "mkf_project" {
  bucket = "auna-dl-${var.env}-mkfintermedio"
}

/* Objeto para subir job script 1 (.py) en S3 */
resource "aws_s3_object" "upload_glue_script_1" {
  bucket = aws_s3_bucket.mkf_project.id
  key = "Scripts/${var.script_name_1}"
  source = "${var.script_name_1}"
  etag = filemd5("${var.script_name_1}")
}
/* Objeto para subir job script 3 (.py) en S3 */
resource "aws_s3_object" "upload_glue_script" {
  bucket = aws_s3_bucket.mkf_project.id
  key = "Scripts/${var.script_name}"
  source = "${var.script_name}"
  etag = filemd5("${var.script_name}")
}

/* Objeto para subir job script 4 (.py) en S3 */
resource "aws_s3_object" "upload_glue_script_4" {
  bucket = aws_s3_bucket.mkf_project.id
  key = "Scripts/${var.script_name_4}"
  source = "${var.script_name_4}"
  etag = filemd5("${var.script_name_4}")
}

/* Folder temporal en analytics */
resource "aws_s3_object" "tmp_folder" {
  bucket = local.s3_analytics_bucket
  key = "structured-data/OLAP/pry-gestion-cobranza/${local.tmp_cuotas_folder}/"
}

/* GLue Job 1*/
resource "aws_glue_job" "glue_job_1" {
  name     = replace("${var.script_name_1}", ".py", "")
  role_arn = "arn:aws:iam::${local.account_id}:role/auna-dl-${var.env}-full-role"
  command {
    script_location = "s3://${aws_s3_bucket.mkf_project.id}/Scripts/${var.script_name_1}"
    /* python_version  = 3 */
  }
  glue_version      = var.glue_version
  max_retries       = var.max_retries
  timeout           = var.job_timeout
  number_of_workers = var.number_of_workers
  worker_type       = var.worker_type
  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = "true"
    "--enable-spark-ui" = "true"
    "--extra-py-files" = "s3://auna-dlaqa-artifacts-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf_cuotas_gestionadas/xlrd.zip,s3://auna-dlaqa-artifacts-s3/structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf_cuotas_gestionadas/et_xmlfile.zip"
    "--TempDir" = "s3://aws-glue-assets-${local.account_id}-${local.region}/temporary/"
    "--s3_landing_bucket" = "auna-dla${var.env}-raw-s3" #CAmbiar aqui verdadero landing
    "--s3_mkf_path" = "structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/xls_mkf_v2/"
    "--s3_sar_path" = "structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/xls_sar_v2/"
  }
}

/* GLue Job 3*/
resource "aws_glue_job" "glue_job" {
  name     = "${var.script_name}"
  role_arn = "arn:aws:iam::${local.account_id}:role/auna-dl-${var.env}-full-role"
  command {
    script_location = "s3://${aws_s3_bucket.mkf_project.id}/Scripts/${var.script_name}"
    /* python_version  = 3 */
  }
  glue_version      = var.glue_version
  max_retries       = var.max_retries
  timeout           = var.job_timeout
  number_of_workers = var.number_of_workers
  worker_type       = var.worker_type
  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = "true"
    "--enable-spark-ui" = "true"
    "--TempDir" = "s3://aws-glue-assets-${local.account_id}-${local.region}/temporary/"
    /* "--s3_stage_bucket" = "auna-dla${var.env}-stage-s3"
    "--s3_analytics_bucket" = "auna-dla${var.env}-analytics-s3" */
    "--s3_stage_bucket" = "${local.s3_stage_bucket}"
    "--s3_analytics_bucket" = "${local.s3_analytics_bucket}"
    "--stage_cg_path" = "structured-data/OLAP/dwh-gestionsalud/pry-gs-marketforceintermedio/mkf_cuotas_gestionadas/"
    "--analytics_cg_path" = "structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/"
    "--s3_tmp_cuotas_folder" = "${local.tmp_cuotas_folder}"
  }
}

/* GLue Job 4*/
resource "aws_glue_job" "glue_job_4" {
  name     = replace("${var.script_name_4}", ".py", "")
  role_arn = "arn:aws:iam::${local.account_id}:role/auna-dl-${var.env}-full-role"
  command {
    script_location = "s3://${aws_s3_bucket.mkf_project.id}/Scripts/${var.script_name_4}"
    /* python_version  = 3 */
  }
  glue_version      = var.glue_version
  max_retries       = var.max_retries
  timeout           = var.job_timeout
  number_of_workers = var.number_of_workers
  worker_type       = var.worker_type
  connections = [ "auna-dl-qa-psql-mkfintermedio" ]
  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = "true"
    "--enable-spark-ui" = "true"
    "--TempDir" = "s3://aws-glue-assets-${local.account_id}-${local.region}/temporary/"
    "--ANALYTICS_BUCKET_NAME" = "auna-dla${var.env}-analytics-s3"
    "--ANALYTICS_BUCKET_PATH" = "structured-data/OLAP/pry-gestion-cobranza/mkf_cuotas_gestionadas/"
    "--JDBC_CONNECTION_URL" = "jdbc:postgresql://db-cluster-auna-dev-instance-1.ccpjcspl9dtx.us-east-1.rds.amazonaws.com:5432/db_consolidado_bi_no_prd_dev"
    "--PASSWORD" = "v9JgPD3#Q6LSNLY4"
    "--RDS_SCHEMA_NAME" = "pry-mva-Recomendador-Retenciones"
    "--RDS_TABLE_NAME" = "mkf_cuotas_gestionadas"
    "--USERNAME" = "amorales"
    "--enable-continuous-log-filter" = "true"
  }
}