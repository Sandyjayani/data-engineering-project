locals {
  s3_lambda_code_bucket_name = regex("arn:aws:s3:::([^:]+)", var.s3_lambda_code_bucket_arn)[0]
}

resource "aws_s3_object" "lambda_code" {
  bucket = local.s3_lambda_code_bucket_name
  key = "s3_file_reader/extraction_functions.zip"
  source = "${path.module}/../../../extraction_functions.zip"
}

resource "aws_s3_object" "layer_code" {
    bucket = local.s3_lambda_code_bucket_name
    key = "layer.zip"
    source = "${path.module}/../../../layer.zip"
    }

