locals {
  s3_lambda_code_bucket_name = regex("arn:aws:s3:::([^:]+)", var.s3_lambda_code_bucket_arn)[0]
}

resource "aws_s3_object" "lambda_code" {
  bucket = local.s3_lambda_code_bucket_name
  key = "s3_files_load/load_functions.zip"
  source = "${path.module}/../../../load_functions.zip"
}

resource "aws_lambda_function" "s3_files_load" {
  function_name = var.load_lambda_name
  s3_bucket     = local.s3_lambda_code_bucket_name
  s3_key        = aws_s3_object.lambda_code.key
  role          = aws_iam_role.load_lambda_role.arn
  handler       = "load_lambda_handler.lambda_handler"
  layers        = [var.lambda_layer_arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:16"]
  runtime       = var.python_runtime
  timeout       = 600
}