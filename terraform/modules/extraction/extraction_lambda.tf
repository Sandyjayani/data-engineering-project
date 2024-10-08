resource "aws_lambda_layer_version" "layer" {
  layer_name          = "layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = local.s3_lambda_code_bucket_name
  s3_key              = aws_s3_object.layer_code.key
}

resource "aws_lambda_function" "s3_file_reader" {
  function_name = var.extraction_lambda_name
  s3_bucket     = local.s3_lambda_code_bucket_name
  s3_key        = aws_s3_object.lambda_code.key
  role          = aws_iam_role.extraction_lambda_role.arn
  handler       = "extraction.lambda_handler"
  layers        =  [aws_lambda_layer_version.layer.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:16"]
  runtime       = var.python_runtime
  timeout       = 180
}
