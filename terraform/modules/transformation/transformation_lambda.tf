resource "aws_lambda_function" "s3_files_transformer" {
  function_name = var.transformation_lambda_name
  s3_bucket     = local.s3_lambda_code_bucket_name
  s3_key        = aws_s3_object.lambda_code.key
  role          = aws_iam_role.transformation_lambda_role.arn
  handler       = "transformation.lambda_handler"
  layers        = [var.lambda_layer_arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:16"]
  runtime       = var.python_runtime
  timeout       = 180
}