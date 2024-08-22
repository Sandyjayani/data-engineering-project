data "archive_file" "transform_lambda" {
    type        = "zip"
    source_dir  = "${path.module}/../../../src/transform"
    output_path = "${path.module}/../../../transform_functions.zip"
}

# data "archive_file" "layer" {
#   type = "zip"
#   source_dir =  "${path.module}/../../../layer" 
#   output_path =  "${path.module}/../../../layer.zip"
# }

# resource "aws_lambda_layer_version" "layer" {
#   layer_name          = "layer"
#   compatible_runtimes = [var.python_runtime]
#   s3_bucket           = local.s3_lambda_code_bucket_name
#   s3_key              = aws_s3_object.layer_code.key
# }

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