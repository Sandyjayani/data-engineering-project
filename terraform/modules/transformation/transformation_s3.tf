locals {
  s3_lambda_code_bucket_name = regex("arn:aws:s3:::([^:]+)", var.s3_lambda_code_bucket_arn)[0]
}

resource "aws_s3_object" "lambda_code" {
  bucket = local.s3_lambda_code_bucket_name
  key = "s3_files_transformer/transformation_functions.zip"
  source = "${path.module}/../../../transform_functions.zip"
}

# resource "aws_s3_object" "layer_code" {
#     bucket = local.s3_lambda_code_bucket_name
#     key = "layer.zip"
#     source = "${path.module}/../../../layer.zip"
#     }

# resource "aws_lambda_layer_version" "layer" {
#   layer_name          = "layer"
#   compatible_runtimes = [var.python_runtime]
#   s3_bucket           = local.s3_lambda_code_bucket_name
#   s3_key              = aws_s3_object.layer_code.key
# }
