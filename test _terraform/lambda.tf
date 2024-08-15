data "archive_file" "lambda" {
    type        = "zip"
    source_dir = "${path.module}/../src"
    output_path = "${path.module}/../functions.zip"
}

data "archive_file" "layer" {
  type = "zip"
  source_dir =  "${path.module}/../layer" 
  output_path =  "${path.module}/../layer.zip"
}

resource "aws_lambda_layer_version" "layer" {
  layer_name          = "layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key              = aws_s3_object.layer_code.key
}

resource "aws_lambda_function" "s3_file_reader" {
  function_name = "s3-file-reader-function"  
  s3_bucket     = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key        = aws_s3_object.lambda_code.key
  role          = aws_iam_role.lambda_role.arn   
  handler       = "upload_to_s3_util_func.lambda_handler"
  layers =  [aws_lambda_layer_version.layer.arn]
  runtime       = var.python_runtime

#   environment {
#     variables = {
#       S3_BUCKET_NAME = aws_s3_bucket.ingestion_bucket.bucket
#     }
#   }
}



# #TODO:
# resource "aws_lambda_function" "s3_file_reader" {
#     function_name = "${common_terraform.vars.lambda_name}"
#     s3_bucket = aws_s3_bucket.lambda_code_bucket.bucket
#     s3_key = "<new_file_name>.zip"
#     role = aws_iam_role.lambda_role.arn
#     # handler = "reader.lambda_handler" ??? do we need this?
#     runtime = "python3.9"
# }

# #TODO:
# data "archive_file" "lambda" {
#     type        = "zip"
#     source_file = "<file_path>"
#     output_path = "<new_file_path>.zip"
# }

# #TODO:
# # is the step function a resource we need to give permissions to? I'd assume so -> how/what
# resource "aws_lambda_permission" "allow_s3" {
#     action = "lambda:InvokeFunction"
#     function_name = aws_lambda_function.s3_file_reader.function_name
#     principal = "s3.amazonaws.com"
#     source_arn = aws_s3_bucket.data_bucket.arn
#     source_account = data.aws_caller_identity.current.account_id
# }

