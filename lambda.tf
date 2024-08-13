#TODO:
resource "aws_lambda_function" "s3_file_reader" {
    function_name = "${var.lambda_name}"
    s3_bucket = aws_s3_bucket.lambda_code_bucket.bucket
    s3_key = "<new_file_name>.zip"
    role = aws_iam_role.lambda_role.arn
    # handler = "reader.lambda_handler" ??? do we need this?
    runtime = "python3.9"
}

#TODO:
data "archive_file" "lambda" {
    type        = "zip"
    source_file = "<file_path>"
    output_path = "<new_file_path>.zip"
}

#TODO:
# is the step function a resource we need to give permissions to? I'd assume so -> how/what
resource "aws_lambda_permission" "allow_s3" {
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.s3_file_reader.function_name
    principal = "s3.amazonaws.com"
    source_arn = aws_s3_bucket.data_bucket.arn
    source_account = data.aws_caller_identity.current.account_id
}

