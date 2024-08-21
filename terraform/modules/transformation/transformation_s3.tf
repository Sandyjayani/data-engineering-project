locals {
  s3_bucket_name = regex("arn:aws:s3:::([^:]+)", var.s3_lambda_code_bucket_arn)[0]
}
