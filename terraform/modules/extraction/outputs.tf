# output "s3_ingestion_bucket_arn" {
#   value = aws_s3_bucket.ingestion_bucket.arn
# }

output "lambda_extraction_role_arn" {
  value = aws_iam_role.extraction_lambda_role.arn
}

output "lambda_layer_arn" {
  value = aws_lambda_layer_version.layer.arn
}