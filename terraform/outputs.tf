output "ingestion_bucket_arn" {
  value = module.permanent.ingestion_bucket_arn
}

output "transformation_bucket_arn" {
  value = module.permanent.transformation_bucket_arn
}

output "lambda_code_bucket_arn" {
  value = module.permanent.lambda_code_bucket_arn
}