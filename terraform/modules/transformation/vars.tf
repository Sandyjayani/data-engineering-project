variable "team_name" {
    type = string
    default = "smith-morra"
}

variable "s3_transformation_bucket_arn" {
  description = "The ARN of the S3 ingestion bucket"
  type = string
}

variable "s3_lambda_code_bucket_arn" {
  description = "The ARN of the S3 ingestion bucket"
  type = string
}

variable "critical_error_topic_arn" {
  description = "The ARN of the sns topic for critical error for extraction"
  type = string
}

variable "transformation_lambda_name" {
  description = "The name of the lambda function for extraction"
  type = string
  default = "extraction_lambda_handler"
}

variable "python_runtime" {
  type    = string
  default = "python3.11"
}

variable "warehouse_secrets_arn" {
  description = "ARN of the Secrets Manager secret that stores DB credentials for warehouse"
  type = string
  default = "arn:aws:secretsmanager:eu-west-2:637423603039:secret:DataTarget_PostgresDB_Credentials-8FWeiI"
}

variable "account_id" {
  type = string
}

variable "region" {
  type = string
}