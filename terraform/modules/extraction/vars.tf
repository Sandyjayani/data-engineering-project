variable "aws_region" {
  description = "The AWS region to deploy resources"
  type = string
  default     = "eu-west-2"
}

variable "team_name" {
    type = string
    default = "smith-morra"
}

# variable "s3_ingestion_bucket" {
#   description = "S3 bucket name for storing ingested data"
#   type = string
#   default     = "smith-morra-ingestion-bucket"
# }

variable "s3_ingestion_bucket_arn" {
  description = "The ARN of the S3 ingestion bucket"
  type = string
}

variable "critical_error_topic_extraction_arn" {
  description = "The ARN of the sns topic for critical error for extraction"
  type = string
}

variable "secrets_arn" {
  description = "ARN of the Secrets Manager secret that stores DB credentials"
  type = string
  default = "arn:aws:secretsmanager:eu-west-2:637423603039:secret:DataSource_PostgresDB_Credentials-is1p1K"
}

variable "subscription_email" {
  type = string
  description = "email to send noticification"
  default = "cwc3214@gmail.com"
}

variable "lambda_schedule_expression" {
  description = "Expression for scheduling the Lambda function"
  default     = "rate(15 minutes)"
}

variable "extraction_lambda_name" {
  description = "Cron expression for scheduling the Lambda function"
  type = string
  default = "extraction_lambda_handler"
}

variable "python_runtime" {
  type    = string
  default = "python3.11"
}
