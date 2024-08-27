variable "team_name" {
    type = string
    default = "smith-morra"
}

variable "s3_transformation_bucket_arn" {
  description = "The ARN of the S3 transformation bucket"
  type = string
}

variable "s3_ingestion_bucket_arn" {
  description = "The ARN of the S3 ingestion bucket"
  type = string
}

variable "s3_lambda_code_bucket_arn" {
  description = "The ARN of the S3 lambda code bucket"
  type = string
}

variable "critical_error_topic_arn" {
  description = "The ARN of the sns topic for critical error for extraction"
  type = string
}

variable "transformation_lambda_name" {
  description = "The name of the lambda function for transformation"
  type = string
  default = "transformation_lambda_handler"
}

variable "python_runtime" {
  type    = string
  default = "python3.11"
}

variable "account_id" {
  type = string
}

variable "region" {
  type = string
}

variable "lambda_layer_arn" {
  type = string
}