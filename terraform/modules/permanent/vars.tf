
variable "team_name" {
    type = string
    default = "smith-morra"
}

variable "s3_ingestion_bucket" {
  description = "S3 bucket name for storing ingested data"
  type = string
  default     = "smith-morra-ingestion-bucket"
}

variable "s3_transformation_bucket" {
  description = "S3 bucket name for storing transformed data"
  type = string
  default     = "smith-morra-transformation-bucket"
}

variable "subscription_email" {
  type = string
  description = "email to send noticification"
  default = "cwc3214@gmail.com"
}
