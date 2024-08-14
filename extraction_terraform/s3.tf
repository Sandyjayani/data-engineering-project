resource "aws_s3_bucket" "lambda_code_bucket" {
    bucket_prefix = "${common_terraform.var.team_name}-code-bucket" # or replace with a name rather than prefix instead
}

resource "aws_s3_bucket" "ingestion_bucket" {
    bucket_prefix = "${common_terraform.var.team_name}-ingestion-bucket" # or replace with a name rather than prefix instead
}

#TODO:
resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key = "<key>"
  source = "<filepath>.zip"
}


# do we want/need notifs in here?