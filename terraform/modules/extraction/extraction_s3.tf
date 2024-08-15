resource "aws_s3_bucket" "lambda_code_bucket" {
    bucket_prefix = "${var.team_name}-code-bucket" 
}

resource "aws_s3_bucket" "ingestion_bucket" {
    bucket = "smith-morra-ingestion-bucket"
}


resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.lambda_code_bucket.bucket
  key = "s3_file_reader/functions.zip"
  source = "${path.module}/../../../functions.zip"
}


# resource "aws_s3_object" "layer_code" {
#   bucket = aws_s3_bucket.lambda_code_bucket.bucket
#   key = "layer.zip"
#   source = "${path.module}/../../../layer.zip"
#   }

