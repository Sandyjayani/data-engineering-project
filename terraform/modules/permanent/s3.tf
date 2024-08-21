resource "aws_s3_bucket" "ingestion_bucket" {
    bucket = var.s3_ingestion_bucket

    tags = {
      Name = "Permanent Ingestion Bucket"
    }

    # force_destroy =  false

    # lifecycle {
    #   prevent_destroy = true
    # }
}

resource "aws_s3_bucket" "transformation_bucket" {
    bucket = var.s3_transformation_bucket

    tags = {
      Name = "Permanent Ingestion Bucket"
    }

    # force_destroy =  false

    # lifecycle {
    #   prevent_destroy = true
    # }
}

output "ingestion_bucket_arn" {
  value = aws_s3_bucket.ingestion_bucket.arn
}

output "transformation_bucket_arn" {
  value = aws_s3_bucket.transformation_bucket.arn
}




