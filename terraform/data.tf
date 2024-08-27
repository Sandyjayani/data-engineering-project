data "aws_caller_identity" "current" {}

data "aws_region" "current" {}


data "archive_file" "lambda" {
    type        = "zip"
    source_dir  = "${path.module}/../src/extraction"
    output_path = "${path.module}/../extraction_functions.zip"
}

data "archive_file" "layer" {
  type = "zip"
  source_dir =  "${path.module}/../layer" 
  output_path =  "${path.module}/../layer.zip"
}

data "archive_file" "transform_lambda" {
    type        = "zip"
    source_dir  = "${path.module}/../src/transform"
    output_path = "${path.module}/../transform_functions.zip"
}

data "archive_file" "load_lambda" {
    type        = "zip"
    source_dir  = "${path.module}/../src/load"
    output_path = "${path.module}/../load_functions.zip"
}