terraform {
    required_providers {
        aws = {
        source  = "hashicorp/aws"
        version = "~> 5.0"
        }
    }
    
    #TODO:
    backend "s3" {
        bucket = "smith-morra-terraform-state-bucket"
        key = "extraction_terraform.tfstate"
        region = "eu-west-2"
    }
}

provider "aws" {
    region = "eu-west-2"
        default_tags {
        tags = {
            ProjectName = "Final Project"
            Team = var.team_name
            DeployedFrom = "Terraform"
            Repository = "data-engineering-project"
            CostCentre = "DE"
            Environment = "dev"
            RetentionDate = "2024-08-31"
        }
    }
}

module "permanent" {
  source = "./modules/permanent"
  team_name = var.team_name
  subscription_email = var.subscription_email
  s3_ingestion_bucket = var.s3_ingestion_bucket
}


module "extraction" {
  source = "./modules/extraction"
  # secrets_arn = var.secrets_arn
  # lambda_schedule_expression = var.lambda_schedule_expression
  s3_ingestion_bucket_arn = module.permanent.ingestion_bucket_arn
  s3_lambda_code_bucket_arn = module.permanent.lambda_code_bucket_arn
  critical_error_topic_arn = module.permanent.critical_error_topic_arn
  team_name = var.team_name
  account_id = data.aws_caller_identity.current.account_id
  region = data.aws_region.current.name
}


module "transformation" {
  source = "./modules/transformation"
  s3_ingestion_bucket_arn = module.permanent.ingestion_bucket_arn
  s3_transformation_bucket_arn = module.permanent.transformation_bucket_arn
  s3_lambda_code_bucket_arn = module.permanent.lambda_code_bucket_arn
  lambda_layer_arn = module.extraction.lambda_layer_arn
  critical_error_topic_arn = module.permanent.critical_error_topic_arn
  team_name = var.team_name
  account_id = data.aws_caller_identity.current.account_id
  region = data.aws_region.current.name

  depends_on = [ module.extraction ]
  #ensure the extraction module is fully created (or updated) before 
  #it begins creating or updating the transformation module.

}

# module "load" {
#   source = ".terraforms/modules/extraction"
#   depends_on = [ module.transformation ]
# }




