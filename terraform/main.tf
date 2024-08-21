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

output "ingestion_bucket_arn" {
  value = module.permanent.ingestion_bucket_arn
}


module "extraction" {
  source = "./modules/extraction"
  # secrets_arn = var.secrets_arn
  # lambda_schedule_expression = var.lambda_schedule_expression
  s3_ingestion_bucket_arn = module.permanent.ingestion_bucket_arn
  critical_error_topic_arn = module.permanent.critical_error_topic_arn
  team_name = var.team_name
}


# placeholder for transformation and loadings
module "transformation" {
  source = "./modules/transformation"
  depends_on = [ module.extraction ]
}

# module "load" {
#   source = ".terraforms/modules/extraction"
#   depends_on = [ module.transformation ]
# }




