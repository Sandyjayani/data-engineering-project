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


module "extraction" {
  #source = "./terraform/extraction"
  source = "./modules/extraction"
  s3_ingestion_bucket = var.s3_ingestion_bucket
  secrets_arn = var.secrets_arn
  sns_topic_email = var.sns_topic_email
  lambda_schedule_expression = var.lambda_schedule_expression
  team_name = var.team_name
}


# # placeholder for transformation and loadings
# module "transformation" {
#   source = ".terraforms/modules/transformation"
#   depends_on = [ module.extraction ]
# }

# module "load" {
#   source = ".terraforms/modules/extraction"
#   depends_on = [ module.transformation ]
# }


output "s3_ingestion_bucket_arn" {
  value = module.extraction.s3_ingestion_bucket_arn
}


