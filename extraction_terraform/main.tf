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
            Team = common_terraform.var.team_name
            DeployedFrom = "Terraform"
            Repository = "data-engineering-project"
            CostCentre = "DE"
            Environment = "dev"
            RetentionDate = "2024-05-31"
        }
    }
}
