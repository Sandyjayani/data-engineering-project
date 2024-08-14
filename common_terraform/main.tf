terraform {
    required_providers {
        aws = {
        source  = "hashicorp/aws"
        version = "~> 5.0"
        }
    }
    
    #TODO:
    backend "s3" {
        bucket = "${var.team_name}-terraform-state-bucket"
        key = "<key>" # path to terraform state file in repo!
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
            RetentionDate = "2024-05-31"
        }
    }
}
