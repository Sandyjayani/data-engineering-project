## Documentation

## Terraform 

__**Main Files**__

**main.tf** 

A configuration for deploying AWS infrastructure, structured using modules.  Organized to manage different parts of a data pipeline, specifically focusing on extraction, transformation, and the setup of permanent resources. 

1. Terraform Configuration Block

	•	required_providers: Specifies the provider (aws) used in the configuration, pulling from the official HashiCorp registry with version constraints.
	•	backend “s3”: Configures Terraform to store the state file in an S3 bucket (smith-morra-terraform-state-bucket) in the eu-west-2 region. The state file (extraction_terraform.tfstate) keeps track of the infrastructure managed by Terraform. 
    
2. AWS Provider Configuration

3. Modules

    Permanent -  Manages resources that are expected to be long-lived, like S3 buckets for data ingestion and other shared infrastructure.
    Extraction - Manages the extraction phase of the ETL (Extract, Transform, Load) pipeline.
    Transformation - Manages the transformation phase of the ETL pipeline.
    load - manages the load phase of the ETL pipeline,

 **stepFunction.tf**

Step Functions is a service that lets you coordinate multiple AWS services into serverless workflows.

Configuration for defining an AWS Step Functions state machine using the aws_sfn_state_machine resource.  Outlines how a state machine is set up to manage an ETL (Extract, Transform, Load) process. Also retry strategies and additional features such as a catch block to handle errors and publish messages to SNS

**stepFunction_scheduler.tf**

set up a scheduler that triggers an AWS Step Function periodically using Amazon CloudWatch Events 

**stepFuntion_iam.tf**

Sets up IAM role permisions for the stepFunction

**data.tf**

several Terraform data sources and archive_file resources used in setting up AWS Lambda functions.

**outputs.tf**

defines three output values that  can be used by other parts of the Terraform configuration or displayed to the user after an apply.  These outputs are essentially exporting the ARNs of the three S3 buckets (ingestion, transformation, and Lambda code) from the permanent module. This allows other parts of the Terraform configuration, or external tools, to reference these bucket ARNs easily. 


__**Permanent Module**__

 **s3.tf** 

  defines three AWS S3 buckets and outputs their ARNs (Amazon Resource Names)  ingestion_bucket, transformation_bucket and lambda_code_bucket

 **subscription.tf** 

  Sets up sns email subscription for critical error notifications with prevent_destroy set to True.

 __**extraction Module**__

**extraction_lambda.tf** 

 defines a Lambda Layer and a Lambda Function. The aws_lambda_function resource creates a Lambda function called s3_file_reader, which is designed to read files from S3. It uses the code stored in an S3 bucket, is configured to use specific layers, and has a set timeout.

**extraction_s3.tf**  

configures the uploading of Lambda function code and Lambda layer code to an S3 bucket.

**extraction_iam** 

 Sets up the necessary iam permissions for the extraction lambda

**alert_monitor** 

sets up monitoring and alerting for critical errors logged by an AWS Lambda function. CloudWatch Log Metric Filter for Critical Errors and CloudWatch Metric Alarm for Critical Errors.

__**transformation Module**__

**transformation_lambda** 

Defines an AWS Lambda function named s3_files_transformer, which is intended to handle the transformation of files stored in S3.  Sets up an AWS Lambda function to handle file transformation tasks, utilizing S3 as the source for the code and leveraging Lambda layers for additional dependencies. The function is configured with a custom IAM role, a defined entry point, and a timeout 

**transformation_s3**  

configures the uploading of Lambda function code and Lambda layer code to an S3 bucket.

**transformation_iam** 

Sets up the necessary iam permissions for the extraction lambda

**alarm_monitor** 

sets up monitoring and alerting for critical errors logged by an AWS Lambda function. CloudWatch Log Metric Filter for Critical Errors and CloudWatch Metric Alarm for Critical Errors.