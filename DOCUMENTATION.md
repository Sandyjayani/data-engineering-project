# Documentation

![img](./ETLPipeline.png)


# Terraform 
Terraform is used to set up the configuration for deploying AWS infrastructure, structured using modules.  The files are organized to manage different parts of a data pipeline, specifically focusing on extraction, transformation, and the setup of permanent resources.

## Main Files

### main.tf
The main.tf file sets up the modules and an s3 bucket that is used to contain a terraform state file that keeps track of the infrastructure managed by terraform
 

1. Terraform Configuration Block

	•	required_providers: Specifies the provider (aws) used in the configuration, pulling from the official HashiCorp registry with version constraints.

	•	backend “s3”: Configures Terraform to store the state file in an S3 bucket (smith-morra-terraform-state-bucket) in the eu-west-2 region. The state file (extraction_terraform.tfstate) keeps track of the infrastructure managed by Terraform. 

2. AWS Provider Configuration

3. Modules

    Permanent -  Manages resources that are expected to be long-lived, like S3 buckets for data ingestion and other shared infrastructure.

    Extraction - Manages the extraction phase of the ETL (Extract, Transform, Load) pipeline.

    Transformation - Manages the transformation phase of the ETL pipeline.
    load - manages the load phase of the ETL pipeline,


## Step Functions
Step Functions is a service that lets you coordinate multiple AWS services into serverless workflows.

### stepFunction.tf

Configuration for defining an AWS Step Functions state machine using the aws_sfn_state_machine resource.  Outlines how a state machine is set up to manage an ETL (Extract, Transform, Load) process. Also retry strategies and additional features such as a catch block to handle errors and publish messages to SNS

### stepFunction_scheduler.tf

set up a scheduler that triggers an AWS Step Function periodically using Amazon CloudWatch Events 

### stepFuntion_iam.tf

Sets up IAM role permisions for the stepFunction

### data.tf

several Terraform data sources and archive_file resources used in setting up AWS Lambda functions.

### outputs.tf

defines three output values that  can be used by other parts of the Terraform configuration or displayed to the user after an apply.  These outputs are essentially exporting the ARNs of the three S3 buckets (ingestion, transformation, and Lambda code) from the permanent module. This allows other parts of the Terraform configuration, or external tools, to reference these bucket ARNs easily. 


## Permanent Module

 ### s3.tf

  defines three AWS S3 buckets and outputs their ARNs (Amazon Resource Names)  ingestion_bucket, transformation_bucket and lambda_code_bucket

 ### subscription.tf

  Sets up sns email subscription for critical error notifications with prevent_destroy set to True.

 ## extraction Module

### extraction_lambda.tf

 defines a Lambda Layer and a Lambda Function. The aws_lambda_function resource creates a Lambda function called s3_file_reader, which is designed to read files from S3. It uses the code stored in an S3 bucket, is configured to use specific layers, and has a set timeout.

### extraction_s3.tf

configures the uploading of Lambda function code and Lambda layer code to an S3 bucket.

### extraction_iam.tf

 Sets up the necessary iam permissions for the extraction lambda

### alert_monitor.tf

sets up monitoring and alerting for critical errors logged by an AWS Lambda function. CloudWatch Log Metric Filter for Critical Errors and CloudWatch Metric Alarm for Critical Errors.

## transformation Module

### transformation_lambda.tf

Defines an AWS Lambda function named s3_files_transformer, which is intended to handle the transformation of files stored in S3.  Sets up an AWS Lambda function to handle file transformation tasks, utilizing S3 as the source for the code and leveraging Lambda layers for additional dependencies. The function is configured with a custom IAM role, a defined entry point, and a timeout 

### transformation_s3.tf

configures the uploading of Lambda function code and Lambda layer code to an S3 bucket.

### transformation_iam.tf

Sets up the necessary iam permissions for the extraction lambda

### alarm_monitor.tf 

sets up monitoring and alerting for critical errors logged by an AWS Lambda function. CloudWatch Log Metric Filter for Critical Errors and CloudWatch Metric Alarm for Critical Errors.




# Step Function
Step Functions is a service that lets you coordinate multiple AWS services into serverless workflows.  In this case, the step function coordinates the invokation of the extraction, transformation and load lambda functions, controling the flow and publishing any step function errors to SNS.

![img](./step_function.png)



# Extraction

**extraction.py - lambda_handler(event, context)** 

- sets up logger and opens a connection to the totesys database

- gets the timestamp for the last table from the csv 

- querries each table in the database and collects any updates since last checked

- uploads new data to s3 bucket

- returns response object with 201 status

imports:

- setup_logger
    - Set up and return a logger with JSON formatting
    - parameter: name (differnt logging instances but still in the same log group)
    - output: a logger, could be used in the other functions 

- create_connection from get_db_connection 
    - Uses boto3 and pg8000 to establish a database connection with AWS

- get_timestamp
    - takes name of table and searches for latest timestamp in csv file for that table. If no timestamp CSV exist,
     it will return timestamp of "0001-01-01_01.01.01".

    - parameters:
        - table_name: str

    - return value:
        - timestamp: str (e.g., '2024-08-14_14-09.01')

- get_table 
    - Queries a table using a pg8000 connection and returns all rows where last_updated is after the passed timestamp.
    Captures the column names from the connection and uses pandas to create a data frame
    from the result and the columns. If result is empty, returns None. Otherwise, returns dataframe.

- upload_tables_to_s3
    -   get the current timestamp
    - call save_timestamps to save the current timestamp in a csv file (for the get_timestamp func
    which generate the timestamp as an input for get_table to input)
    - create a var for the file key in
    "[Table Name]/Year/Month/Day/hh-mm/[tablename]-[timestamp].csv"
    - convert the given dataframe to csv (should be written to an in-memory
    buffer, not via local
    file and delete)
    - upload the csv from the buffer to the s3
    - return a confirmation message with the upload details


# Transform

**transformation_lambda.py - lambda_handler(event, context)**

The function will be triggered within the step function after the successful execution of the extraction lambda.

It:

- instantiates a logger using the setup_logger util func
- logs:
    - the completion of steps within its execution
    - any execptions that are raised
- calls the load_ingestion_data to create a dictionary where:
    - key: table_name
    - value: dataframe containing any newly ingested data for that table
- calls the transformation functions for each OLAP table,
    - passing in the dictionary or a single dataframe
    - and capturing the output transformed dataframes.
- calls the upload_tables_to_s3 function, passing in
    - the transformed dataframes to be saved as parquet tables.
- catches any exceptions raised during execution, logs them and raises
    - the exception again to be handled by the step function.


