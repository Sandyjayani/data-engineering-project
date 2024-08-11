# Data Engineering Project

## Project Overview

This project implements an end-to-end data engineering solution for extracting data from the Totesys operational database, transforming it, and loading it into a data warehouse. The solution is built using AWS services and follows best practices for data engineering, including maintaining historical data, implementing ETL processes, and using infrastructure as code.

## Architecture

The project consists of three main components:

1. Data Ingestion
2. Data Processing
3. Data Loading

Each component is implemented as an AWS Lambda function, with data stored in S3 buckets between stages.

## Components

### Data Ingestion

- Lambda function: `ingestion_lambda.py`
- S3 bucket: `totesys-ingestion-bucket`

This component extracts data from the Totesys PostgreSQL database and stores it in Parquet format in the ingestion S3 bucket.

### Data Processing

- Lambda function: `processing_lambda.py`
- S3 bucket: `totesys-processed-bucket`

This component transforms the ingested data, implementing historical tracking for fact tables and preparing the data for the warehouse schema.

### Data Loading

- Lambda function: `loading_lambda.py`

This component loads the processed data into the data warehouse, maintaining historical records for fact tables.

## Technologies Used

- AWS (S3, Lambda, EventBridge, CloudWatch)
- Python
- PostgreSQL
- Terraform
- Github(CI/CD)

## Project Structure

## Setup and Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up AWS credentials
4. Run Terraform: `terraform init && terraform apply`

## Testing

Run the tests using pytest:

pytest tests/

## Contributing

Please follow these steps to contribute to the project:

1. Create a new branch for your feature or bug fix.
2. Write tests for your changes.
3. Implement your changes, ensuring they pass all tests.
4. Update documentation as necessary.
5. Submit a pull request for review.
