## Documentation

**Terraform** 

**Main Files**

**main.tf** 

 The main Terraform configuration file sets up infrastructure on AWS  using Terraform. The aws provider is configured with region-specific settings and default tags, and the state is stored in an S3 bucket. Two modules are specified, Permanent and Extraction.  The permanent module contains the terraform infrastructure that constructs the ingestion bucket that is not destroyed inorder to maintain the state of the bucket.

 **Permanent Module**

 **s3.tf** - Sets up a permanent ingestion bucket with prevent_destroy set to True

 **subscription.tf** - Sets up sns email subscription for critical error notifications with prevent_destroy set to True.

 **extraction Module**