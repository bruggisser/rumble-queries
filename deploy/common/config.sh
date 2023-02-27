#!/usr/bin/env bash

# AWS specific values
SSH_KEY_NAME="aws-key"  # The name of the AWS key used to access S3 and EC2
INSTANCE_PROFILE="spark-role"  # Identifies an IAM to pass to created EC2 instances

S3_REGION="eu-central-1"
S3_INPUT_BUCKET="hep-adl-ethz"  # The S3 bucket name where datasets are stored without the 's3://' prefix; e.g. "my-new-bucket"
S3_INPUT_PATH="s3://${S3_INPUT_BUCKET}/hep-parquet/native"  # The full path within the bucket where the data is stored; e.g. s3://my-new-bucket/path/to/folder
