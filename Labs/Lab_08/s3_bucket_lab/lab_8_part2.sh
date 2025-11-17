#!/bin/bash

# Check for required arguments
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <local_file> <bucket_name> <expiration_seconds>"
    exit 1
fi

LOCAL_FILE=$1
BUCKET=$2
EXPIRATION=$3

# Upload file to S3
aws s3 cp "$LOCAL_FILE" "s3://$BUCKET/"

# Check upload result
if [ $? -ne 0 ]; then
    echo "Error: Failed to upload file to S3."
    exit 1
fi

echo "File uploaded successfully."

# Generate a presigned URL
PRESIGNED_URL=$(aws s3 presign --expires-in "$EXPIRATION" "s3://$BUCKET/$LOCAL_FILE")

echo "Presigned URL (expires in $EXPIRATION seconds):"
echo "$PRESIGNED_URL"

