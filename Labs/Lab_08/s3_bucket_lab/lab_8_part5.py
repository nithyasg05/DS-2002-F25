import boto3
import requests

# -----------------------------------------
# Step 1 — Download a file from the internet
# -----------------------------------------
file_url = "https://media.giphy.com/media/ICOgUNjpvO0PC/giphy.gif"
local_file = "lab8_download.gif"

print("Downloading file from the internet...")
response = requests.get(file_url)

# Save it locally
with open(local_file, "wb") as f:
    f.write(response.content)

print(f"Saved file as {local_file}")

# -----------------------------------------
# Step 2 — Upload file to S3
# -----------------------------------------
bucket_name = "ds2002-f25-tsd5gt"   # ← YOUR BUCKET NAME
object_name = "lab8_uploaded.gif"    # Key in S3 (you can rename if you want)

s3 = boto3.client("s3", region_name="us-east-1")

print("Uploading to S3...")
s3.upload_file(
    Filename=local_file,
    Bucket=bucket_name,
    Key=object_name
)

print(f"Uploaded to s3://{bucket_name}/{object_name}")

# -----------------------------------------
# Step 3 — Generate a presigned URL
# -----------------------------------------
expires_in = 300   # 5 minutes (you can change)

presigned_url = s3.generate_presigned_url(
    ClientMethod="get_object",
    Params={
        "Bucket": bucket_name,
        "Key": object_name
    },
    ExpiresIn=expires_in
)

# -----------------------------------------
# Step 4 — Output the presigned URL
# -----------------------------------------
print("\nPresigned URL:")
print(presigned_url)
print("\nThis URL will expire in", expires_in, "seconds.")

