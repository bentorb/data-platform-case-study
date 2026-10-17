"""Initialises the landing zone: creates the bucket if it does not exist.

Runs once per `docker compose up`. Safe to re-run.
"""

from botocore.exceptions import ClientError

from s3_client import BUCKET, get_client

s3 = get_client()

try:
    s3.head_bucket(Bucket=BUCKET)
    print(f"Bucket {BUCKET} exists")
except ClientError:
    s3.create_bucket(Bucket=BUCKET)
    print(f"Created bucket {BUCKET}")
