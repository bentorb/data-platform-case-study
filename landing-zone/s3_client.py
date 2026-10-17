"""S3 client for the landing zone, configured from environment variables."""

import os

import boto3
from botocore.config import Config

BUCKET = os.environ["LANDING_ZONE_BUCKET"]


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["LANDING_ZONE_ENDPOINT_URL"],
        aws_access_key_id=os.environ["LANDING_ZONE_ACCESS_KEY"],
        aws_secret_access_key=os.environ["LANDING_ZONE_SECRET_KEY"],
        region_name="eu-south-2",
        # Self-hosted S3 stores need the bucket in the path, not the hostname.
        config=Config(s3={"addressing_style": "path"}),
    )
