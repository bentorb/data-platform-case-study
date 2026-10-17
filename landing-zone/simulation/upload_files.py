"""Simulates an upstream system delivering files: uploads the files in /data to the bucket.

Not part of the platform itself; in production the source system or the extraction service
deliver the files.
"""

from pathlib import Path

from s3_client import BUCKET, get_client

DATA_DIR = Path("/data")

s3 = get_client()

for path in sorted(DATA_DIR.glob("*.csv")):
    s3.upload_file(str(path), BUCKET, path.name)
    print(f"Uploaded {path.name} to s3://{BUCKET}/{path.name}")
