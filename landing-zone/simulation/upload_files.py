"""Simulates an upstream system delivering files: uploads each CSV in /data to the bucket
as yesterday's delivery, <name>/<YYYY-MM-DD>.csv, with yesterday's date in UTC. The
pipeline's run for yesterday, the one created when its DAG is first switched on, then
finds its file.

Not part of the platform itself; in production the source system or the extraction service
deliver the files, one per day.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from s3_client import BUCKET, get_client

DATA_DIR = Path("/data")

yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
s3 = get_client()

for path in sorted(DATA_DIR.glob("*.csv")):
    key = f"{path.stem}/{yesterday}.csv"
    s3.upload_file(str(path), BUCKET, key)
    print(f"Uploaded {path.name} to s3://{BUCKET}/{key}")
