"""Simulates an upstream system delivering files: uploads each CSV in /data to the bucket
as one day's delivery, <name>/<YYYY-MM-DD>.csv. The day is yesterday in UTC, so the
pipeline's run for yesterday, the one created when its DAG is first switched on, finds
its file; DELIVERY_DATE (YYYY-MM-DD) delivers it for another day, e.g. before
triggering or backfilling that day.

Not part of the platform itself; in production the source system or the extraction service
deliver the files, one per day.
"""

import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from s3_client import BUCKET, get_client

DATA_DIR = Path("/data")


def delivery_date() -> str:
    """DELIVERY_DATE if set, else yesterday in UTC, as YYYY-MM-DD."""
    value = os.environ.get("DELIVERY_DATE", "").strip()
    if not value:
        return (datetime.now(UTC) - timedelta(days=1)).date().isoformat()
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        sys.exit(f"DELIVERY_DATE must be YYYY-MM-DD, not {value!r}")


day = delivery_date()
s3 = get_client()

for path in sorted(DATA_DIR.glob("*.csv")):
    key = f"{path.stem}/{day}.csv"
    s3.upload_file(str(path), BUCKET, key)
    print(f"Uploaded {path.name} to s3://{BUCKET}/{key}")
