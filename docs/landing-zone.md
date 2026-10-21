# Landing zone

## Why a landing zone

Source systems rarely write straight into a warehouse. They deliver files to object storage, and pipelines pick them up from there. The landing zone reproduces that pattern locally, so ingestion has a real download step:

```
data/<name>.csv ──(landing-zone-upload)──► s3://landing/<name>/<yesterday>.csv ──(ingestion)──► warehouse raw
```

## S3-compatible storage: RustFS

The landing zone is used only through the **S3 API**, the de facto standard for object storage. Any S3-compatible store works, including AWS S3 itself: switching means changing the endpoint and credentials, not the code.

- **RustFS** (`rustfs/rustfs:1.0.1`) was chosen as an open-source, actively maintained, MinIO-compatible store with a web console.
- **MinIO**, the usual choice for this, stopped publishing community Docker images in October 2025, and its open-source repository has since been archived. That is a good illustration of why the platform depends on the S3 API rather than on one product.
- Self-hosted S3 stores need **path-style addressing** (`http://landing-zone-storage:9000/landing/<key>`). The AWS default puts the bucket in the hostname (`landing.landing-zone-storage`), which does not resolve on a Compose network.
- Only the **console** is published to the host (`127.0.0.1:9001`). The S3 API is reachable only from other containers.
- Objects are stored in the `landing_zone_data` volume. RustFS runs as a non-root user that already owns its data folder, so no permission fixes are needed.

## Initialisation vs. simulated delivery

Two one-off jobs, kept separate on purpose:

```
landing-zone-storage (healthy) ──► landing-zone-init (completed) ──► landing-zone-upload
```

| Job | Script | Role |
|---|---|---|
| `landing-zone-init` | `landing-zone/init/01_create_bucket.py` | **Platform setup:** creates the `landing` bucket if missing. In production this would be infrastructure-as-code (e.g. Terraform). |
| `landing-zone-upload` | `landing-zone/simulation/upload_files.py` | **Simulation:** uploads each file in `data/` as yesterday's delivery, `<name>/<yesterday>.csv`, standing in for an upstream system delivering a file per day, so the run created when a pipeline is first switched on finds its file. `DELIVERY_DATE` (YYYY-MM-DD) delivers them for another day instead (see [Operations](operations.md)). It would not exist in production, where the source system or an extraction service delivers the files. |

- Both are idempotent: the bucket is created only if missing, and uploading a file again only replaces the object. Loading its day again replaces the rows of its earlier load, or, if it has not changed, is skipped.
- `landing-zone-upload` waits for `landing-zone-init` to complete successfully, so it never uploads into a missing bucket.
- Airflow does not wait for either. As with the warehouse, a task that finds no file fails and retries.

## Jobs image

Both jobs run from one small image, `data-platform-landing-jobs:local`, built from `landing-zone/Dockerfile`:
- `python:3.13.15-slim` plus `boto3`, pinned, installed at build time (not when containers start).
- The scripts are copied into the image, and each service chooses its script with `command`. A shared module (`s3_client.py`) configures the S3 client the same way for both.
- Runs as the unprivileged `nobody` user: the jobs only read files and call the S3 API.
- Only `data/` is mounted (read-only), so files can change without a rebuild. Script changes need `docker compose up -d --build`.
