# Image for the landing zone's one-off jobs (landing-zone-init, landing-zone-upload):
# Python and boto3, nothing else. Each service chooses its script with `command`.
FROM python:3.13.15-slim

RUN pip install --no-cache-dir boto3==1.43.108

COPY s3_client.py /opt/landing-zone/
COPY init/ /opt/landing-zone/init/
COPY simulation/ /opt/landing-zone/simulation/

# Lets the scripts import the shared s3_client module.
ENV PYTHONPATH=/opt/landing-zone

# Run as an unprivileged user; the scripts only read files and call the S3 API.
USER nobody

ENTRYPOINT ["python"]
