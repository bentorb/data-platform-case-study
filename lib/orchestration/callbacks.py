"""Task callbacks shared by every DAG of the platform (see defaults.py).

A task that fails for good, after its retries, is logged as an error and, when
ALERT_WEBHOOK_URL is set, posted to that webhook as {"text": ...}: the format of
Google Chat and Slack incoming webhooks. Retries are only logged.
"""

from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger(__name__)

WEBHOOK_TIMEOUT_SECONDS = 10


def notify_failure(context) -> None:
    """on_failure_callback: logs the failure and sends it to the alert webhook."""
    ti = context["ti"]
    error = context.get("exception")
    message = (
        f"Task failed: {ti.dag_id}.{ti.task_id}\n"
        f"Run: {ti.run_id}, attempt {ti.try_number}\n"
        f"Error: {type(error).__name__ if error else 'unknown'}: {error}\n"
        f"Log: {ti.log_url}"
    )
    log.error(message)

    webhook_url = os.environ.get("ALERT_WEBHOOK_URL")
    if not webhook_url:
        return
    # A failing notification must not hide the task's own error.
    try:
        response = requests.post(webhook_url, json={"text": message}, timeout=WEBHOOK_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        log.exception("Could not send the failure notification to the alert webhook")


def log_retry(context) -> None:
    """on_retry_callback: logs the retry, without notifying anyone."""
    ti = context["ti"]
    error = context.get("exception")
    log.warning(
        "Task %s.%s will be retried after attempt %s of run %s: %s",
        ti.dag_id, ti.task_id, ti.try_number, ti.run_id, error,
    )
