"""Task callbacks shared by every DAG of the platform (see defaults.py).

A task that fails for good, after its retries, is logged as an error and posted to its
owner's alert webhook as {"text": ...}: the format of Google Chat and Slack incoming
webhooks. Each owner's webhook is an Airflow connection named alerts_<owner> (e.g.
alerts_data_platform for owner data-platform), with the URL in its extra as
{"webhook_url": ...}. Owners without one fall back to ALERT_WEBHOOK_URL; with neither,
failures are only logged. Retries are only logged.
"""

from __future__ import annotations

import logging
import os

import requests
from airflow.sdk import Connection

log = logging.getLogger(__name__)

WEBHOOK_TIMEOUT_SECONDS = 10


def notify_failure(context) -> None:
    """on_failure_callback: logs the failure and sends it to the owner's alert webhook."""
    ti = context["ti"]
    owner = context["task"].owner
    error = context.get("exception")
    message = (
        f"Task failed: {ti.dag_id}.{ti.task_id}\n"
        f"Owner: {owner}\n"
        f"Run: {ti.run_id}, attempt {ti.try_number}\n"
        f"Error: {type(error).__name__ if error else 'unknown'}: {error}\n"
        f"Log: {ti.log_url}"
    )
    log.error(message)

    webhook_url = owner_webhook_url(owner) or os.environ.get("ALERT_WEBHOOK_URL")
    if not webhook_url:
        return
    # A failing notification must not hide the task's own error.
    try:
        response = requests.post(webhook_url, json={"text": message}, timeout=WEBHOOK_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        log.exception("Could not send the failure notification to the alert webhook")


def owner_webhook_url(owner: str) -> str | None:
    """The webhook URL of the owner's alerts_<owner> connection, or None without one."""
    conn_id = f"alerts_{owner.replace('-', '_')}"
    try:
        connection = Connection.get(conn_id)
    except Exception:  # Not defined, or not readable: fall back to the default webhook.
        return None
    return connection.extra_dejson.get("webhook_url") or None


def log_retry(context) -> None:
    """on_retry_callback: logs the retry, without notifying anyone."""
    ti = context["ti"]
    error = context.get("exception")
    log.warning(
        "Task %s.%s will be retried after attempt %s of run %s: %s",
        ti.dag_id, ti.task_id, ti.try_number, ti.run_id, error,
    )
