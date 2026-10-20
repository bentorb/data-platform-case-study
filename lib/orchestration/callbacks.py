"""Task callbacks shared by every DAG of the platform (see defaults.py).

A task that fails for good, after its retries, is logged as an error and posted to its
owner's alert webhook as {"text": ...}: the format of Google Chat and Slack incoming
webhooks. Each owner's webhook is an Airflow connection named alerts_<owner> (e.g.
alerts_data_platform for owner data-platform), with the URL in its extra as
{"webhook_url": ...}. Owners without one fall back to ALERT_WEBHOOK_URL; with neither,
alerts are only logged. Retries are only logged.

dbt tests that warn (severity: warn) do not fail their task, so they are reported the
same way by a separate callback, given to Cosmos's dbt tasks (lib/transformation/dbt.py).
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
    send_alert(owner, message)


def notify_warning(context) -> None:
    """Cosmos's on_warning_callback: logs the dbt tests that warned and sends them to the
    owner's alert webhook. Cosmos adds the tests' names and result lines to the context
    (test_names, test_results), and calls it only when at least one test warned."""
    ti = context["ti"]
    owner = context["task"].owner
    warnings = "\n".join(
        f"- {name}: {result}"
        for name, result in zip(context.get("test_names", []), context.get("test_results", []), strict=False)
    )
    message = (
        f"dbt tests warned: {ti.dag_id}.{ti.task_id}\n"
        f"Owner: {owner}\n"
        f"Run: {ti.run_id}, attempt {ti.try_number}\n"
        f"Warnings:\n{warnings or '- (not parsed from the dbt output; see the log)'}\n"
        f"Log: {ti.log_url}"
    )
    log.warning(message)
    send_alert(owner, message)


def send_alert(owner: str, message: str) -> None:
    """Posts `message` to the owner's alert webhook, or the default one; with neither,
    does nothing. A failing webhook is logged, never raised: an alert must not hide the
    task's own error, or fail a task whose tests only warned."""
    webhook_url = owner_webhook_url(owner) or os.environ.get("ALERT_WEBHOOK_URL")
    if not webhook_url:
        return
    try:
        response = requests.post(webhook_url, json={"text": message}, timeout=WEBHOOK_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        log.exception("Could not send the alert to the webhook")


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
        ti.dag_id,
        ti.task_id,
        ti.try_number,
        ti.run_id,
        error,
    )
