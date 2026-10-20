"""Failure alerts (lib/orchestration/callbacks.py): routed to the owner's webhook, then the
default one, and never hiding the task's own error."""

from types import SimpleNamespace

import pytest
import requests

from lib.orchestration import callbacks


class FakeConnections:
    """Stands in for airflow.sdk.Connection: get() returns the connections given."""

    def __init__(self, webhooks):
        self.webhooks = webhooks

    def get(self, conn_id):
        if conn_id not in self.webhooks:
            raise LookupError(conn_id)
        return SimpleNamespace(extra_dejson={"webhook_url": self.webhooks[conn_id]})


@pytest.fixture
def posts(monkeypatch):
    """Records the webhook posts instead of sending them."""
    sent = []

    def post(url, json, timeout):
        sent.append((url, json["text"]))
        return SimpleNamespace(raise_for_status=lambda: None)

    monkeypatch.setattr(callbacks.requests, "post", post)
    return sent


def context(owner):
    ti = SimpleNamespace(dag_id="orders_pipeline", task_id="load", run_id="run_1", try_number=3, log_url="http://log")
    return {"ti": ti, "task": SimpleNamespace(owner=owner), "exception": ValueError("boom")}


@pytest.mark.parametrize(
    "owner, webhooks, default, expected_url",
    [
        ("sales-data", {"alerts_sales_data": "http://sales"}, "http://default", "http://sales"),
        ("sales-data", {}, "http://default", "http://default"),
        ("sales-data", {"alerts_sales_data": ""}, "http://default", "http://default"),
    ],
)
def test_alert_routing(monkeypatch, posts, owner, webhooks, default, expected_url):
    monkeypatch.setattr(callbacks, "Connection", FakeConnections(webhooks))
    monkeypatch.setenv("ALERT_WEBHOOK_URL", default)
    callbacks.notify_failure(context(owner))
    assert [url for url, _ in posts] == [expected_url]
    message = posts[0][1]
    assert "orders_pipeline.load" in message and f"Owner: {owner}" in message and "ValueError: boom" in message


def test_no_webhook_only_logs(monkeypatch, posts):
    monkeypatch.setattr(callbacks, "Connection", FakeConnections({}))
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    callbacks.notify_failure(context("sales-data"))
    assert posts == []


def test_an_unreachable_webhook_is_only_logged(monkeypatch, caplog):
    monkeypatch.setattr(callbacks, "Connection", FakeConnections({}))
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://default")

    def post(url, json, timeout):
        raise requests.ConnectionError("unreachable")

    monkeypatch.setattr(callbacks.requests, "post", post)
    callbacks.notify_failure(context("sales-data"))  # does not raise
    assert "Could not send the failure notification" in caplog.text
