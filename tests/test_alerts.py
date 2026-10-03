"""Tests for webhook failure alerts. No network: requests.post is faked."""

from __future__ import annotations

from types import SimpleNamespace

import etl.alerts as alerts


class _Resp:
    def __init__(self, ok=True):
        self.ok = ok

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError("HTTP 500")


def _context():
    ti = SimpleNamespace(
        dag_id="etl_usgs_earthquakes_load",
        task_id="load_usgs_earthquakes",
        run_id="manual__2026-09-25",
        try_number=3,
        log_url="http://localhost:8080/log",
    )
    return {"task_instance": ti, "run_id": ti.run_id, "exception": ValueError("boom")}


def test_no_webhook_configured_is_a_quiet_no_op(monkeypatch):
    monkeypatch.delenv(alerts.WEBHOOK_ENV_VAR, raising=False)
    called = []
    monkeypatch.setattr(alerts.requests, "post", lambda *a, **k: called.append(1))
    assert alerts.send_alert("hi") is False
    assert called == []


def test_payload_works_for_slack_and_discord(monkeypatch):
    sent = {}

    def fake_post(url, json, timeout):
        sent.update(url=url, json=json, timeout=timeout)
        return _Resp()

    monkeypatch.setattr(alerts.requests, "post", fake_post)
    assert alerts.send_alert("hello", webhook_url="https://example.test/hook") is True
    assert sent["json"] == {"text": "hello", "content": "hello"}


def test_env_var_is_used_when_no_url_passed(monkeypatch):
    monkeypatch.setenv(alerts.WEBHOOK_ENV_VAR, "https://example.test/env")
    seen = []
    monkeypatch.setattr(alerts.requests, "post", lambda url, json, timeout: seen.append(url) or _Resp())
    assert alerts.send_alert("hi") is True
    assert seen == ["https://example.test/env"]


def test_delivery_failure_never_raises(monkeypatch):
    def boom(*a, **k):
        raise ConnectionError("network down")

    monkeypatch.setattr(alerts.requests, "post", boom)
    assert alerts.send_alert("hi", webhook_url="https://example.test/hook") is False

    monkeypatch.setattr(alerts.requests, "post", lambda *a, **k: _Resp(ok=False))
    assert alerts.send_alert("hi", webhook_url="https://example.test/hook") is False


def test_long_messages_are_truncated(monkeypatch):
    sent = {}
    monkeypatch.setattr(alerts.requests, "post", lambda url, json, timeout: sent.update(json=json) or _Resp())
    alerts.send_alert("x" * 5000, webhook_url="https://example.test/hook")
    assert len(sent["json"]["content"]) == alerts.MAX_MESSAGE_CHARS


def test_failure_message_names_dag_task_and_error():
    msg = alerts.format_failure_message(_context())
    assert "etl_usgs_earthquakes_load.load_usgs_earthquakes" in msg
    assert "ValueError: boom" in msg
    assert "try 3" in msg
    assert "http://localhost:8080/log" in msg


def test_notify_failure_survives_a_garbage_context(monkeypatch):
    monkeypatch.setattr(alerts.requests, "post", lambda *a, **k: _Resp())
    assert alerts.notify_failure({}, webhook_url="https://example.test/hook") is True
