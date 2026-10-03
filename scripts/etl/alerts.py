"""Failure alerts via an incoming webhook (Slack or Discord).

Design rules:
  * An alert must never mask the real failure: every function here swallows
    its own errors and returns False instead of raising.
  * No webhook configured is not an error (local runs, CI): it is logged and
    skipped.
  * The payload carries both "text" (Slack) and "content" (Discord), so the
    same webhook URL format works for either service.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping

import requests

log = logging.getLogger("etl.alerts")

WEBHOOK_ENV_VAR = "ALERT_WEBHOOK_URL"
MAX_MESSAGE_CHARS = 1500  # Discord rejects content over 2000 characters


def send_alert(text: str, webhook_url: str | None = None, timeout: float = 10.0) -> bool:
    """POST a message to the webhook. Returns True only if it was delivered."""
    url = webhook_url or os.environ.get(WEBHOOK_ENV_VAR)
    if not url:
        log.warning("alert not sent: no webhook configured (%s)", WEBHOOK_ENV_VAR)
        return False
    text = text[:MAX_MESSAGE_CHARS]
    try:
        resp = requests.post(url, json={"text": text, "content": text}, timeout=timeout)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001 - alerting must never raise
        log.error("alert delivery failed: %s", exc)
        return False
    return True


def format_failure_message(context: Mapping[str, Any]) -> str:
    """Build a readable message from an Airflow task-failure context."""
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", None) or getattr(context.get("dag"), "dag_id", "unknown")
    task_id = getattr(ti, "task_id", "unknown")
    run_id = context.get("run_id") or getattr(ti, "run_id", "unknown")
    try_number = getattr(ti, "try_number", "?")
    log_url = getattr(ti, "log_url", None)
    exc = context.get("exception")

    lines = [
        f"Airflow task FAILED: {dag_id}.{task_id}",
        f"Run: {run_id} (try {try_number})",
        f"Error: {type(exc).__name__}: {exc}" if exc else "Error: (no exception recorded)",
    ]
    if log_url:
        lines.append(f"Logs: {log_url}")
    return "\n".join(lines)


def notify_failure(context: Mapping[str, Any], webhook_url: str | None = None) -> bool:
    """Airflow on_failure_callback: format the failure and send it."""
    try:
        return send_alert(format_failure_message(context), webhook_url=webhook_url)
    except Exception as exc:  # noqa: BLE001
        log.error("could not build failure alert: %s", exc)
        return False
