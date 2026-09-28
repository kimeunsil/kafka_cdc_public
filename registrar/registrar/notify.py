"""Slack Incoming Webhook 알림 (SLACK_WEBHOOK_URL 이 없으면 로그만 남김)."""

from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger(__name__)


def notify(text: str) -> None:
    url = os.environ.get("SLACK_WEBHOOK_URL")
    cluster = os.environ.get("CLUSTER_NAME", "")
    msg = f"[{cluster}] {text}" if cluster else text
    log.warning(msg)
    if not url:
        return
    try:
        requests.post(url, json={"text": msg}, timeout=5)
    except requests.RequestException as e:  # 알림 실패가 동기화를 막지 않도록
        log.error("slack 알림 실패: %s", e)
