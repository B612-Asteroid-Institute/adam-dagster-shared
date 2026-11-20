"""Slack posting and Block Kit builders.

Dry-run (payload logged, nothing posted) unless ALERTING_SLACK_TOKEN_SECRET names
the bot token in Secret Manager; ALERTING_DRY_RUN=true forces dry-run. Posting never
raises: success is reported as a bool.
"""

from __future__ import annotations

import base64
import json
import os
import time
from typing import Any

import requests

from adam_dagster_shared.er_logging import env_flag

DEFAULT_CHANNEL = "C05KNK4KZFA"  # #engineering-gcp-alerts
PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "moeyens-thor-dev")
_TOKEN_TTL_SECONDS = 15 * 60
_token_cache: dict[str, tuple[str, float]] = {}


def channel_id() -> str:
    return os.environ.get("ALERTING_SLACK_CHANNEL", "").strip() or DEFAULT_CHANNEL


def dry_run() -> bool:
    return env_flag("ALERTING_DRY_RUN") or not os.environ.get(
        "ALERTING_SLACK_TOKEN_SECRET", ""
    ).strip()


def env_prefix(namespace: str) -> str:
    return "" if namespace == "production" else f"[dev · {namespace}] "


def _fetch_bot_token(log) -> str | None:
    secret_name = os.environ.get("ALERTING_SLACK_TOKEN_SECRET", "").strip()
    cached = _token_cache.get(secret_name)
    if cached and time.monotonic() - cached[1] < _TOKEN_TTL_SECONDS:
        return cached[0]
    try:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        resp = AuthorizedSession(credentials).get(
            f"https://secretmanager.googleapis.com/v1/{secret_name}:access", timeout=10
        )
        resp.raise_for_status()
        token = base64.b64decode(resp.json()["payload"]["data"]).decode().strip()
        _token_cache[secret_name] = (token, time.monotonic())
        return token
    except Exception as exc:
        log.warning(f"alerting: bot token fetch failed ({exc}); message not posted")
        return None


def post_message(log, text: str, blocks: list[dict[str, Any]]) -> bool:
    payload = {
        "channel": channel_id(),
        "text": text,
        "blocks": blocks,
        "unfurl_links": False,
        "unfurl_media": False,
    }
    if dry_run():
        log.info("ALERTING_DRY_RUN_POST " + json.dumps(payload, ensure_ascii=False))
        return True
    token = _fetch_bot_token(log)
    if token is None:
        return False
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
            timeout=5,
        )
        body = resp.json() if resp.status_code < 500 else {}
        if resp.status_code >= 400 or not body.get("ok"):
            log.warning(
                f"alerting: chat.postMessage failed status={resp.status_code} "
                f"error={body.get('error', 'unknown')}"
            )
            return False
        return True
    except Exception as exc:
        log.warning(f"alerting: chat.postMessage exception ({exc})")
        return False


# Block Kit builders. Links go in URL buttons only: Slack ignores unfurl_links=false
# for links inside blocks, and buttons never unfurl.


def section(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text[:2900]}}


def context_block(text: str) -> dict[str, Any]:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text[:2900]}]}


def link_button(label: str, url: str) -> dict[str, Any]:
    return {"type": "button", "text": {"type": "plain_text", "text": label[:75]}, "url": url[:3000]}


def actions_block(buttons: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "actions", "elements": buttons[:25]}


def digest_blocks(summary: dict[str, Any], namespace: str) -> tuple[str, list[dict[str, Any]]]:
    total = summary["total_failures"]
    lines = [
        f"📋 *{env_prefix(namespace)}Daily error digest — {summary['date_label']}*",
        f"*Failures: {total}* · canceled: {summary['canceled']}",
    ]
    if summary["by_class"]:
        lines.append("*By class:* " + " · ".join(f"{k} {n}" for k, n in summary["by_class"]))
    if summary["user_failures"]:
        lines.append(
            f"*Users:* {summary['user_failures']} user-job failures "
            f"({summary['user_count']} users) — handled on their job messages"
        )
    blocks = [section("\n".join(lines))]

    buttons = []
    er = summary.get("er")
    if not er:
        blocks.append(section("*Across all services (Error Reporting):* unavailable this run"))
    else:
        plural = "s" if er["groups"] != 1 else ""
        capped = "" if er.get("complete", True) else " (scan capped; counts are a lower bound)"
        er_lines = [
            f"*Across all services (Error Reporting):* {er['groups']} active error "
            f"group{plural} in 24h · {er['new_today']} new{capped}"
        ]
        for count, headline, service, group_id in er.get("top", []):
            er_lines.append(f"• ×{count} — `{headline}` ({service})")
            buttons.append(
                link_button(
                    f"×{count} {headline}",
                    f"https://console.cloud.google.com/errors/detail/{group_id};time=P7D"
                    f"?project={PROJECT}",
                )
            )
        blocks.append(section("\n".join(er_lines)))
    buttons.append(
        link_button(
            "Error Reporting console",
            f"https://console.cloud.google.com/errors?project={PROJECT}",
        )
    )
    blocks.append(actions_block(buttons))
    blocks.append(context_block("covers the last 24h"))
    return f"Daily error digest: {total} failures", blocks
