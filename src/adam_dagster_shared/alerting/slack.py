"""Slack message rendering and posting for the alerting pipeline.

Posting is deliberately boring: chat.postMessage with a bot token, one
channel, Block Kit bodies built by pure functions (testable without Slack).

Configuration (all env):
- ALERTING_SLACK_CHANNEL     target channel id (default: #engineering-gcp-alerts)
- ALERTING_SLACK_TOKEN_SECRET  full Secret Manager resource name of the bot
  token (projects/.../secrets/.../versions/latest). Unset => dry-run: the
  exact payload is logged instead of posted. The token is fetched at runtime
  (google-auth is already a transitive dependency) and cached in-process —
  never mounted via envSecrets, which the run launcher would propagate to
  every run pod as a hard requirement (see ai-garden dagster/garden.yml).

Every function swallows its own errors and reports success as a bool: a
Slack outage must never fail a sensor tick or a digest run.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import requests


DEFAULT_CHANNEL = "C05KNK4KZFA"  # #engineering-gcp-alerts


# One namespace resolution for the whole package (env, serviceaccount file,
# "local") — three drifting fallbacks was a measured review finding.
from adam_dagster_shared.er_logging import current_namespace  # noqa: E402,F401

_token_cache: dict[str, tuple[str, float]] = {}
_TOKEN_TTL_SECONDS = 15 * 60


def channel_id() -> str:
    return os.environ.get("ALERTING_SLACK_CHANNEL", "").strip() or DEFAULT_CHANNEL


def dry_run() -> bool:
    """Dry-run unless a token secret is configured and not explicitly forced."""
    if os.environ.get("ALERTING_DRY_RUN", "").strip().lower() in ("1", "true", "yes"):
        return True
    return not os.environ.get("ALERTING_SLACK_TOKEN_SECRET", "").strip()


def env_prefix(namespace: str) -> str:
    """Non-production posts announce their namespace so shared-channel
    readers can tell validation traffic from production signal."""
    if namespace == "production":
        return ""
    return f"[dev · {namespace}] "


def _fetch_bot_token(log) -> str | None:
    secret_name = os.environ.get("ALERTING_SLACK_TOKEN_SECRET", "").strip()
    if not secret_name:
        return None
    cached = _token_cache.get(secret_name)
    if cached and time.monotonic() - cached[1] < _TOKEN_TTL_SECONDS:
        return cached[0]
    try:
        # Lazy import: only the poster needs auth, and only outside dry-run.
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        session = AuthorizedSession(credentials)
        resp = session.get(
            f"https://secretmanager.googleapis.com/v1/{secret_name}:access",
            timeout=10,
        )
        resp.raise_for_status()
        import base64

        token = base64.b64decode(resp.json()["payload"]["data"]).decode("utf-8").strip()
        _token_cache[secret_name] = (token, time.monotonic())
        return token
    except Exception as exc:
        log.warning(f"alerting: bot token fetch failed ({exc}); message not posted")
        return None


def post_message(log, text: str, blocks: list[dict[str, Any]]) -> bool:
    """Post (or, in dry-run, log) one message. Returns True on success."""
    payload = {
        "channel": channel_id(),
        "text": text,
        "blocks": blocks,
        # Console/ER links would each unfurl into a giant generic
        # "Google Cloud Platform" preview card under the message.
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
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
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


# ===== Block builders (pure) =====


def _ctx_block(text: str) -> dict[str, Any]:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text[:2900]}]}


def _section(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text[:2900]}}


def link_button(label: str, url: str) -> dict[str, Any]:
    """Block Kit URL button.

    Links live in buttons, never in mrkdwn text: Slack's unfurl_links=false
    is not honored for links inside blocks (measured — every console link
    grew a giant "Google Cloud Platform" preview card), while URL buttons
    never unfurl.
    """
    return {
        "type": "button",
        "text": {"type": "plain_text", "text": label[:75]},
        "url": url[:3000],
    }


def actions_block(buttons: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "actions", "elements": buttons[:25]}


def digest_blocks(summary: dict[str, Any], namespace: str) -> tuple[str, list[dict[str, Any]]]:
    """Render the daily digest from a plain summary dict (see digest.py)."""
    prefix = env_prefix(namespace)
    total = summary["total_failures"]
    head = f"📋 *{prefix}Daily error digest — {summary['date_label']}*"
    lines = [head, f"*Failures: {total}* · canceled: {summary['canceled']}"]

    if summary["by_class"]:
        class_bits = [f"{klass} {count}" for klass, count in summary["by_class"]]
        lines.append("*By class:* " + " · ".join(class_bits))
    if summary["user_failures"]:
        lines.append(
            f"*Users:* {summary['user_failures']} user-job failures "
            f"({summary['user_count']} users) — handled on their job messages"
        )
    if summary["top_signatures"]:
        sig_lines = [
            f"• ×{count} — {label[:140]} (`{sig[:8]}`)"
            for sig, count, label in summary["top_signatures"]
        ]
        lines.append("*Top Dagster signatures:*\n" + "\n".join(sig_lines))

    blocks = [_section("\n".join(lines))]

    # Cross-surface section from Error Reporting (covers adam-api, cutout
    # workers, shards — everything bridged; the sweep above is Dagster-only).
    er = summary.get("er")
    buttons = []
    if not er:
        # An unavailable cross-surface view must be visible, not silently
        # absent — a digest that quietly narrows to Dagster-only misleads.
        blocks.append(
            _section("*Across all services (Error Reporting):* unavailable this run")
        )
    else:
        maybe = "" if er.get("complete", True) else " (scan capped; counts are a lower bound)"
        er_lines = [
            f"*Across all services (Error Reporting):* "
            f"{er['groups']} active error group{'s' if er['groups'] != 1 else ''} in 24h · "
            f"{er['new_today']} new{maybe}"
        ]
        for count, headline, service, group_id in er.get("top", []):
            er_lines.append(f"• ×{count} — `{headline}` ({service})")
            buttons.append(
                link_button(
                    f"×{count} {headline}",
                    f"https://console.cloud.google.com/errors/detail/{group_id};time=P7D"
                    f"?project={er['project']}",
                )
            )
        blocks.append(_section("\n".join(er_lines)))

    buttons.append(
        link_button(
            "Error Reporting console",
            "https://console.cloud.google.com/errors?project=moeyens-thor-dev",
        )
    )
    blocks.append(actions_block(buttons))
    blocks.append(_ctx_block("covers the last 24h"))
    fallback = f"Daily error digest: {total} failures"
    return fallback, blocks
