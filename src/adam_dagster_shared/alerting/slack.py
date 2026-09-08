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

from .classify import Tier, Verdict, origin_label

DEFAULT_CHANNEL = "C05KNK4KZFA"  # #engineering-gcp-alerts


def current_namespace() -> str:
    """Same resolution order as dag.utils.get_current_namespace, duplicated
    here so the alerting package stays importable with only dagster+requests
    (dag.utils pulls astropy/kubernetes/google-cloud-container)."""
    for env_var in ("GARDEN_NAMESPACE", "K8S_NAMESPACE", "KUBERNETES_NAMESPACE"):
        value = os.environ.get(env_var)
        if value and value.strip():
            return value.strip()
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
            return f.read().strip()
    except FileNotFoundError:
        return "nonamespace"

_TIER_EMOJI = {
    Tier.PAGE: "🔴",
    Tier.NOTIFY: "🟡",
    Tier.DIGEST: "📋",
    Tier.USER_FACING: "🔭",
}

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


def alert_blocks(
    verdict: Verdict,
    *,
    job_name: str,
    run_id: str,
    tags: dict[str, str],
    namespace: str,
    occurrences_24h: int,
    first_seen_ts: float | None,
    escalated: bool,
    webserver_url: str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Render one PAGE/NOTIFY alert. Returns (fallback_text, blocks)."""
    tier = Tier.PAGE if escalated else verdict.tier
    emoji = _TIER_EMOJI[tier]
    prefix = env_prefix(namespace)
    origin = origin_label(tags)

    headline = (
        f"{emoji} *{prefix}{tier.value}* — {verdict.klass}"
        + (" (escalated: signature clustering)" if escalated else "")
    )
    where = f"*{job_name}*" + (f" · step `{verdict.step_key}`" if verdict.step_key else "")

    lines = [headline, where, verdict.reason]
    if verdict.exception:
        lines.append(f"```{verdict.exception[:600]}```")

    detail_bits = [f"origin: {origin}", f"signature `{verdict.signature}`"]
    if occurrences_24h > 1:
        detail_bits.append(f"*{occurrences_24h} occurrences in 24h*")
    if first_seen_ts is not None:
        age_h = max(0.0, (time.time() - first_seen_ts) / 3600.0)
        detail_bits.append(f"first seen {age_h:.1f}h ago")
    partition = tags.get("dagster/partition")
    if partition:
        detail_bits.append(f"partition `{partition}`")
    backfill = tags.get("dagster/backfill")
    if backfill:
        detail_bits.append(f"backfill `{backfill}`")

    blocks = [_section("\n".join(lines)), _ctx_block(" · ".join(detail_bits))]
    blocks.append(_ctx_block(f"run `{run_id[:8]}`"))
    if webserver_url:
        blocks.append(
            actions_block([link_button("Open run in Dagster", f"{webserver_url}/runs/{run_id}")])
        )

    fallback = f"{tier.value} {verdict.klass}: {job_name} ({verdict.reason})"
    return fallback, blocks


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
    baseline = summary.get("baseline_median_per_day")
    head = f"📋 *{prefix}Daily error digest — {summary['date_label']}*"
    vs = f" vs trailing median {baseline}/day" if baseline is not None else ""
    lines = [head, f"*Failures: {total}*{vs} · canceled: {summary['canceled']}"]

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
    if er:
        er_lines = [
            f"*Across all services (Error Reporting):* "
            f"{er['groups']} active error group{'s' if er['groups'] != 1 else ''} in 24h · "
            f"{er['new_today']} new"
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
