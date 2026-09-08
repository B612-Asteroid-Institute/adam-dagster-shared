"""Rich Slack rendering for new Error Reporting groups.

Google's native ER→Slack notification carries only project/service/version
("New error in moeyens-thor-dev error-reporting-alerting/adam-etl unknown"),
which fails the founding requirement that a Slack message say WHAT failed.
This sensor polls ER's groupStats, and for each group it has never announced
posts one rich message built from the group's own data: the headline our log
handler wrote (exception class + step), the exception excerpt, counts,
first-seen, and links. ER remains the sole authority on grouping and novelty
— the only state kept here is which group ids were already announced.

Scoping: only groups whose service starts with "<this namespace>/" are
announced, so a preview namespace announces its own errors and production
announces production's — dev and prod share one ER project.
"""

from __future__ import annotations

import datetime
import json
import os
import re

import dagster as dg
from dagster import SensorEvaluationContext

from .slack import actions_block, current_namespace, link_button, post_message

_ER_BASE = "https://clouderrorreporting.googleapis.com/v1beta1"
_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "moeyens-thor-dev")
_ANNOUNCED_CAP = 500  # ids kept in the cursor before pruning oldest
_EXCERPT_BUDGET = 1800  # stack chars shown in Slack (section text caps at 3000)

# Body lines that restate what the headline already says; dropped from the
# excerpt so the code block leads with signal instead of boilerplate.
_BOILERPLATE = re.compile(
    r"^(Execution of (step|run) .* (failed|canceled)\.?"
    r"|unhandled error executing entry .*"
    r"|uncaught exception: .*"
    r"|Unhandled exception during .*)$"
)
# The most diagnostic single line: the final "<ExceptionClass>: message".
# Matches a (dotted) Capitalized name immediately followed by ": " — exception
# class names end in anything (measured: ZtfCutoutStreamAborted,
# ShardManifestDivergence), so no suffix list. Lowercase "key: value" lines
# and prose with spaces before the colon ("Steps failed: ...") don't match.
_CAUSE = re.compile(r"^(?:[A-Za-z_]\w*\.)*[A-Z]\w*: \S.*")


def _enabled_status() -> dg.DefaultSensorStatus:
    if os.environ.get("ALERTING_ENABLED", "").strip().lower() in ("1", "true", "yes"):
        return dg.DefaultSensorStatus.RUNNING
    return dg.DefaultSensorStatus.STOPPED


def _er_get(path: str, params: dict) -> dict:
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    session = AuthorizedSession(credentials)
    resp = session.get(f"{_ER_BASE}/projects/{_PROJECT}/{path}", params=params, timeout=20)
    resp.raise_for_status()
    return json.loads(resp.text, strict=False)


def parse_reported_message(message: str) -> dict:
    """Split one of our handler-formatted event messages into render parts.

    Layout (see er_log_handler in this repo, and adam-api's er_logging):
    line 1 = "<Cls> in <step>:" or "<Cls> at <METHOD> <route>:"; body = event
    text + error text; final "-- key: value · key: value" footer. Dagster
    events use run_id/job/step keys; adam-api uses request/status/user/client.
    Tolerates foreign shapes by falling back to raw excerpts.
    """
    lines = message.splitlines()
    headline = lines[0].rstrip(":") if lines else "error"
    body_lines = lines[1:]
    footer = ""
    if body_lines and body_lines[-1].startswith("-- "):
        footer = body_lines[-1][3:]
        body_lines = body_lines[:-1]
    meta = {}
    for bit in footer.split("·"):
        bit = bit.strip()
        if ":" in bit:
            key, value = bit.split(":", 1)
            if key.strip() and value.strip():
                meta[key.strip()] = value.strip()
    # The cause line (last "<Class>: message" in the body) is the single most
    # diagnostic line — surfaced separately so Slack readers get the *why*
    # without parsing the stack. Boilerplate lines that restate the headline
    # are dropped from the excerpt.
    cause = ""
    for line in body_lines:
        if _CAUSE.match(line.strip()):
            cause = line.strip()
    kept = [ln for ln in body_lines if not _BOILERPLATE.match(ln.strip())]
    while kept and not kept[0].strip():
        kept = kept[1:]
    # The cause renders as its own quoted line — showing it again as the
    # excerpt's first line is pure duplication.
    if cause and kept and kept[0].strip() == cause:
        kept = kept[1:]
        while kept and not kept[0].strip():
            kept = kept[1:]
    body = "\n".join(kept).strip()
    # Cause chains put the decisive final exception at the END; a head-only
    # excerpt truncates exactly the wrong part. Keep both ends when long,
    # weighted toward the tail (innermost frames + final exception lines).
    if len(body) > _EXCERPT_BUDGET:
        excerpt = body[:700].rstrip() + "\n  ⋯\n" + body[-1000:].lstrip()
    else:
        excerpt = body
    return {
        "headline": headline,
        "excerpt": excerpt,
        "cause": cause,
        "run_id": meta.get("run_id", ""),
        "step": meta.get("step", ""),
        "meta": meta,
    }


def fetch_er_day_summary(namespace: str) -> dict | None:
    """Cross-surface 24h summary from Error Reporting for the daily digest.

    The digest's run-table sweep only sees Dagster; ER sees every bridged
    surface (adam-api, cutout workers, shards). Fail-open: None on any error
    and the digest renders without this section.
    """
    try:
        stats = _er_get(
            "groupStats", {"timeRange.period": "PERIOD_1_DAY", "pageSize": 100}
        ).get("errorGroupStats", [])
        prefix = f"{namespace}/"
        now = datetime.datetime.now(datetime.timezone.utc)
        groups, new_today, top = 0, 0, []
        for g in stats:
            services = {
                s.get("service", "") for s in g.get("affectedServices", []) if s.get("service")
            }
            mine = sorted(s for s in services if s.startswith(prefix))
            if not mine:
                continue
            groups += 1
            first = g.get("firstSeenTime", "")
            try:
                first_dt = datetime.datetime.fromisoformat(first.replace("Z", "+00:00"))
                if (now - first_dt).total_seconds() < 24 * 3600:
                    new_today += 1
            except (ValueError, TypeError):
                pass
            headline = (g.get("representative", {}).get("message") or "").splitlines()
            top.append(
                (
                    int(g.get("count", "1")),
                    (headline[0].rstrip(":") if headline else "error")[:120],
                    mine[0].split("/", 1)[-1],
                    g.get("group", {}).get("groupId", ""),
                )
            )
        top.sort(key=lambda t: -t[0])
        return {"groups": groups, "new_today": new_today, "top": top[:3], "project": _PROJECT}
    except Exception:
        return None


def _relative(first_seen_iso: str) -> str:
    try:
        first = datetime.datetime.fromisoformat(first_seen_iso.replace("Z", "+00:00"))
        mins = (datetime.datetime.now(datetime.timezone.utc) - first).total_seconds() / 60
        if mins < 90:
            return f"{max(1, int(mins))} min ago"
        if mins < 48 * 60:
            return f"{mins / 60:.1f}h ago"
        return f"{mins / 1440:.0f}d ago"
    except (ValueError, TypeError):
        return first_seen_iso[:19]


def render_group_blocks(group_stat: dict, event: dict, namespace: str) -> tuple[str, list]:
    parsed = parse_reported_message(event.get("message", ""))
    group_id = group_stat.get("group", {}).get("groupId", "")
    count = group_stat.get("count", "1")
    services = {
        s.get("service", "") for s in group_stat.get("affectedServices", []) if s.get("service")
    }
    location = next(iter(services), "").split("/", 1)[-1] or "dagster"
    prefix = "" if namespace == "production" else f"[dev · {namespace}] "

    critical = parsed["meta"].get("severity", "").lower() == "critical"
    label = "New CRITICAL error" if critical else "New error class"
    line1 = f"{'🔥' if critical else '🔴'} *{prefix}{label}* — `{parsed['headline']}`"
    # The why, quoted directly under the headline, so triage never requires
    # reading the code block. Skipped when it would just repeat the headline.
    cause_line = ""
    if parsed["cause"] and parsed["cause"] not in parsed["headline"]:
        cause_line = f"\n> {parsed['cause'][:300]}"

    # Two-column facts grid: only populated fields render.
    def _field(name: str, value: str) -> dict:
        return {"type": "mrkdwn", "text": f"*{name}*\n{value}"[:1990]}

    fields = [_field("Service", location)]
    if parsed["step"]:
        fields.append(_field("Step", f"`{parsed['step']}`"))
    if parsed["meta"].get("request"):
        req = parsed["meta"]["request"]
        status = parsed["meta"].get("status", "")
        fields.append(_field("Request", f"`{req}`" + (f" → HTTP {status}" if status else "")))
    if parsed["meta"].get("user"):
        fields.append(_field("User", parsed["meta"]["user"]))
    fields.append(
        _field(
            "Occurrences",
            f"{count} · first seen {_relative(group_stat.get('firstSeenTime', ''))}",
        )
    )
    version = (event.get("serviceContext") or {}).get("version", "")
    if version:
        # "Did the last deploy introduce this?" is the first triage question.
        fields.append(_field("Build", f"`{version}`"))

    # Links live in URL buttons: unfurl_links=false is not honored for links
    # inside blocks (measured), while buttons never unfurl.
    buttons = [
        link_button(
            "View in Error Reporting",
            f"https://console.cloud.google.com/errors/detail/{group_id};time=P30D"
            f"?project={_PROJECT}",
        )
    ]
    dagster_url = os.environ.get("ALERTING_DAGSTER_URL", "").strip()
    if parsed["run_id"] and dagster_url:
        buttons.append(
            link_button("Open run in Dagster", f"{dagster_url}/runs/{parsed['run_id']}")
        )
    footer_bits = [f"run `{parsed['run_id'][:8]}`"] if parsed["run_id"] else []

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"{line1}{cause_line}"[:2900]},
        },
        {"type": "section", "fields": fields[:10]},
    ]
    if parsed["excerpt"]:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"```{parsed['excerpt'][:_EXCERPT_BUDGET]}```"[:2950]},
            }
        )
    if footer_bits:
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": " · ".join(footer_bits)[:2900]}],
            }
        )
    blocks.append(actions_block(buttons))
    fallback = f"{label}: {parsed['headline']} ({location})"
    return fallback, blocks


@dg.sensor(
    name="alerting_er_group_renderer",
    minimum_interval_seconds=120,
    default_status=_enabled_status(),
)
def alerting_er_group_renderer(context: SensorEvaluationContext):
    """Announce Error Reporting groups this environment hasn't announced."""
    namespace = current_namespace()
    try:
        state = json.loads(context.cursor) if context.cursor else {}
    except (ValueError, TypeError):
        state = {}
    announced = state.setdefault("announced", {})
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()

    try:
        stats = _er_get(
            "groupStats",
            {"timeRange.period": "PERIOD_1_WEEK", "pageSize": 100},
        ).get("errorGroupStats", [])
    except Exception as exc:
        context.log.warning(f"alerting renderer: groupStats fetch failed ({exc})")
        return

    scope_prefix = f"{namespace}/"
    first_sync = not announced
    for g in stats:
        services = {
            s.get("service", "")
            for s in g.get("affectedServices", [])
            if s.get("service")
        }
        if not any(s.startswith(scope_prefix) for s in services):
            continue
        group_id = g.get("group", {}).get("groupId", "")
        if not group_id or group_id in announced:
            continue
        announced[group_id] = now
        if first_sync:
            # Adoption run: mark every pre-existing group as known without
            # posting, so enabling the renderer never floods the channel.
            continue
        try:
            events = _er_get(
                "events",
                {"groupId": group_id, "pageSize": 1, "timeRange.period": "PERIOD_1_WEEK"},
            ).get("errorEvents", [])
            event = events[0] if events else {}
            fallback, blocks = render_group_blocks(g, event, namespace)
            posted = post_message(context.log, fallback, blocks)
            context.log.info(
                f"alerting renderer: announced group {group_id} posted={posted}"
            )
        except Exception as exc:
            context.log.warning(f"alerting renderer: group {group_id} failed ({exc})")

    if len(announced) > _ANNOUNCED_CAP:
        for gid in sorted(announced, key=announced.get)[: len(announced) - _ANNOUNCED_CAP]:
            del announced[gid]
    context.update_cursor(json.dumps(state))
