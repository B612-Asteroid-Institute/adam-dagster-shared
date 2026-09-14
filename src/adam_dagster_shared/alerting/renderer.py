"""Error Reporting group renderer: one rich Slack card per new ER group in this namespace.

ER owns grouping and novelty; the sensor cursor only remembers which group ids it has
announced. Delivery semantics (adoption snapshot, pending retry, per-tick budget) are
in docs/ALERTING.md.
"""

from __future__ import annotations

import datetime
import json
import os
import re

import dagster as dg

from adam_dagster_shared.er_logging import current_namespace

from .sensors import enabled_status, load_cursor
from .slack import (
    PROJECT,
    actions_block,
    context_block,
    env_prefix,
    link_button,
    post_message,
    section,
)

_ER_BASE = "https://clouderrorreporting.googleapis.com/v1beta1"
_ANNOUNCED_CAP = 500  # group ids kept in the cursor
_EXCERPT_BUDGET = 1800  # stack chars per card
_PAGE_CAP = 10  # groupStats pages of 100 per fetch
_POST_BUDGET_PER_TICK = 10

# Body lines that only restate the headline.
_BOILERPLATE = re.compile(
    r"^(Execution of (step|run) .* (failed|canceled)\.?"
    r"|unhandled error executing entry .*"
    r"|uncaught exception: .*"
    r"|Unhandled exception during .*)$"
)
# "<ExceptionClass>: message" — the most diagnostic single line.
_CAUSE = re.compile(r"^(?:[A-Za-z_]\w*\.)*[A-Z]\w*: \S.*")


def _er_get(path: str, params: dict) -> dict:
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    resp = AuthorizedSession(credentials).get(
        f"{_ER_BASE}/projects/{PROJECT}/{path}", params=params, timeout=20
    )
    resp.raise_for_status()
    return json.loads(resp.text, strict=False)


def _fetch_group_stats(period: str) -> tuple[list, bool]:
    """All groupStats pages up to _PAGE_CAP; returns (stats, complete)."""
    stats: list = []
    token = ""
    for _ in range(_PAGE_CAP):
        params = {"timeRange.period": period, "pageSize": 100}
        if token:
            params["pageToken"] = token
        page = _er_get("groupStats", params)
        stats.extend(page.get("errorGroupStats", []))
        token = page.get("nextPageToken", "")
        if not token:
            return stats, True
    return stats, False


def _in_scope(stats: list, namespace: str) -> dict[str, tuple[dict, str]]:
    """group id -> (stat, first affected service in this namespace)."""
    prefix = f"{namespace}/"
    out: dict[str, tuple[dict, str]] = {}
    for g in stats:
        mine = sorted(
            s["service"]
            for s in g.get("affectedServices", [])
            if s.get("service", "").startswith(prefix)
        )
        group_id = g.get("group", {}).get("groupId", "")
        if mine and group_id:
            out[group_id] = (g, mine[0])
    return out


def _parse_time(iso: str) -> datetime.datetime | None:
    try:
        return datetime.datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError):
        return None


def _relative(iso: str) -> str:
    first = _parse_time(iso)
    if first is None:
        return iso[:19]
    mins = (datetime.datetime.now(datetime.timezone.utc) - first).total_seconds() / 60
    if mins < 90:
        return f"{max(1, int(mins))} min ago"
    if mins < 48 * 60:
        return f"{mins / 60:.1f}h ago"
    return f"{mins / 1440:.0f}d ago"


def parse_reported_message(message: str) -> dict:
    """Split a handler-formatted message into headline, cause line, excerpt and footer meta."""
    lines = message.splitlines()
    headline = lines[0].rstrip(":") if lines else "error"
    body = lines[1:]
    meta: dict[str, str] = {}
    if body and body[-1].startswith("-- "):
        for bit in body.pop()[3:].split("·"):
            key, _, value = bit.partition(":")
            if key.strip() and value.strip():
                meta[key.strip()] = value.strip()
    cause = ""
    for line in body:
        if _CAUSE.match(line.strip()):
            cause = line.strip()
    kept = [ln for ln in body if not _BOILERPLATE.match(ln.strip())]
    # The cause renders as its own quoted line; don't repeat it at the top of the excerpt.
    while kept and (not kept[0].strip() or kept[0].strip() == cause):
        kept.pop(0)
    text = "\n".join(kept).strip()
    if len(text) > _EXCERPT_BUDGET:  # keep both ends: the decisive exception is at the tail
        text = text[:700].rstrip() + "\n  ⋯\n" + text[-1000:].lstrip()
    return {"headline": headline, "cause": cause, "excerpt": text, "meta": meta}


def fetch_er_day_summary(namespace: str) -> dict | None:
    """24h counts across every bridged surface, for the digest; None (fail-open) on error."""
    try:
        stats, complete = _fetch_group_stats("PERIOD_1_DAY")
        now = datetime.datetime.now(datetime.timezone.utc)
        rows = []
        new_today = 0
        for group_id, (g, service) in _in_scope(stats, namespace).items():
            first = _parse_time(g.get("firstSeenTime", ""))
            if first and (now - first).total_seconds() < 86400:
                new_today += 1
            headline = (g.get("representative", {}).get("message") or "error").splitlines()[0]
            rows.append(
                (int(g.get("count", "1")), headline.rstrip(":")[:120], service.split("/", 1)[-1], group_id)
            )
        rows.sort(key=lambda r: -r[0])
        return {"groups": len(rows), "new_today": new_today, "top": rows[:3], "complete": complete}
    except Exception:
        return None


def render_group_blocks(group_stat: dict, event: dict, namespace: str, service: str) -> tuple[str, list]:
    p = parse_reported_message(event.get("message", ""))
    meta = p["meta"]
    group_id = group_stat.get("group", {}).get("groupId", "")
    location = service.split("/", 1)[-1] or "dagster"
    critical = meta.get("severity", "").lower() == "critical"
    label = "New CRITICAL error" if critical else "New error class"
    head = f"{'🔥' if critical else '🔴'} *{env_prefix(namespace)}{label}* — `{p['headline']}`"
    if p["cause"] and p["cause"] not in p["headline"]:
        head += f"\n> {p['cause'][:300]}"

    fields = [("Service", location)]
    if meta.get("step"):
        fields.append(("Step", f"`{meta['step']}`"))
    if meta.get("request"):
        status = f" → HTTP {meta['status']}" if meta.get("status") else ""
        fields.append(("Request", f"`{meta['request']}`{status}"))
    if meta.get("user"):
        fields.append(("User", meta["user"]))
    fields.append(
        ("Occurrences", f"{group_stat.get('count', '1')} · first seen {_relative(group_stat.get('firstSeenTime', ''))}")
    )
    version = (event.get("serviceContext") or {}).get("version", "")
    if version:
        fields.append(("Build", f"`{version}`"))

    buttons = [
        link_button(
            "View in Error Reporting",
            f"https://console.cloud.google.com/errors/detail/{group_id};time=P30D?project={PROJECT}",
        )
    ]
    run_id = meta.get("run_id", "")
    dagster_url = os.environ.get("ALERTING_DAGSTER_URL", "").strip()
    if run_id and dagster_url:
        buttons.append(link_button("Open run in Dagster", f"{dagster_url}/runs/{run_id}"))

    blocks = [
        section(head),
        {
            "type": "section",
            "fields": [{"type": "mrkdwn", "text": f"*{k}*\n{v}"[:1990]} for k, v in fields[:10]],
        },
    ]
    if p["excerpt"]:
        blocks.append(section(f"```{p['excerpt']}```"))
    if run_id:
        blocks.append(context_block(f"run `{run_id[:8]}`"))
    blocks.append(actions_block(buttons))
    return f"{label}: {p['headline']} ({location})", blocks


@dg.sensor(
    name="alerting_er_group_renderer",
    minimum_interval_seconds=120,
    default_status=enabled_status(),
)
def alerting_er_group_renderer(context: dg.SensorEvaluationContext):
    namespace = current_namespace()
    state = load_cursor(context.cursor)
    announced = state.setdefault("announced", {})
    pending = state.setdefault("pending", {})
    state.setdefault("initialized", bool(announced))  # pre-flag cursors already adopted
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()

    try:
        stats, complete = _fetch_group_stats("PERIOD_1_WEEK")
    except Exception as exc:
        context.log.warning(f"alerting renderer: groupStats fetch failed ({exc})")
        return
    in_scope = _in_scope(stats, namespace)

    if not state["initialized"]:
        # The first COMPLETE snapshot is adopted silently; a capped scan waits.
        if complete:
            for group_id in in_scope:
                announced.setdefault(group_id, now)
            state["initialized"] = True
            context.log.info(f"alerting renderer: initialized; adopted {len(in_scope)} existing groups")
        else:
            context.log.warning("alerting renderer: discovery incomplete (page cap); initialization deferred")
        context.update_cursor(json.dumps(state))
        return

    for group_id in in_scope:
        if group_id not in announced:
            pending.setdefault(group_id, now)
    if complete:  # groups gone from the window can never be rendered
        for group_id in [gid for gid in pending if gid not in in_scope]:
            del pending[group_id]

    delivered = 0
    for group_id in sorted(pending, key=pending.get):
        if group_id not in in_scope:
            continue
        if delivered >= _POST_BUDGET_PER_TICK:
            context.log.info(f"alerting renderer: post budget reached; {len(pending)} still pending")
            break
        g, service = in_scope[group_id]
        delivered += 1
        try:
            # Sample from this namespace's service: a group shared with another
            # environment must not show that environment's traceback.
            events = _er_get(
                "events",
                {
                    "groupId": group_id,
                    "pageSize": 1,
                    "timeRange.period": "PERIOD_1_WEEK",
                    "serviceFilter.service": service,
                },
            ).get("errorEvents", [])
            fallback, blocks = render_group_blocks(g, events[0] if events else {}, namespace, service)
            if post_message(context.log, fallback, blocks):
                announced[group_id] = now
                del pending[group_id]
                context.log.info(f"alerting renderer: announced group {group_id} posted=True")
            else:
                context.log.warning(f"alerting renderer: delivery failed for group {group_id}; kept pending")
        except Exception as exc:
            context.log.warning(f"alerting renderer: group {group_id} failed ({exc}); kept pending")

    for gid in sorted(announced, key=announced.get)[: max(0, len(announced) - _ANNOUNCED_CAP)]:
        del announced[gid]
    context.update_cursor(json.dumps(state))
