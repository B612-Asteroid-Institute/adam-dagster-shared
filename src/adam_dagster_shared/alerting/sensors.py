"""Daemon-side watches: hung runs, failing sensor/schedule ticks, runs that never start.

Everything here happens outside run pods, where no log handler can see it.
"""

from __future__ import annotations

import datetime
import json
import os
from collections import Counter

import dagster as dg
from dagster import DagsterRunStatus, RunsFilter
from dagster._core.definitions.run_request import InstigatorType
from dagster._core.scheduler.instigation import TickStatus

from adam_dagster_shared.er_logging import current_namespace, env_flag

from .classify import classify
from .extract import extract_failure_context
from .slack import actions_block, env_prefix, link_button, post_message, section

_FLOOR_SECONDS = 2 * 3600
_NO_HISTORY_SECONDS = 6 * 3600
_P95_FACTOR = 3.0
_BASELINE_TTL = 24 * 3600
_REALERT = 24 * 3600
_BASELINE_SAMPLE = 200  # recent successes per job
_PAGE = 200
_PAGE_CAP = 10  # at most 2000 STARTED runs per tick


def enabled_status() -> dg.DefaultSensorStatus:
    if env_flag("ALERTING_ENABLED"):
        return dg.DefaultSensorStatus.RUNNING
    return dg.DefaultSensorStatus.STOPPED


def load_cursor(cursor: str | None) -> dict:
    try:
        return json.loads(cursor) if cursor else {}
    except (ValueError, TypeError):
        return {}


def p95_seconds(durations: list[float]) -> float | None:
    cleaned = sorted(d for d in durations if d and d > 0)
    if not cleaned:
        return None
    return cleaned[min(len(cleaned) - 1, int(len(cleaned) * 0.95))]


def hung_threshold(p95: float | None) -> float:
    if p95 is None:
        return _NO_HISTORY_SECONDS
    return max(_FLOOR_SECONDS, _P95_FACTOR * p95)


def watchdog_should_flag(state: dict, run_id: str, age: float, threshold: float, now: float) -> bool:
    """Record and return whether the run newly exceeds its threshold (re-alerts after 24h).

    Deliberately independent of Slack posting so verdicts are still logged with posting off.
    """
    if age <= threshold or now - state["alerted"].get(run_id, 0.0) <= _REALERT:
        return False
    state["alerted"][run_id] = now
    return True


def _fetch_started(instance) -> tuple[list, bool]:
    """STARTED runs oldest-first, paginated; returns (records, complete)."""
    records: list = []
    seen: set[str] = set()
    cursor = None
    for _ in range(_PAGE_CAP):
        page = instance.get_run_records(
            filters=RunsFilter(statuses=[DagsterRunStatus.STARTED]),
            limit=_PAGE,
            ascending=True,
            cursor=cursor,
        )
        new = [r for r in page if r.dagster_run.run_id not in seen]
        if not new:
            return records, len(page) < _PAGE
        records.extend(new)
        seen.update(r.dagster_run.run_id for r in new)
        if len(page) < _PAGE:
            return records, True
        cursor = page[-1].dagster_run.run_id
    return records, False


def _job_p95(instance, job: str) -> float | None:
    try:
        successes = instance.get_run_records(
            filters=RunsFilter(statuses=[DagsterRunStatus.SUCCESS], job_name=job),
            limit=_BASELINE_SAMPLE,
        )
    except Exception:
        return None
    return p95_seconds(
        [
            r.end_time - r.start_time
            for r in successes
            if getattr(r, "start_time", None) and getattr(r, "end_time", None)
        ]
    )


@dg.sensor(
    name="alerting_hung_run_watchdog",
    minimum_interval_seconds=600,
    default_status=enabled_status(),
)
def alerting_hung_run_watchdog(context: dg.SensorEvaluationContext):
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    state = load_cursor(context.cursor)
    baselines = state.setdefault("baselines", {})  # job -> {"p95": float|None, "at": ts}
    state.setdefault("alerted", {})  # run_id -> ts
    namespace = current_namespace()

    started, complete = _fetch_started(context.instance)
    for record in started:
        run = record.dagster_run
        age = now - (getattr(record, "start_time", None) or record.create_timestamp.timestamp())
        baseline = baselines.get(run.job_name)
        if baseline is None or now - baseline["at"] > _BASELINE_TTL:
            baseline = {"p95": _job_p95(context.instance, run.job_name), "at": now}
            baselines[run.job_name] = baseline
        if not watchdog_should_flag(state, run.run_id, age, hung_threshold(baseline["p95"]), now):
            continue
        history = (
            f"; p95 for this job is {baseline['p95'] / 60:.0f}m"
            if baseline["p95"]
            else " (no duration history; 6h floor exceeded)"
        )
        user = (run.tags or {}).get("external-user")
        text = (
            f"🟡 *{env_prefix(namespace)}Hung run* — *{run.job_name}* run `{run.run_id[:8]}` "
            f"STARTED {age / 3600:.1f}h ago{history}" + (f" · user {user}" if user else "")
        )
        context.log.info(f"alerting watchdog verdict: {text}")
        if env_flag("ALERTING_WATCHDOG_POSTING", default=True):
            post_message(context.log, f"hung run: {run.job_name} {run.run_id[:8]}", [section(text)])

    if complete:  # an unscanned run has not necessarily finished
        live = {r.dagster_run.run_id for r in started}
        state["alerted"] = {k: v for k, v in state["alerted"].items() if k in live}
    context.update_cursor(json.dumps(state))


# ===== Tick failures and start timeouts =====

_TICK_LOOKBACK = 3600  # first tick (or a lost cursor) looks back this far
_TICK_OVERLAP = 300  # ticks that began before the last scan but failed after it
_START_TIMEOUT_STORM = 3  # failed-to-start runs per hour before a post


def _instigator_url(state, dagster_url: str) -> str:
    try:
        location = state.origin.repository_origin.code_location_origin.location_name
        kind = "sensors" if state.instigator_type == InstigatorType.SENSOR else "schedules"
        return f"{dagster_url}/locations/{location}/{kind}/{state.instigator_name}"
    except Exception:
        return f"{dagster_url}/automation"


def failing_instigators(instance, since: float) -> list[tuple]:
    """(state, failed ticks newest-first) for every running sensor/schedule that failed since."""
    out = []
    for state in instance.all_instigator_state():
        try:
            if not state.is_running or not state.selector_id:
                continue
            ticks = instance.get_ticks(
                state.instigator_origin_id,
                state.selector_id,
                after=since,
                limit=25,
                statuses=[TickStatus.FAILURE],
            )
        except Exception:
            continue
        if ticks:
            out.append((state, list(ticks)))
    return out


def start_timeout_storm(instance, now: float) -> list[str]:
    """Job names of runs that hit the run-start timeout in the last hour (empty below the storm size)."""
    since = datetime.datetime.fromtimestamp(now - 3600, datetime.timezone.utc)
    recent = instance.get_run_records(
        filters=RunsFilter(statuses=[DagsterRunStatus.FAILURE], updated_after=since), limit=200
    )
    if len(recent) < _START_TIMEOUT_STORM:
        return []
    stalled = []
    for record in recent[:100]:
        try:
            if classify(extract_failure_context(instance, record.dagster_run)).klass == "start-timeout":
                stalled.append(record.dagster_run.job_name)
        except Exception:
            continue
    return stalled if len(stalled) >= _START_TIMEOUT_STORM else []


@dg.sensor(
    name="alerting_tick_failure_watch",
    minimum_interval_seconds=600,
    default_status=enabled_status(),
)
def alerting_tick_failure_watch(context: dg.SensorEvaluationContext):
    """A sensor or schedule that errors every tick silently stops launching work; a run
    that never starts (image pull, scheduling, node capacity) never runs a log handler."""
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    state = load_cursor(context.cursor)
    alerted = state.setdefault("alerted", {})
    since = float(state.get("since") or (now - _TICK_LOOKBACK)) - _TICK_OVERLAP
    namespace = current_namespace()
    dagster_url = os.environ.get("ALERTING_DAGSTER_URL", "").strip()
    posting = env_flag("ALERTING_WATCHDOG_POSTING", default=True)

    for instigator, ticks in failing_instigators(context.instance, since):
        error = ticks[0].error
        cls = ((getattr(error, "cls_name", None) or "").rsplit(".", 1)[-1]) or "Error"
        first = ((getattr(error, "message", None) or "").strip().splitlines() or ["tick failed"])[0]
        key = f"{instigator.instigator_name}|{cls}"
        if now - alerted.get(key, 0.0) <= _REALERT:
            continue
        alerted[key] = now
        kind = instigator.instigator_type.value.lower().replace("_", "-")
        plural = "s" if len(ticks) != 1 else ""
        text = (
            f"🟠 *{env_prefix(namespace)}{kind.capitalize()} failing to evaluate* — "
            f"*{instigator.instigator_name}* · {len(ticks)} failed tick{plural} since the last scan\n"
            f"> {cls}: {first[:300]}"
        )
        context.log.info(f"alerting tick watch verdict: {text}")
        blocks = [section(text)]
        if dagster_url:
            blocks.append(actions_block([link_button("Open in Dagster", _instigator_url(instigator, dagster_url))]))
        if posting:
            post_message(context.log, f"{kind} failing: {instigator.instigator_name}", blocks)

    if now - alerted.get("start-timeout", 0.0) > _REALERT:
        stalled = start_timeout_storm(context.instance, now)
        if stalled:
            alerted["start-timeout"] = now
            top = ", ".join(f"`{job}` ×{n}" for job, n in Counter(stalled).most_common(3))
            text = (
                f"🟠 *{env_prefix(namespace)}Runs failing to start* — {len(stalled)} runs in the last hour "
                f"hit the run-start timeout before any step ran (image pull, scheduling or node capacity): {top}"
            )
            context.log.info(f"alerting tick watch verdict: {text}")
            blocks = [section(text)]
            if dagster_url:
                blocks.append(actions_block([link_button("Open runs in Dagster", f"{dagster_url}/runs")]))
            if posting:
                post_message(context.log, "runs failing to start", blocks)

    state["alerted"] = {k: v for k, v in alerted.items() if now - v <= 2 * _REALERT}
    state["since"] = now
    context.update_cursor(json.dumps(state))
