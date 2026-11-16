"""Daemon-side watches: hung runs, failing sensor/schedule ticks, runs that never start.

Everything here happens outside run pods, where no log handler can see it.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
from collections import Counter, defaultdict

import dagster as dg
from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    RunsFilter,
    RunStatusChangeRecordsFilter,
)
from dagster._core.definitions.run_request import InstigatorType
from dagster._core.scheduler.instigation import TickStatus

from adam_dagster_shared.er_logging import current_namespace, env_flag

from .classify import FailureContext, classify
from .extract import run_failure_of
from .slack import actions_block, env_prefix, link_button, post_message, section

_FLOOR_SECONDS = 2 * 3600
_NO_HISTORY_SECONDS = 6 * 3600
_P95_FACTOR = 3.0
_BASELINE_TTL = 24 * 3600
_REALERT = 24 * 3600
_BASELINE_SAMPLE = 500  # recent successes per job
_SELECTION_MIN_SAMPLES = 5  # below this, fall back to the job-wide p95
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


def selection_key(run) -> str:
    """Asset-selection fingerprint: __ASSET_JOB hosts every automation run, whose durations
    range from seconds (a risk-list fetch) to hours (a single-run backfill)."""
    selection = getattr(run, "asset_selection", None)
    if not selection:
        return ""
    names = sorted(getattr(k, "to_user_string", lambda: str(k))() for k in selection)
    return hashlib.sha1("|".join(names).encode()).hexdigest()[:12]


def _job_baselines(instance, job: str) -> dict:
    """{"p95": job-wide p95 | None, "by_sel": {selection key: p95}} from recent successes."""
    try:
        successes = instance.get_run_records(
            filters=RunsFilter(statuses=[DagsterRunStatus.SUCCESS], job_name=job),
            limit=_BASELINE_SAMPLE,
        )
    except Exception:
        return {"p95": None, "by_sel": {}}
    durations: list[float] = []
    by_sel: dict[str, list[float]] = defaultdict(list)
    for r in successes:
        if getattr(r, "start_time", None) and getattr(r, "end_time", None):
            durations.append(r.end_time - r.start_time)
            by_sel[selection_key(r.dagster_run)].append(r.end_time - r.start_time)
    return {
        "p95": p95_seconds(durations),
        "by_sel": {k: p95_seconds(v) for k, v in by_sel.items() if k and len(v) >= _SELECTION_MIN_SAMPLES},
    }


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
        if "dagster/backfill" in (run.tags or {}):
            continue  # human-driven, watched, and hours long by design
        age = now - (getattr(record, "start_time", None) or record.create_timestamp.timestamp())
        baseline = baselines.get(run.job_name)
        if baseline is None or now - baseline["at"] > _BASELINE_TTL or "by_sel" not in baseline:
            baseline = {**_job_baselines(context.instance, run.job_name), "at": now}
            baselines[run.job_name] = baseline
        p95 = baseline["by_sel"].get(selection_key(run)) or baseline["p95"]
        if age <= hung_threshold(p95) or now - state["alerted"].get(run.run_id, 0.0) <= _REALERT:
            continue
        history = (
            f"; p95 for this job/selection is {p95 / 60:.0f}m"
            if p95
            else " (no duration history; 6h floor exceeded)"
        )
        user = (run.tags or {}).get("external-user")
        text = (
            f"🟡 *{env_prefix(namespace)}Hung run* — *{run.job_name}* run `{run.run_id[:8]}` "
            f"STARTED {age / 3600:.1f}h ago{history}" + (f" · user {user}" if user else "")
        )
        context.log.info(f"alerting watchdog verdict: {text}")
        # Suppress for a day only once Slack accepted the post (or posting is off).
        if not env_flag("ALERTING_WATCHDOG_POSTING", default=True) or post_message(
            context.log, f"hung run: {run.job_name} {run.run_id[:8]}", [section(text)]
        ):
            state["alerted"][run.run_id] = now

    if complete:  # an unscanned run has not necessarily finished
        live = {r.dagster_run.run_id for r in started}
        state["alerted"] = {k: v for k, v in state["alerted"].items() if k in live}
    context.update_cursor(json.dumps(state))


# ===== Tick failures and daemon-side run failures =====

_TICK_LOOKBACK = 3600  # first tick (or a lost cursor) looks back this far
_TICK_OVERLAP = 300  # ticks that began before the last scan but failed after it
_START_TIMEOUT_STORM = 3  # failed-to-start runs per hour before a post
_FAILURE_SCAN_LIMIT = 500  # RUN_FAILURE events examined per tick (one event-log query)
_LOST_RUNS_LISTED = 3  # run ids quoted in a card


def _instigator_url(state, dagster_url: str) -> str:
    try:
        location = state.origin.repository_origin.code_location_origin.location_name
        kind = "sensors" if state.instigator_type == InstigatorType.SENSOR else "schedules"
        return f"{dagster_url}/locations/{location}/{kind}/{state.instigator_name}"
    except Exception:
        return f"{dagster_url}/automation"


def innermost_error(error) -> tuple[str, str]:
    """(class, first message line) of the deepest cause: the daemon wraps a sensor's exception
    in SensorExecutionError / ScheduleExecutionError, which says nothing about the fault."""
    node, cls, message = error, "", ""
    for _ in range(10):
        if node is None:
            break
        cls = (getattr(node, "cls_name", None) or "").rsplit(".", 1)[-1] or cls
        message = (getattr(node, "message", None) or "").strip() or message
        node = getattr(node, "cause", None)
    first = (message.splitlines() or ["tick failed"])[0]
    prefix = f"{cls}: "
    return cls or "Error", first[len(prefix):] if first.startswith(prefix) else first


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


def unbridged_run_failures(instance, since: float) -> tuple[list[dict], bool]:
    """RUN_FAILURE events the daemon stored directly, newest first: run-start timeouts, run
    workers lost after every resume attempt, max-runtime kills, launch failures. The daemon
    logs them at INFO and they never pass a log handler, so this is their only path to a card.
    Excluded: the run worker's "Steps failed: [...]" summary and failures that carry an
    exception, which the Dagster-side handler already reported. Returns (failures, complete)."""
    result = instance.fetch_run_status_changes(
        RunStatusChangeRecordsFilter(
            event_type=DagsterEventType.RUN_FAILURE, after_timestamp=float(since)
        ),
        limit=_FAILURE_SCAN_LIMIT,
    )
    out: list[dict] = []
    for record in result.records:
        try:
            entry = record.event_log_entry
            evt = entry.dagster_event
            data = evt.event_specific_data
            if getattr(data, "error", None) is not None:
                continue
            if getattr(getattr(data, "failure_reason", None), "value", None) == "STEP_FAILURE":
                continue
            run_failure = run_failure_of(evt) or (None, "")
            job = entry.job_name or evt.job_name or ""
            ctx = FailureContext(run_id=entry.run_id, job_name=job, tags={}, run_failure=run_failure)
            out.append(
                {
                    "run_id": entry.run_id,
                    "job": job,
                    "klass": classify(ctx).klass,
                    "message": (run_failure[1].splitlines() or [""])[0],
                    "ts": entry.timestamp,
                }
            )
        except Exception:
            continue
    return out, not result.has_more


@dg.sensor(
    name="alerting_tick_failure_watch",
    minimum_interval_seconds=600,
    default_status=enabled_status(),
)
def alerting_tick_failure_watch(context: dg.SensorEvaluationContext):
    """What happens in the daemon, where no run-pod log handler can see it: a sensor or
    schedule that errors every tick silently stops launching work; a run that never starts
    (image pull, scheduling, node capacity) or loses its run worker never runs a handler."""
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    state = load_cursor(context.cursor)
    alerted = state.setdefault("alerted", {})
    last_scan = float(state.get("since") or (now - _TICK_LOOKBACK))
    since = last_scan - _TICK_OVERLAP
    namespace = current_namespace()
    dagster_url = os.environ.get("ALERTING_DAGSTER_URL", "").strip()
    posting = env_flag("ALERTING_WATCHDOG_POSTING", default=True)
    rejected = 0

    def deliver(keys: list[str], fallback: str, text: str, button: tuple[str, str]) -> None:
        """Log the verdict and post it; suppress re-alerts only once Slack accepted the post
        (or posting is off). A rejected post is retried next tick, not silenced for a day."""
        nonlocal rejected
        context.log.info(f"alerting tick watch verdict: {text}")
        blocks = [section(text)]
        if dagster_url:
            blocks.append(actions_block([link_button(button[0], button[1])]))
        if posting and not post_message(context.log, fallback, blocks):
            rejected += 1
            return
        for key in keys:
            alerted[key] = now

    for instigator, ticks in failing_instigators(context.instance, since):
        cls, first = innermost_error(ticks[0].error)
        key = f"{instigator.instigator_name}|{cls}"
        if now - alerted.get(key, 0.0) <= _REALERT:
            continue
        kind = instigator.instigator_type.value.lower().replace("_", "-")
        plural = "s" if len(ticks) != 1 else ""
        text = (
            f"🟠 *{env_prefix(namespace)}{kind.capitalize()} failing to evaluate* — "
            f"*{instigator.instigator_name}* · {len(ticks)} failed tick{plural} since the last scan\n"
            f"> {cls}: {first[:300]}"
        )
        deliver(
            [key],
            f"{kind} failing: {instigator.instigator_name}",
            text,
            ("Open in Dagster", _instigator_url(instigator, dagster_url)),
        )

    failures, complete = unbridged_run_failures(context.instance, now - 3600)
    if not complete:
        context.log.warning(
            f"alerting tick watch: over {_FAILURE_SCAN_LIMIT} daemon-side run failures in the "
            "last hour; only the newest were scanned"
        )

    stalled = [f["job"] for f in failures if f["klass"] == "start-timeout"]
    if len(stalled) >= _START_TIMEOUT_STORM and now - alerted.get("start-timeout", 0.0) > _REALERT:
        top = ", ".join(f"`{job}` ×{n}" for job, n in Counter(stalled).most_common(3))
        text = (
            f"🟠 *{env_prefix(namespace)}Runs failing to start* — {len(stalled)} runs in the last hour "
            f"hit the run-start timeout before any step ran (image pull, scheduling or node capacity): {top}"
        )
        deliver(["start-timeout"], "runs failing to start", text, ("Open runs in Dagster", f"{dagster_url}/runs"))

    lost = [
        f
        for f in failures
        if f["klass"] != "start-timeout"
        and f["ts"] > since
        and now - alerted.get(f"run|{f['run_id']}", 0.0) > _REALERT
    ]
    if lost:
        top = ", ".join(f"`{job}` ×{n}" for job, n in Counter(f["job"] for f in lost).most_common(3))
        ids = ", ".join(f"`{f['run_id'][:8]}`" for f in lost[:_LOST_RUNS_LISTED])
        plural = "s" if len(lost) != 1 else ""
        text = (
            f"🟠 *{env_prefix(namespace)}Run{plural} failed outside the run worker* — {len(lost)} run{plural} "
            "since the last scan (worker lost after its resume attempts, max runtime exceeded, or launch "
            f"failed; no step reported it): {top} · {ids}\n> {lost[0]['message'][:300]}"
        )
        deliver(
            [f"run|{f['run_id']}" for f in lost],
            "runs failed outside the run worker",
            text,
            ("Open runs in Dagster", f"{dagster_url}/runs"),
        )

    state["alerted"] = {k: v for k, v in alerted.items() if now - v <= 2 * _REALERT}
    state["since"] = last_scan if rejected else now  # a rejected post keeps the window open
    context.update_cursor(json.dumps(state))
