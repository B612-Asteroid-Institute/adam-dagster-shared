"""Hung-run watchdog: flags STARTED runs older than max(2h, 3×p95 of the job's history)."""

from __future__ import annotations

import datetime
import json

import dagster as dg
from dagster import DagsterRunStatus, RunsFilter

from adam_dagster_shared.er_logging import current_namespace, env_flag

from .slack import env_prefix, post_message, section

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
