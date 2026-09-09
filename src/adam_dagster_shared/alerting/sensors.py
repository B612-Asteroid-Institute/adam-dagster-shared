"""Alerting sensors: the audit-only failure reporter and the hung-run watchdog.

The failure reporter classifies every newly failed run against the measured
taxonomy and logs one ALERTING_VERDICT JSON line per run — an audit trail and
the classifier's live regression feed. It never posts: channel ownership
belongs to the Error Reporting group renderer, and the digest re-derives its
numbers independently.

The watchdog flags runs stuck in STARTED far beyond their job's historical
p95 duration. Detection and verdict logging are unconditional; Slack posting
is gated separately (ALERTING_WATCHDOG_POSTING, defaulting to the shared
ALERTING_SENSOR_POSTING gate).
"""

from __future__ import annotations

import datetime
import json
import os

import dagster as dg
from dagster import DagsterRunStatus, RunsFilter, SensorEvaluationContext

from .classify import classify
from .extract import extract_failure_context
from .slack import current_namespace, post_message

_POLL_LIMIT = 25  # runs per tick; a 194-failure storm drains in ~8 ticks
_POLL_LIMIT_MAX = 200  # adaptive ceiling when a full page shares one timestamp
_POLL_OVERLAP_SECONDS = 1.0  # refetch window; Dagster's updated_after is strictly >
_RECENT_IDS_KEPT = 200  # processed-run dedup across the overlap window
_FIRST_TICK_LOOKBACK = 3600.0  # explicit adoption horizon, not epoch zero


def _posting_enabled() -> bool:
    return os.environ.get("ALERTING_SENSOR_POSTING", "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _watchdog_posting_enabled() -> bool:
    """Watchdog posting is decoupled from the (now audit-only) reporter gate."""
    value = os.environ.get("ALERTING_WATCHDOG_POSTING", "").strip().lower()
    if value:
        return value in ("1", "true", "yes")
    return _posting_enabled()


def _enabled_status() -> dg.DefaultSensorStatus:
    if os.environ.get("ALERTING_ENABLED", "").strip().lower() in ("1", "true", "yes"):
        return dg.DefaultSensorStatus.RUNNING
    return dg.DefaultSensorStatus.STOPPED


def load_state(cursor: str | None) -> dict:
    try:
        state = json.loads(cursor) if cursor else {}
    except (ValueError, TypeError):
        state = {}
    state.setdefault("after_ts", 0.0)
    state.setdefault("recent_ids", [])
    return state


def _fetch_failures(instance, updated_after: datetime.datetime) -> list:
    """Failure records after the cursor, growing the page on timestamp ties.

    Dagster pages have no stable continuation: with a fixed limit, a page
    full of runs sharing one update timestamp can never be advanced past
    (the filter is strictly greater-than). Grow the fetch until the page is
    not a single-timestamp full page, bounded by _POLL_LIMIT_MAX.
    """
    limit = _POLL_LIMIT
    while True:
        records = instance.get_run_records(
            filters=RunsFilter(statuses=[DagsterRunStatus.FAILURE], updated_after=updated_after),
            limit=limit,
            order_by="update_timestamp",
            ascending=True,
        )
        if len(records) < limit or limit >= _POLL_LIMIT_MAX:
            return records
        stamps = {r.update_timestamp.timestamp() for r in records}
        if len(stamps) > 1:
            return records
        limit = min(limit * 2, _POLL_LIMIT_MAX)


@dg.sensor(
    name="alerting_failure_reporter",
    minimum_interval_seconds=30,
    default_status=_enabled_status(),
)
def alerting_failure_reporter(context: SensorEvaluationContext):
    """Classify newly failed runs; log one ALERTING_VERDICT line per run."""
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    state = load_state(context.cursor)
    if not state["after_ts"]:
        state["after_ts"] = now - _FIRST_TICK_LOOKBACK

    records = _fetch_failures(
        context.instance,
        datetime.datetime.fromtimestamp(
            state["after_ts"] - _POLL_OVERLAP_SECONDS, tz=datetime.timezone.utc
        ),
    )

    max_ts = state["after_ts"]
    for record in records:
        run = record.dagster_run
        max_ts = max(max_ts, record.update_timestamp.timestamp())
        if run.run_id in state["recent_ids"]:
            continue
        state["recent_ids"].append(run.run_id)
        try:
            # Runs Dagster will retry are not terminal yet.
            if str((run.tags or {}).get("dagster/will_retry", "")).lower() == "true":
                continue
            ctx = extract_failure_context(context.instance, run)
            verdict = classify(ctx)
            context.log.info(
                "ALERTING_VERDICT "
                + json.dumps(
                    {
                        "run_id": run.run_id,
                        "job": ctx.job_name,
                        "class": verdict.klass,
                        "user_facing": verdict.user_facing,
                        "signature": verdict.signature,
                        "step": verdict.step_key,
                        "exception": (verdict.exception or "")[:300],
                    },
                    ensure_ascii=False,
                )
            )
        except Exception as exc:
            # One bad run must not block the rest; fail-open is the contract.
            context.log.warning(f"alerting: failed to process run {run.run_id}: {exc}")

    state["after_ts"] = max_ts
    state["recent_ids"] = state["recent_ids"][-_RECENT_IDS_KEPT:]
    context.update_cursor(json.dumps(state))


# ===== Hung-run watchdog =====

_WATCHDOG_FLOOR_SECONDS = 2 * 3600
_WATCHDOG_P95_FACTOR = 3.0
_WATCHDOG_BASELINE_TTL = 24 * 3600
_WATCHDOG_REALERT = 24 * 3600
_BASELINE_SAMPLE = 200  # recent successes per job for the p95 estimate
_WATCHDOG_PAGE = 200
_WATCHDOG_PAGE_CAP = 10  # at most 2000 STARTED runs scanned per tick


def watchdog_should_flag(state: dict, run_id: str, age: float, threshold: float, now: float) -> bool:
    """Record and return whether this run newly exceeds its hung threshold.

    Detection must be independent of Slack posting: with posting disabled the
    watchdog still has to mark and log its verdicts, or "verdict-only mode"
    silently becomes no-verdict mode (found live 2026-09-02: an 18h-stuck run
    went unrecorded because the posting gate sat inside this condition).
    """
    last_alert = state["alerted"].get(run_id, 0.0)
    if age <= threshold or now - last_alert <= _WATCHDOG_REALERT:
        return False
    state["alerted"][run_id] = now
    return True


def p95_seconds(durations: list[float]) -> float | None:
    cleaned = sorted(d for d in durations if d and d > 0)
    if not cleaned:
        return None
    return cleaned[min(len(cleaned) - 1, int(len(cleaned) * 0.95))]


def hung_threshold(p95: float | None) -> float:
    if p95 is None:
        return 6 * 3600  # no history: only flag runs stuck past 6h
    return max(_WATCHDOG_FLOOR_SECONDS, _WATCHDOG_P95_FACTOR * p95)


def _fetch_started(instance) -> tuple[list, bool]:
    """All STARTED runs, oldest first, paginated. Returns (records, complete).

    The newest-200 default sample can permanently miss the oldest hung run
    during a coordinator wave — exactly the workload the watchdog exists
    for. Oldest-first plus bounded pagination; a no-progress page (storage
    that ignores the cursor) terminates the scan as incomplete.
    """
    records: list = []
    seen: set[str] = set()
    cursor = None
    for _ in range(_WATCHDOG_PAGE_CAP):
        page = instance.get_run_records(
            filters=RunsFilter(statuses=[DagsterRunStatus.STARTED]),
            limit=_WATCHDOG_PAGE,
            ascending=True,
            cursor=cursor,
        )
        new = [r for r in page if r.dagster_run.run_id not in seen]
        if not new:
            return records, len(page) < _WATCHDOG_PAGE
        records.extend(new)
        seen.update(r.dagster_run.run_id for r in new)
        if len(page) < _WATCHDOG_PAGE:
            return records, True
        cursor = page[-1].dagster_run.run_id
    return records, False


@dg.sensor(
    name="alerting_hung_run_watchdog",
    minimum_interval_seconds=600,
    default_status=_enabled_status(),
)
def alerting_hung_run_watchdog(context: SensorEvaluationContext):
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    try:
        state = json.loads(context.cursor) if context.cursor else {}
    except (ValueError, TypeError):
        state = {}
    state.setdefault("baselines", {})  # job -> {"p95": float, "at": ts}
    state.setdefault("alerted", {})  # run_id -> ts
    namespace = current_namespace()

    started, complete = _fetch_started(context.instance)
    for record in started:
        run = record.dagster_run
        start_ts = getattr(record, "start_time", None) or record.create_timestamp.timestamp()
        age = now - start_ts
        job = run.job_name

        baseline = state["baselines"].get(job)
        if baseline is None or now - baseline["at"] > _WATCHDOG_BASELINE_TTL:
            try:
                successes = context.instance.get_run_records(
                    filters=RunsFilter(statuses=[DagsterRunStatus.SUCCESS], job_name=job),
                    limit=_BASELINE_SAMPLE,
                )
                durations = [
                    r.end_time - r.start_time
                    for r in successes
                    if getattr(r, "start_time", None) and getattr(r, "end_time", None)
                ]
            except Exception:
                durations = []
            baseline = {"p95": p95_seconds(durations), "at": now}
            state["baselines"][job] = baseline

        threshold = hung_threshold(baseline["p95"])
        if watchdog_should_flag(state, run.run_id, age, threshold, now):
            p95_min = (baseline["p95"] or 0) / 60
            user = (run.tags or {}).get("external-user")
            text = (
                f"🟡 *{'' if namespace == 'production' else f'[dev · {namespace}] '}Hung run* — "
                f"*{job}* run `{run.run_id[:8]}` STARTED "
                f"{age / 3600:.1f}h ago"
                + (f"; p95 for this job is {p95_min:.0f}m" if baseline["p95"] else
                   " (no duration history; 6h floor exceeded)")
                + (f" · user {user}" if user else "")
            )
            context.log.info(f"alerting watchdog verdict: {text}")
            if _watchdog_posting_enabled():
                post_message(
                    context.log,
                    f"hung run: {job} {run.run_id[:8]}",
                    [{"type": "section", "text": {"type": "mrkdwn", "text": text[:2900]}}],
                )

    # Prune alert marks only after a COMPLETE scan: an unscanned run has not
    # necessarily finished.
    if complete:
        live = {r.dagster_run.run_id for r in started}
        state["alerted"] = {k: v for k, v in state["alerted"].items() if k in live}
    context.update_cursor(json.dumps(state))
