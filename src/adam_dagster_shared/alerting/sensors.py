"""Alerting sensors: the failure reporter and the hung-run watchdog.

The failure reporter is a plain polling sensor (not a run_status_sensor)
because it needs a custom cursor: alongside the high-water mark it persists
per-signature cooldown state and the posting circuit breaker, which
run_status_sensor's managed cursor cannot carry. Polling uses
RunsFilter(updated_after=...) ordered by update_timestamp — the same
transition edge run_status_sensor consumes.

Anti-spam is layered (measured worst day: 194 failures):
- per-signature cooldown: at most one post per signature per 6h; repeats
  accumulate into the occurrence count shown on the next post/digest.
- cluster escalation: >= 5 occurrences of one NOTIFY-tier signature in 24h
  escalates that signature to PAGE (posted once per 24h).
- circuit breaker: at most 10 alert posts per rolling hour; overflow is
  verdict-logged and lands in the daily digest instead.

State keeping and decisions are pure functions over the cursor dict so they
unit-test without a Dagster instance.
"""

from __future__ import annotations

import datetime
import json
import os

import dagster as dg
from dagster import DagsterRunStatus, RunsFilter, SensorEvaluationContext

from .classify import Tier, classify
from .extract import extract_failure_context
from .slack import alert_blocks, current_namespace, post_message

_POLL_LIMIT = 25  # runs per tick; a 194-failure storm drains in ~8 ticks
_COOLDOWN_SECONDS = 6 * 3600
_ESCALATE_COUNT_24H = 5
_ESCALATE_WINDOW = 24 * 3600
_BREAKER_MAX_POSTS_PER_HOUR = 10
_STATE_PRUNE_AGE = 48 * 3600
_RECENT_IDS_KEPT = 50

# Job types whose failures are owned by the adam-jobs -> adam-api path (they
# also carry external-user tags, but belt and braces).
_USER_JOB_NAMES = {
    "run_transfer_trajectory_job",
    "run_precovery_v2_sharded_job",
    "run_ephemeris_job",
    "run_parameterized_precovery",
    "run_parameterized_impact_simulation",
}


def _posting_enabled() -> bool:
    """When false, the failure reporter and watchdog log verdicts but never
    post — the ER-native notification + group renderer own the channel."""
    return os.environ.get("ALERTING_SENSOR_POSTING", "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )


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
    state.setdefault("signatures", {})  # fp -> {first, last, count_24h_start, count, posted, paged}
    state.setdefault("breaker", {"hour_start": 0.0, "posts": 0})
    return state


def prune_state(state: dict, now: float) -> None:
    """Drop signature entries idle past the prune age so the cursor stays small."""
    stale = [
        fp for fp, s in state["signatures"].items() if now - s.get("last", 0.0) > _STATE_PRUNE_AGE
    ]
    for fp in stale:
        del state["signatures"][fp]
    state["recent_ids"] = state["recent_ids"][-_RECENT_IDS_KEPT:]


def record_occurrence(state: dict, fp: str, now: float) -> dict:
    """Update per-signature counters; returns the signature's state dict."""
    sig = state["signatures"].setdefault(
        fp, {"first": now, "last": now, "win_start": now, "count": 0, "posted": 0.0, "paged": 0.0}
    )
    if now - sig["win_start"] > _ESCALATE_WINDOW:
        sig["win_start"], sig["count"] = now, 0
    sig["count"] += 1
    sig["last"] = now
    return sig


def decide_post(state: dict, sig: dict, tier: Tier, klass: str, now: float) -> tuple[bool, bool]:
    """(should_post, escalated) under cooldown, escalation, and breaker rules."""
    escalated = (
        tier is Tier.NOTIFY
        and klass in ("code-error", "unknown", "run-failure")
        and sig["count"] >= _ESCALATE_COUNT_24H
        and now - sig["paged"] > _ESCALATE_WINDOW
    )
    if escalated:
        should = True
    elif tier is Tier.PAGE:
        should = now - sig["paged"] > _COOLDOWN_SECONDS
    elif tier is Tier.NOTIFY:
        should = now - sig["posted"] > _COOLDOWN_SECONDS
    else:  # DIGEST / USER_FACING never post individually
        return False, False

    if not should:
        return False, False

    breaker = state["breaker"]
    if now - breaker["hour_start"] > 3600:
        breaker["hour_start"], breaker["posts"] = now, 0
    if breaker["posts"] >= _BREAKER_MAX_POSTS_PER_HOUR:
        return False, escalated  # overflow: verdict-logged, lands in digest
    breaker["posts"] += 1
    if escalated or tier is Tier.PAGE:
        sig["paged"] = now
    sig["posted"] = now
    return True, escalated


@dg.sensor(
    name="alerting_failure_reporter",
    minimum_interval_seconds=30,
    default_status=_enabled_status(),
)
def alerting_failure_reporter(context: SensorEvaluationContext):
    """Classify newly failed runs and post severity-routed Slack alerts."""
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    state = load_state(context.cursor)
    namespace = current_namespace()

    records = context.instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.FAILURE],
            updated_after=datetime.datetime.fromtimestamp(
                state["after_ts"], tz=datetime.timezone.utc
            ),
        ),
        limit=_POLL_LIMIT,
        order_by="update_timestamp",
        ascending=True,
    )

    max_ts = state["after_ts"]
    for record in records:
        run = record.dagster_run
        update_ts = record.update_timestamp.timestamp()
        max_ts = max(max_ts, update_ts)
        if run.run_id in state["recent_ids"]:
            continue
        state["recent_ids"].append(run.run_id)
        try:
            # Runs Dagster will retry are not terminal yet — same guard the
            # adam-jobs sensor uses.
            if str((run.tags or {}).get("dagster/will_retry", "")).lower() == "true":
                continue
            ctx = extract_failure_context(context.instance, run)
            verdict = classify(ctx)
            if verdict.tier is Tier.USER_FACING or run.job_name in _USER_JOB_NAMES:
                verdict.tier = Tier.USER_FACING
            sig = record_occurrence(state, verdict.signature, now)
            context.log.info(
                "ALERTING_VERDICT "
                + json.dumps(
                    {
                        "run_id": run.run_id,
                        "job": ctx.job_name,
                        "tier": verdict.tier.value,
                        "class": verdict.klass,
                        "signature": verdict.signature,
                        "count_24h": sig["count"],
                        "step": verdict.step_key,
                        "exception": (verdict.exception or "")[:300],
                    },
                    ensure_ascii=False,
                )
            )
            should_post, escalated = decide_post(state, sig, verdict.tier, verdict.klass, now)
            if should_post and _posting_enabled():
                fallback, blocks = alert_blocks(
                    verdict,
                    job_name=ctx.job_name,
                    run_id=run.run_id,
                    tags=ctx.tags,
                    namespace=namespace,
                    occurrences_24h=sig["count"],
                    first_seen_ts=sig["first"],
                    escalated=escalated,
                    webserver_url=os.environ.get("ALERTING_DAGSTER_URL", "").strip() or None,
                )
                post_message(context.log, fallback, blocks)
        except Exception as exc:
            # One bad run must not block the rest; fail-open is the contract.
            context.log.warning(f"alerting: failed to process run {run.run_id}: {exc}")

    state["after_ts"] = max_ts
    prune_state(state, now)
    context.update_cursor(json.dumps(state))


# ===== Hung-run watchdog =====

_WATCHDOG_FLOOR_SECONDS = 2 * 3600
_WATCHDOG_P95_FACTOR = 3.0
_WATCHDOG_BASELINE_TTL = 24 * 3600
_WATCHDOG_REALERT = 24 * 3600
_BASELINE_SAMPLE = 200  # recent successes per job for the p95 estimate


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
    if not durations:
        return None
    ordered = sorted(durations)
    return ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))]


def hung_threshold(p95: float | None) -> float:
    """The Aug-17 hung precovery runs sat ~70x their 63-min p95 for 3 days;
    3x p95 (floored at 2h for thin history) would have fired within hours."""
    if p95 is None:
        return _WATCHDOG_FLOOR_SECONDS * 3
    return max(_WATCHDOG_P95_FACTOR * p95, _WATCHDOG_FLOOR_SECONDS)


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

    started = context.instance.get_run_records(
        filters=RunsFilter(statuses=[DagsterRunStatus.STARTED]), limit=200
    )
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
                f"🟡 *{'' if namespace == 'production' else f'[dev · {namespace}] '}NOTIFY* — "
                f"run stuck: *{job}* run `{run.run_id[:8]}` STARTED "
                f"{age / 3600:.1f}h ago"
                + (f"; p95 for this job is {p95_min:.0f}m" if baseline["p95"] else
                   " (no duration history; 6h floor exceeded)")
                + (f" · user {user}" if user else "")
            )
            context.log.info(f"alerting watchdog verdict: {text}")
            if _posting_enabled():
                post_message(
                    context.log,
                    f"hung run: {job} {run.run_id[:8]}",
                    [{"type": "section", "text": {"type": "mrkdwn", "text": text[:2900]}}],
                )

    # Prune alert marks for runs no longer STARTED.
    live = {r.dagster_run.run_id for r in started}
    state["alerted"] = {k: v for k, v in state["alerted"].items() if k in live}
    context.update_cursor(json.dumps(state))
