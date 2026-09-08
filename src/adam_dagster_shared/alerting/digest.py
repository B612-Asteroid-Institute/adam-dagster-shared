"""Daily error digest: one message summarizing the last 24h of failures.

The digest re-derives its numbers by sweeping the runs table and classifying
each failure with the same pure rules the sensor uses — no state handoff, so
a sensor outage cannot corrupt the digest and vice versa. DIGEST- and
USER_FACING-tier events (which never post individually) get their airtime
here.
"""

from __future__ import annotations

import datetime
from collections import Counter

import dagster as dg
from dagster import DagsterRunStatus, RunsFilter

from .slack import current_namespace  # self-contained namespace lookup
from .classify import Tier, classify
from .extract import extract_failure_context
from .sensors import _enabled_status
from .slack import digest_blocks, post_message

_SWEEP_LIMIT = 1000  # > 5x the worst measured day (194 failures)
_TOP_SIGNATURES = 5


def build_digest_summary(instance, now: datetime.datetime) -> dict:
    since = now - datetime.timedelta(hours=24)
    failures = instance.get_run_records(
        filters=RunsFilter(statuses=[DagsterRunStatus.FAILURE], updated_after=since),
        limit=_SWEEP_LIMIT,
    )
    canceled = instance.get_run_records(
        filters=RunsFilter(statuses=[DagsterRunStatus.CANCELED], updated_after=since),
        limit=_SWEEP_LIMIT,
    )

    by_class: Counter[str] = Counter()
    sig_counts: Counter[str] = Counter()
    sig_labels: dict[str, str] = {}
    user_failures = 0
    users: set[str] = set()

    for record in failures:
        run = record.dagster_run
        try:
            ctx = extract_failure_context(instance, run)
            verdict = classify(ctx)
        except Exception:
            by_class["unclassifiable"] += 1
            continue
        by_class[verdict.klass] += 1
        sig_counts[verdict.signature] += 1
        sig_labels.setdefault(
            verdict.signature,
            f"{ctx.job_name}: {verdict.exception or verdict.reason}",
        )
        if verdict.tier is Tier.USER_FACING:
            user_failures += 1
            user = ctx.tags.get("external-user")
            if user:
                users.add(user)

    return {
        "date_label": now.strftime("%a %b %d"),
        "total_failures": len(failures),
        "canceled": len(canceled),
        "by_class": by_class.most_common(),
        "user_failures": user_failures,
        "user_count": len(users),
        "top_signatures": [
            (sig, count, sig_labels.get(sig, ""))
            for sig, count in sig_counts.most_common(_TOP_SIGNATURES)
        ],
    }


@dg.op
def post_daily_error_digest(context) -> None:
    # No type hint on `context`: dagster 1.13.5 identity-compares the raw
    # annotation object, and this module's `from __future__ import
    # annotations` turns any hint into a string it rejects.
    now = datetime.datetime.now(datetime.timezone.utc)
    namespace = current_namespace()
    summary = build_digest_summary(context.instance, now)
    # Cross-surface view: ER sees adam-api, cutout workers, and shards too.
    from .renderer import fetch_er_day_summary

    summary["er"] = fetch_er_day_summary(namespace)
    fallback, blocks = digest_blocks(summary, namespace)
    posted = post_message(context.log, fallback, blocks)
    context.log.info(f"alerting digest: {summary['total_failures']} failures, posted={posted}")
    if not posted:
        # A silently swallowed delivery error would make "the digest job
        # succeeded" a lie; a failed run is visible and retryable.
        raise RuntimeError("daily digest was not delivered to Slack (see warnings above)")


@dg.job(
    name="alerting_daily_digest_job",
    description="Posts the daily error digest to Slack.",
    tags={"dagster-k8s/config": {"container_config": {"resources": {"requests": {"cpu": "250m", "memory": "512Mi"}}}}},
)
def alerting_daily_digest_job():
    post_daily_error_digest()


alerting_daily_digest_schedule = dg.ScheduleDefinition(
    job=alerting_daily_digest_job,
    cron_schedule="0 16 * * *",  # 09:00 PT
    execution_timezone="UTC",
    default_status=(
        dg.DefaultScheduleStatus.RUNNING
        if _enabled_status() == dg.DefaultSensorStatus.RUNNING
        else dg.DefaultScheduleStatus.STOPPED
    ),
)
