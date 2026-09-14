"""Daily digest: 24h of Dagster failures by class, plus a cross-surface Error Reporting summary."""

from __future__ import annotations

import datetime
from collections import Counter

import dagster as dg
from dagster import DagsterRunStatus, RunsFilter

from adam_dagster_shared.er_logging import current_namespace

from .classify import classify
from .extract import extract_failure_context
from .renderer import fetch_er_day_summary
from .sensors import enabled_status
from .slack import digest_blocks, post_message

_SWEEP_LIMIT = 1000  # > 5x the worst measured day (194 failures)


def build_digest_summary(instance, now: datetime.datetime) -> dict:
    since = now - datetime.timedelta(hours=24)

    def runs(status):
        return instance.get_run_records(
            filters=RunsFilter(statuses=[status], updated_after=since), limit=_SWEEP_LIMIT
        )

    failures = runs(DagsterRunStatus.FAILURE)
    by_class: Counter[str] = Counter()
    user_failures = 0
    users: set[str] = set()
    for record in failures:
        try:
            ctx = extract_failure_context(instance, record.dagster_run)
            verdict = classify(ctx)
        except Exception:
            by_class["unclassifiable"] += 1
            continue
        by_class[verdict.klass] += 1
        if verdict.user_facing:
            user_failures += 1
            users.add(ctx.tags.get("external-user", ""))
    return {
        "date_label": now.strftime("%a %b %d"),
        "total_failures": len(failures),
        "canceled": len(runs(DagsterRunStatus.CANCELED)),
        "by_class": by_class.most_common(),
        "user_failures": user_failures,
        "user_count": len(users - {""}),
    }


@dg.op
def post_daily_error_digest(context) -> None:
    # No type hint on `context`: dagster 1.13 rejects the stringified annotation
    # produced by `from __future__ import annotations`.
    namespace = current_namespace()
    summary = build_digest_summary(context.instance, datetime.datetime.now(datetime.timezone.utc))
    summary["er"] = fetch_er_day_summary(namespace)
    fallback, blocks = digest_blocks(summary, namespace)
    posted = post_message(context.log, fallback, blocks)
    context.log.info(f"alerting digest: {summary['total_failures']} failures, posted={posted}")
    if not posted:
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
        if enabled_status() == dg.DefaultSensorStatus.RUNNING
        else dg.DefaultScheduleStatus.STOPPED
    ),
)
