"""Error reporting to Slack, with Google Error Reporting as the system of record.

Every surface emits structured error entries (er_log_handler for Dagster,
er_logging for services and workers); Error Reporting groups and counts them;
the renderer sensor posts one rich Slack card per NEW group. A daily digest
classifies the day's Dagster failures against a measured taxonomy (every
unmatched shape fails open to "unknown") and adds a cross-surface ER summary;
a watchdog flags hung runs. See docs/ALERTING.md.

Everything is gated on ALERTING_ENABLED and runs in dry-run (payloads logged,
nothing posted) until ALERTING_SLACK_TOKEN_SECRET is configured.
"""

from .digest import alerting_daily_digest_job, alerting_daily_digest_schedule
from .renderer import alerting_er_group_renderer
from .sensors import alerting_failure_reporter, alerting_hung_run_watchdog


def alerting_definitions() -> dict:
    """Everything one code location needs to host the alerting machinery.

    Exactly one location per deployment should register these (they poll
    instance-wide state, so a second registrant would double-post). Merge
    into that location's Definitions:

        alerting = alerting_definitions()
        Definitions(
            jobs=[*your_jobs, *alerting["jobs"]],
            schedules=[*your_schedules, *alerting["schedules"]],
            sensors=[*your_sensors, *alerting["sensors"]],
        )

    The Error Reporting log handler (er_log_handler) is NOT part of this —
    it attaches instance-wide through the Helm chart's
    pythonLogs.dagsterHandlerConfig and needs no registration anywhere.
    """
    return {
        "jobs": [alerting_daily_digest_job],
        "schedules": [alerting_daily_digest_schedule],
        "sensors": [
            alerting_failure_reporter,
            alerting_hung_run_watchdog,
            alerting_er_group_renderer,
        ],
    }


__all__ = [
    "alerting_daily_digest_job",
    "alerting_daily_digest_schedule",
    "alerting_definitions",
    "alerting_er_group_renderer",
    "alerting_failure_reporter",
    "alerting_hung_run_watchdog",
]
