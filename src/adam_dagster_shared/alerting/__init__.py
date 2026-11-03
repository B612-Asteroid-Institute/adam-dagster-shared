"""Slack error reporting with Google Error Reporting as the system of record.

Handlers (er_log_handler for Dagster, er_logging for services/workers) emit structured
entries; ER groups them; the renderer posts one Slack card per NEW group; a daily
digest and a hung-run watchdog cover the rest. Gated on ALERTING_ENABLED; dry-run
until ALERTING_SLACK_TOKEN_SECRET is set. See docs/ALERTING.md.
"""

from .digest import alerting_daily_digest_job, alerting_daily_digest_schedule
from .renderer import alerting_er_group_renderer
from .sensors import alerting_hung_run_watchdog


def alerting_definitions() -> dict:
    """Jobs/schedules/sensors for the ONE code location that hosts alerting.

    They poll instance-wide state, so a second registrant would double-post.
    """
    return {
        "jobs": [alerting_daily_digest_job],
        "schedules": [alerting_daily_digest_schedule],
        "sensors": [alerting_hung_run_watchdog, alerting_er_group_renderer],
    }
