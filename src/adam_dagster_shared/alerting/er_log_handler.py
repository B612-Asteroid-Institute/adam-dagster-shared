"""Dagster-side Error Reporting handler.

Attached instance-wide through the Helm chart's ``pythonLogs.dagsterHandlerConfig``;
emits one ER JSON line per ERROR+ record. The headline "<ExceptionClass> in <step>"
plus reportLocation.functionName=<step> makes ER group by (exception class, step);
Dagster's own "Stack Trace:" block is left as-is because ER does not parse it.
See docs/ALERTING.md.
"""

from __future__ import annotations

import logging
import os
import re
import traceback

from adam_dagster_shared.er_logging import (
    current_namespace,
    footer,
    neutralize_stack,
    write_entry,
)

# "<job> - <run id> - [<attempt> - ]" prefix, stripped when dagster_meta is missing.
_PREFIX = re.compile(
    r"^\S+ - [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} - (?:\d+ - )?"
)


def _service_context() -> dict:
    location = os.environ.get("DAGSTER_LOCATION_NAME", "").strip() or "dagster"
    version = (
        os.environ.get("DAGSTER_IMAGE_VERSION", "").strip()
        or os.environ.get("GARDEN_ACTION_VERSION", "").strip()
        or "unknown"
    )
    return {"service": f"{current_namespace()}/{location}", "version": version}


def _innermost_cls(error) -> str:
    """Terminal class name of the innermost SerializableErrorInfo cause."""
    cls, node = "", error
    for _ in range(10):
        if node is None:
            break
        cls = getattr(node, "cls_name", None) or cls
        node = getattr(node, "cause", None)
    return (cls or "Error").rsplit(".", 1)[-1]


class ErrorReportingHandler(logging.Handler):
    def __init__(self, level: int | str = logging.ERROR):
        super().__init__(level=level)
        self._service_context = _service_context()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            meta = getattr(record, "dagster_meta", None) or {}
            message = meta.get("orig_message") or _PREFIX.sub("", record.getMessage())
            exc_cls = ""
            # The exception rides on the record's DagsterEvent, not in the message.
            event = getattr(record, "dagster_event", None)
            data = getattr(event, "event_specific_data", None)
            error = getattr(data, "error", None)
            if error is not None:
                message += "\n\n" + (getattr(data, "error_display_string", None) or error.to_string())
                exc_cls = _innermost_cls(error)
            elif getattr(event, "event_type_value", "") in ("PIPELINE_FAILURE", "RUN_FAILURE"):
                return  # "Steps failed: [...]" summary; the STEP_FAILURE record already reported
            if record.exc_info and record.exc_info[0] is not None:
                message += "\n" + neutralize_stack(
                    "".join(traceback.format_exception(*record.exc_info))
                )
                exc_cls = exc_cls or record.exc_info[0].__name__

            run_id = meta.get("run_id") or ""
            step_key = meta.get("step_key") or ""
            job_name = meta.get("job_name") or meta.get("pipeline_name") or ""
            if exc_cls:
                message = f"{exc_cls} in {step_key or 'run'}:\n{message}"
            critical = record.levelno >= logging.CRITICAL
            message += footer(
                f"run_id: {run_id}" if run_id else "",
                f"job: {job_name}" if job_name else "",
                f"step: {step_key}" if step_key else "",
                "severity: critical" if critical else "",
            )
            write_entry(
                message,
                critical,
                self._service_context,
                file_path=job_name or record.name,
                function_name=step_key or "run",
                labels={"logger": record.name, "dagster_run_id": run_id},
            )
        except Exception:  # noqa: BLE001 - a logging handler must never raise
            pass
