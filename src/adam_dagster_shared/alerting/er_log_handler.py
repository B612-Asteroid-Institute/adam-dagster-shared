"""Logging handler that emits Error Reporting-compatible structured entries.

Attached instance-wide via the Helm chart's ``pythonLogs.dagsterHandlerConfig``
— additive alongside Dagster's normal console logging (the Dagster UI event
log and compute-log capture are separate machinery and untouched). At ERROR
level it emits ONE JSON line per record to stdout; Cloud Logging ingests that
line as a single structured entry, and Error Reporting evaluates it because
the ``@type`` marker forces recognition even for stackless messages.

Design constraints, each measured the hard way (see cloud_errors_2 worklog):

- **Grouping stability.** ER groups stack-ful errors by innermost exception
  type + top stack frames, and stackless ones by the FIRST 3 message tokens +
  the ``reportLocation`` function name. Both defaults are wrong here, each
  measured live: run-id-prefixed messages made every run its own group, and
  parsed stacks made every op error share the SAME group (the top frames are
  dagster's error boundary + client-library internals, identical across
  assets — a NotFound in mpc_obs_identity and one in
  unified_aims_source_mirror merged). So this handler deliberately keeps
  Dagster's ``Stack Trace:`` header UN-normalized — ER never parses it, the
  full stack stays in the message for humans — and constructs the grouping
  key itself: the headline leads with the innermost exception class
  (first-3-token key) and ``reportLocation.functionName`` carries the step
  key. Net grouping = (exception class, step), the same fingerprint
  semantics as the measured taxonomy. Run/job context goes in a trailing
  footer where grouping never looks.
- **Severity.** Dagster logs to stderr, which stamps every line ERROR at
  ingestion; this handler sets severity explicitly and only ships ERROR+.
- **serviceContext.** namespace/location + image version instead of the
  ``gke_instances``/pod-name defaults ER derives on its own.

No Google client libraries: the GKE agent forwards any JSON line on stdout as
a structured entry, so this stays dependency-free and can never block or slow
logging (worst case it prints nothing).
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys

_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
_REPORTED_ERROR_TYPE = (
    "type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent"
)

# Fallback prefix strip when dagster_meta is unavailable:
# "<job> - <36-char run id> - [<attempt> - ][<step> - ]<EVENT> - rest"
_PREFIX = re.compile(
    r"^\S+ - [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} - (?:\d+ - )?"
)


def _service_context() -> dict:
    namespace = os.environ.get("GARDEN_NAMESPACE", "").strip()
    if not namespace:
        try:
            with open(_NAMESPACE_PATH) as f:
                namespace = f.read().strip()
        except OSError:
            namespace = "unknown"
    location = os.environ.get("DAGSTER_LOCATION_NAME", "").strip() or "dagster"
    version = (
        os.environ.get("DAGSTER_IMAGE_VERSION", "").strip()
        or os.environ.get("GARDEN_ACTION_VERSION", "").strip()
        or "unknown"
    )
    return {"service": f"{namespace}/{location}", "version": version}


def _innermost_cls(error) -> str:
    """Innermost cls_name of a SerializableErrorInfo cause chain."""
    cls = ""
    node, hops = error, 0
    while node is not None and hops < 10:
        cls = getattr(node, "cls_name", None) or cls
        node = getattr(node, "cause", None)
        hops += 1
    # cls_name is dotted ("google.api_core.exceptions.NotFound") sometimes;
    # keep the terminal class name so the grouping token stays compact.
    return (cls or "Error").rsplit(".", 1)[-1]


class ErrorReportingHandler(logging.Handler):
    """Emit ERROR+ records as single-line ER-compatible JSON on stdout."""

    def __init__(self, level: int | str = logging.ERROR):
        super().__init__(level=level)
        self._service_context = _service_context()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            meta = getattr(record, "dagster_meta", None) or {}
            message = meta.get("orig_message") or _PREFIX.sub("", record.getMessage())

            # The real exception rides on the record's DagsterEvent, not in
            # orig_message: dagster's own console formatter appends
            # error_display_string the same way (log_manager._error_str_for_event).
            event = getattr(record, "dagster_event", None)
            error_text = ""
            exc_cls = ""
            if event is not None:
                data = getattr(event, "event_specific_data", None)
                error = getattr(data, "error", None) if data is not None else None
                if error is not None:
                    error_text = (
                        getattr(data, "error_display_string", None) or error.to_string()
                    )
                    exc_cls = _innermost_cls(error)
                elif getattr(event, "event_type_value", "") in (
                    "PIPELINE_FAILURE",
                    "RUN_FAILURE",
                ):
                    # A run-level failure with no error payload is the
                    # "Steps failed: [...]" summary — fully redundant with the
                    # STEP_FAILURE record already emitted; reporting it would
                    # double-count every failure into a junk catch-all group.
                    return
            if error_text:
                message = message + "\n\n" + error_text
            if record.exc_info and record.exc_info[0] is not None:
                import traceback

                message = message + "\n" + "".join(traceback.format_exception(*record.exc_info))
                exc_cls = exc_cls or record.exc_info[0].__name__
            # The grouping headline: first 3 tokens carry the exception class.
            step_for_headline = (
                (getattr(record, "dagster_meta", None) or {}).get("step_key") or ""
            )
            if exc_cls:
                message = f"{exc_cls} in {step_for_headline or 'run'}:\n{message}"

            run_id = meta.get("run_id") or ""
            step_key = meta.get("step_key") or ""
            job_name = meta.get("job_name") or meta.get("pipeline_name") or ""
            # Varying context lives in a footer: stack-ful grouping keys on the
            # parsed frames, stackless grouping on the first 3 tokens — a
            # trailing footer is invisible to both.
            footer_bits = [b for b in (
                f"run_id: {run_id}" if run_id else "",
                f"job: {job_name}" if job_name else "",
                f"step: {step_key}" if step_key else "",
                # The Slack renderer styles CRITICAL groups distinctly; ER's
                # event API exposes no severity, so it rides in the footer.
                "severity: critical" if record.levelno >= logging.CRITICAL else "",
            ) if b]
            if footer_bits:
                message = message + "\n-- " + " · ".join(footer_bits)

            entry = {
                "@type": _REPORTED_ERROR_TYPE,
                "severity": "ERROR" if record.levelno < logging.CRITICAL else "CRITICAL",
                "message": message,
                "serviceContext": self._service_context,
                # Required when no parsable stack is present; keys stackless
                # grouping (with the message's leading tokens) to the step.
                "context": {
                    "reportLocation": {
                        "filePath": job_name or record.name,
                        "lineNumber": 0,
                        "functionName": step_key or "run",
                    }
                },
                "logging.googleapis.com/labels": {
                    "logger": record.name,
                    "er_handler": "adam-alerting",
                    "dagster_run_id": run_id,
                },
            }
            sys.stdout.write(json.dumps(entry, ensure_ascii=False) + "\n")
            sys.stdout.flush()
        except Exception:  # noqa: BLE001 - a logging handler must never raise
            pass
