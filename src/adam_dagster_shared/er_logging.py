"""Google Error Reporting bridge for non-Dagster services and workers.

One implementation for every plain-Python surface (adam-api's Django/ninja
processes, the cutout workers, the precovery-v2 shard entrypoint): a root
logging handler that emits ONE single-line, ER-compatible JSON entry to
stdout per ERROR+ record, plus a chained sys.excepthook so exceptions that
escape main() report as CRITICAL before the process dies. The GKE logging
agent ingests each line as a single structured entry; the ``@type`` marker
forces Error Reporting to evaluate it. (Dagster processes use
``alerting.er_log_handler`` instead — attached instance-wide via the Helm
chart's pythonLogs, no per-job or per-asset wiring anywhere.)

Usage, once per process, right after ``logging.basicConfig``::

    from adam_dagster_shared.er_logging import maybe_install
    maybe_install()

Inert unless ER_LOGGING_ENABLED is set. Identity comes from env:
ER_SERVICE_NAME (the service label), ER_SERVICE_VERSION (image/build tag),
GARDEN_NAMESPACE or the mounted serviceaccount namespace.

Grouping (each rule measured live during the 2026-09 preview validation):

- ER groups stackless entries by the first 3 message tokens +
  reportLocation.functionName, and switches to frame-based grouping when it
  recognizes a Python traceback — which would merge unrelated failures
  through shared framework frames (django-ninja's operation runner, the
  worker loop). The traceback header is therefore rewritten
  (``Stack trace`` instead of ``Traceback``) and the handler owns the key:
  - records carrying request context group as
    ``(exception class, METHOD /route/{id})`` with per-request ids collapsed;
  - all other records group as ``(exception class, logger name)``.
- ``django.request`` records without exc_info are Django's response-status
  echoes ("Internal Server Error: /path" fired for every 5xx response) — a
  duplicate of the exception already reported by the application handler;
  skipped.
- ``django.security.*`` (DisallowedHost etc.) fires on attacker-supplied
  input: grouping by the probed path would mint one ER group per scanner
  URL, so those collapse to one group per security class.
- Request specifics ride in a trailing ``-- `` footer (invisible to both
  grouping modes) plus ``context.httpRequest``; CRITICAL severity rides in
  the footer too, because ER's event API exposes no severity and the Slack
  renderer styles CRITICALs distinctly.

Dependency-free and can never raise: worst case it prints nothing.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import traceback

_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
_REPORTED_ERROR_TYPE = (
    "type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent"
)

# Path segments that vary per request but not per bug: UUIDs, hex ids, and
# plain integers collapse to {id} so grouping keys on the route, not the row.
_UUID_SEG = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_HEX_SEG = re.compile(r"^[0-9a-fA-F]{16,}$")
_INT_SEG = re.compile(r"^\d+$")


def _enabled() -> bool:
    return os.environ.get("ER_LOGGING_ENABLED", "").strip().lower() in ("1", "true", "yes")


def _service_context() -> dict:
    namespace = os.environ.get("GARDEN_NAMESPACE", "").strip()
    if not namespace:
        try:
            with open(_NAMESPACE_PATH) as f:
                namespace = f.read().strip()
        except OSError:
            namespace = "local"
    service = os.environ.get("ER_SERVICE_NAME", "").strip() or "service"
    version = (
        os.environ.get("ER_SERVICE_VERSION", "").strip()
        or os.environ.get("GARDEN_ACTION_VERSION", "").strip()
        or "unknown"
    )
    return {"service": f"{namespace}/{service}", "version": version}


def normalize_path(path: str) -> str:
    """Collapse per-request id segments so one route = one grouping key."""
    parts = []
    for seg in path.split("/"):
        if _UUID_SEG.match(seg) or _HEX_SEG.match(seg) or _INT_SEG.match(seg):
            parts.append("{id}")
        else:
            parts.append(seg)
    return "/".join(parts) or "/"


def _stack_text(exc_info) -> str:
    """Traceback with the header ER's parser keys on rewritten (see module doc)."""
    text = "".join(traceback.format_exception(*exc_info))
    return text.replace(
        "Traceback (most recent call last):", "Stack trace (most recent call last):", 1
    )


def _request_bits(record: logging.LogRecord) -> dict:
    """Best-effort request context from the record, never touching a database.

    Sources, in order: explicit ``extra`` fields an application's exception
    handler may set (http_method/http_path/http_status), then the WSGI/ASGI
    request object ``django.request`` records carry. User identity is read
    only from an already-resolved user object (never through a lazy proxy,
    which could trigger a database query from inside a logging handler).
    """
    bits: dict = {}
    method = getattr(record, "http_method", None)
    path = getattr(record, "http_path", None)
    status = getattr(record, "http_status", None) or getattr(record, "status_code", None)
    request = getattr(record, "request", None)
    if request is not None:
        try:
            method = method or getattr(request, "method", None)
            path = path or getattr(request, "path", None)
            meta = getattr(request, "META", None) or {}
            bits["remote_ip"] = meta.get("HTTP_X_FORWARDED_FOR", "").split(",")[
                0
            ].strip() or meta.get("REMOTE_ADDR", "")
            bits["user_agent"] = meta.get("HTTP_USER_AGENT", "")
            user = request.__dict__.get("user")
            if user is not None and user.__class__.__name__ != "SimpleLazyObject":
                if getattr(user, "is_authenticated", False):
                    bits["user"] = getattr(user, "email", "") or getattr(user, "username", "")
        except Exception:
            pass
    if method:
        bits["method"] = str(method)
    if path:
        bits["path"] = str(path)
    if status:
        bits["status"] = str(status)
    return bits


class ErrorReportingHandler(logging.Handler):
    """Emit ERROR+ records as single-line ER-compatible JSON on stdout."""

    def __init__(self, level: int | str = logging.ERROR):
        super().__init__(level=level)
        self._service_context = _service_context()
        self._enabled = _enabled()

    def emit(self, record: logging.LogRecord) -> None:
        if not self._enabled:
            return
        try:
            # django.request without exc_info is Django's response-status echo
            # — the exception itself was already reported (see module doc).
            if record.name == "django.request" and not (
                record.exc_info and record.exc_info[0] is not None
            ):
                return
            message = record.getMessage()
            exc_cls = ""
            if record.exc_info and record.exc_info[0] is not None:
                exc_cls = record.exc_info[0].__name__
                message = message + "\n" + _stack_text(record.exc_info)

            req = _request_bits(record)
            route = ""
            if req.get("method") and req.get("path"):
                route = f"{req['method']} {normalize_path(req['path'])}"
            # django.security.* fires on attacker-supplied input: never group
            # by the probed path (see module doc).
            if record.name.startswith("django.security"):
                route = ""

            # The grouping headline: first 3 tokens carry the exception class.
            if exc_cls:
                message = (
                    f"{exc_cls} at {route}:\n{message}"
                    if route
                    else f"{exc_cls} in {record.name}:\n{message}"
                )

            footer_bits = [
                b
                for b in (
                    (
                        f"request: {req['method']} {req['path']}"
                        if req.get("method") and req.get("path")
                        else ""
                    ),
                    f"status: {req['status']}" if req.get("status") else "",
                    f"user: {req['user']}" if req.get("user") else "",
                    f"client: {req['remote_ip']}" if req.get("remote_ip") else "",
                    # The Slack renderer styles CRITICAL groups distinctly;
                    # ER's event API exposes no severity.
                    "severity: critical" if record.levelno >= logging.CRITICAL else "",
                )
                if b
            ]
            if footer_bits:
                message = message + "\n-- " + " · ".join(footer_bits)

            context: dict = {
                "reportLocation": {
                    "filePath": self._service_context["service"].split("/", 1)[-1],
                    "lineNumber": 0,
                    "functionName": route or record.name,
                }
            }
            if req.get("method"):
                http_request = {"method": req["method"], "url": req.get("path", "")}
                if req.get("status"):
                    try:
                        http_request["responseStatusCode"] = int(req["status"])
                    except (TypeError, ValueError):
                        pass
                if req.get("user_agent"):
                    http_request["userAgent"] = req["user_agent"]
                if req.get("remote_ip"):
                    http_request["remoteIp"] = req["remote_ip"]
                context["httpRequest"] = http_request

            entry = {
                "@type": _REPORTED_ERROR_TYPE,
                "severity": "ERROR" if record.levelno < logging.CRITICAL else "CRITICAL",
                "message": message,
                "serviceContext": self._service_context,
                "context": context,
                "logging.googleapis.com/labels": {
                    "logger": record.name,
                    "er_handler": "adam-alerting",
                },
            }
            sys.stdout.write(json.dumps(entry, ensure_ascii=False) + "\n")
            sys.stdout.flush()
        except Exception:  # noqa: BLE001 - a logging handler must never raise
            pass


_original_excepthook = None


def _logging_excepthook(exc_type, exc, tb):
    """Route uncaught exceptions through logging before the default hook.

    A root logging handler never sees exceptions that escape main() — the
    exact failure mode of the shard entrypoint (a raise propagating out of
    run_shard printed a raw stderr traceback that GKE truncated mid-line).
    KeyboardInterrupt stays silent, matching normal operator expectations.
    """
    try:
        if not issubclass(exc_type, KeyboardInterrupt):
            logging.getLogger("adam.uncaught").critical(
                "uncaught exception: %s", exc, exc_info=(exc_type, exc, tb)
            )
    except Exception:  # noqa: BLE001 - the hook must never mask the crash
        pass
    if _original_excepthook is not None:
        _original_excepthook(exc_type, exc, tb)


def maybe_install() -> bool:
    """Attach the handler to the root logger once; True if active.

    Call after ``logging.basicConfig`` at process start. Also chains a
    sys.excepthook so uncaught exceptions report before the process dies.
    Safe to call more than once, and safe (inert) when ER_LOGGING_ENABLED
    is unset.
    """
    global _original_excepthook
    root = logging.getLogger()
    for existing in root.handlers:
        if isinstance(existing, ErrorReportingHandler):
            return existing._enabled
    handler = ErrorReportingHandler()
    root.addHandler(handler)
    if handler._enabled and sys.excepthook is not _logging_excepthook:
        _original_excepthook = sys.excepthook
        sys.excepthook = _logging_excepthook
    return handler._enabled
