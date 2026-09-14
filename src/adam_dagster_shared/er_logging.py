"""Error Reporting bridge for non-Dagster processes (adam-api, workers).

A root logging handler that writes one ER-compatible JSON line to stdout per
ERROR+ record, plus a chained sys.excepthook so uncaught exceptions report as
CRITICAL. Inert unless ER_LOGGING_ENABLED is set; stdlib only; never raises.
Message layout and grouping rules: docs/ALERTING.md.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import traceback

_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
REPORTED_ERROR_TYPE = (
    "type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent"
)
# Path segments that vary per request, not per bug: UUIDs, long hex ids, integers.
_ID_SEG = re.compile(
    r"^(\d+|[0-9a-fA-F]{16,}|[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})$"
)


def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name, "").strip().lower()
    return value in ("1", "true", "yes") if value else default


def current_namespace() -> str:
    """K8s namespace from env, then the serviceaccount mount, else "local"."""
    for var in ("GARDEN_NAMESPACE", "K8S_NAMESPACE", "KUBERNETES_NAMESPACE"):
        if os.environ.get(var, "").strip():
            return os.environ[var].strip()
    try:
        with open(_NAMESPACE_PATH) as f:
            return f.read().strip()
    except OSError:
        return "local"


def _service_context() -> dict:
    # Read at emit time: entrypoints may set ER_SERVICE_NAME after install.
    service = os.environ.get("ER_SERVICE_NAME", "").strip() or "service"
    version = (
        os.environ.get("ER_SERVICE_VERSION", "").strip()
        or os.environ.get("GARDEN_ACTION_VERSION", "").strip()
        or "unknown"
    )
    return {"service": f"{current_namespace()}/{service}", "version": version}


def neutralize_stack(text: str) -> str:
    """Rewrite traceback headers so ER never switches to frame-based grouping."""
    return text.replace(
        "Traceback (most recent call last):", "Stack trace (most recent call last):"
    )


def normalize_path(path: str) -> str:
    """/jobs/<uuid>/results/42 -> /jobs/{id}/results/{id}."""
    return "/".join("{id}" if _ID_SEG.match(seg) else seg for seg in path.split("/")) or "/"


def footer(*bits: str) -> str:
    """Trailing "-- k: v · k: v" line: context that must not affect grouping."""
    kept = [b for b in bits if b]
    return "\n-- " + " · ".join(kept) if kept else ""


def write_entry(
    message: str,
    critical: bool,
    service_context: dict,
    file_path: str,
    function_name: str,
    labels: dict,
    http_request: dict | None = None,
) -> None:
    """Print one ReportedErrorEvent line; the GKE agent forwards it to ER."""
    context: dict = {
        "reportLocation": {"filePath": file_path, "lineNumber": 0, "functionName": function_name}
    }
    if http_request:
        context["httpRequest"] = http_request
    entry = {
        "@type": REPORTED_ERROR_TYPE,
        "severity": "CRITICAL" if critical else "ERROR",
        "message": message,
        "serviceContext": service_context,
        "context": context,
        "logging.googleapis.com/labels": {"er_handler": "adam-alerting", **labels},
    }
    sys.stdout.write(json.dumps(entry, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _request_bits(record: logging.LogRecord) -> dict:
    """Request context from ``extra`` fields or a Django request; never touches the DB."""
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
            # Only an already-resolved user: a lazy proxy could query the database.
            user = request.__dict__.get("user")
            if user is not None and user.__class__.__name__ != "SimpleLazyObject":
                if getattr(user, "is_authenticated", False):
                    bits["user"] = getattr(user, "email", "") or getattr(user, "username", "")
        except Exception:
            pass
    for key, value in (("method", method), ("path", path), ("status", status)):
        if value:
            bits[key] = str(value)
    return bits


class ErrorReportingHandler(logging.Handler):
    """One ER JSON line per ERROR+ record; pass to ``basicConfig(handlers=[...])`` or dictConfig.

    Constructing it also chains ``sys.excepthook`` so uncaught exceptions report as CRITICAL.
    """

    def __init__(self, level: int | str = logging.ERROR):
        super().__init__(level=level)
        _chain_excepthook()

    def emit(self, record: logging.LogRecord) -> None:
        if not env_flag("ER_LOGGING_ENABLED"):
            return
        try:
            exc_info = record.exc_info if record.exc_info and record.exc_info[0] else None
            # django.request without exc_info is Django's "Internal Server Error: /path"
            # echo of an exception the application handler already reported.
            if record.name == "django.request" and exc_info is None:
                return
            message = record.getMessage()
            exc_cls = ""
            if exc_info is not None:
                exc_cls = exc_info[0].__name__
                message += "\n" + neutralize_stack("".join(traceback.format_exception(*exc_info)))

            req = _request_bits(record)
            has_request = bool(req.get("method") and req.get("path"))
            route = f"{req['method']} {normalize_path(req['path'])}" if has_request else ""
            if record.name.startswith("django.security"):
                route = ""  # attacker-chosen paths would mint one group per probe
            if exc_cls:
                where = f"at {route}" if route else f"in {record.name}"
                message = f"{exc_cls} {where}:\n{message}"
            critical = record.levelno >= logging.CRITICAL
            message += footer(
                f"request: {req['method']} {req['path']}" if has_request else "",
                f"status: {req['status']}" if req.get("status") else "",
                f"user: {req['user']}" if req.get("user") else "",
                f"client: {req['remote_ip']}" if req.get("remote_ip") else "",
                "severity: critical" if critical else "",
            )

            http_request = None
            if req.get("method"):
                http_request = {"method": req["method"], "url": req.get("path", "")}
                if req.get("status", "").isdigit():
                    http_request["responseStatusCode"] = int(req["status"])
                if req.get("user_agent"):
                    http_request["userAgent"] = req["user_agent"]
                if req.get("remote_ip"):
                    http_request["remoteIp"] = req["remote_ip"]
            ctx = _service_context()
            write_entry(
                message,
                critical,
                ctx,
                file_path=ctx["service"].split("/", 1)[-1],
                function_name=route or record.name,
                labels={"logger": record.name},
                http_request=http_request,
            )
        except Exception:  # noqa: BLE001 - a logging handler must never raise
            pass


_original_excepthook = None


def _excepthook(exc_type, exc, tb):
    try:
        if not issubclass(exc_type, KeyboardInterrupt):
            logging.getLogger("adam.uncaught").critical(
                "uncaught exception: %s", exc, exc_info=(exc_type, exc, tb)
            )
    except Exception:  # noqa: BLE001 - the hook must never mask the crash
        pass
    if _original_excepthook is not None:
        _original_excepthook(exc_type, exc, tb)


def _chain_excepthook() -> None:
    global _original_excepthook
    if env_flag("ER_LOGGING_ENABLED") and sys.excepthook is not _excepthook:
        _original_excepthook = sys.excepthook
        sys.excepthook = _excepthook
