"""Tests for the Error Reporting logging bridge (adam_jobs/er_logging.py).

The handler's contract is a grouping-stability contract: ER groups stackless
entries by the first 3 message tokens + reportLocation.functionName, so the
"<Cls> in <logger>" headline and the logger-name functionName ARE the
fingerprint. These tests pin that contract.
"""

import json
import logging
import sys

from adam_dagster_shared.er_logging import ErrorReportingHandler, maybe_install


def _record(msg="boom", logger_name="adam_jobs.cutout_worker.loop", exc=None):
    record = logging.LogRecord(
        name=logger_name,
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=(),
        exc_info=None,
    )
    if exc is not None:
        try:
            raise exc
        except type(exc):
            record.exc_info = sys.exc_info()
    return record


def _emit(monkeypatch, capsys, record):
    monkeypatch.setenv("ER_LOGGING_ENABLED", "true")
    monkeypatch.setenv("GARDEN_NAMESPACE", "unit-test-ns")
    monkeypatch.setenv("ER_SERVICE_NAME", "cutout-worker-ztf")
    handler = ErrorReportingHandler()
    handler.emit(record)
    out = capsys.readouterr().out.strip()
    return json.loads(out) if out else None


def test_disabled_without_env(monkeypatch, capsys):
    monkeypatch.delenv("ER_LOGGING_ENABLED", raising=False)
    ErrorReportingHandler().emit(_record())
    assert capsys.readouterr().out == ""


def test_headline_and_function_name_carry_the_grouping_key(monkeypatch, capsys):
    entry = _emit(
        monkeypatch,
        capsys,
        _record("unhandled error executing entry 42", exc=ValueError("bad row")),
    )
    assert entry["message"].startswith("ValueError in adam_jobs.cutout_worker.loop:")
    assert (
        entry["context"]["reportLocation"]["functionName"] == "adam_jobs.cutout_worker.loop"
    )
    assert entry["serviceContext"] == {
        "service": "unit-test-ns/cutout-worker-ztf",
        "version": "unknown",
    }
    assert entry["@type"].endswith("ReportedErrorEvent")


def test_traceback_header_is_not_er_parsable(monkeypatch, capsys):
    entry = _emit(monkeypatch, capsys, _record(exc=RuntimeError("x")))
    assert "Traceback (most recent call last):" not in entry["message"]
    assert "Stack trace (most recent call last):" in entry["message"]
    assert "in _record" in entry["message"]  # frames survive for humans


def test_never_raises_on_hostile_record(monkeypatch, capsys):
    class Hostile:
        def __str__(self):
            raise RuntimeError("hostile __str__")

    monkeypatch.setenv("ER_LOGGING_ENABLED", "true")
    ErrorReportingHandler().emit(_record(msg=Hostile()))
    assert capsys.readouterr().out == ""


def test_maybe_install_is_idempotent(monkeypatch):
    monkeypatch.setenv("ER_LOGGING_ENABLED", "true")
    root = logging.getLogger()
    before = list(root.handlers)
    before_hook = sys.excepthook
    try:
        assert maybe_install() is True
        assert maybe_install() is True
        added = [h for h in root.handlers if isinstance(h, ErrorReportingHandler)]
        assert len(added) == 1
    finally:
        root.handlers = before
        sys.excepthook = before_hook


def test_uncaught_exceptions_report_as_critical(monkeypatch, capsys):
    # A raise escaping main() never reaches a logging handler — the shard
    # entrypoint's real failure mode — so maybe_install chains sys.excepthook.
    import adam_dagster_shared.er_logging as er

    monkeypatch.setenv("ER_LOGGING_ENABLED", "true")
    monkeypatch.setenv("GARDEN_NAMESPACE", "unit-test-ns")
    monkeypatch.setenv("ER_SERVICE_NAME", "precovery-v2-shard")
    root = logging.getLogger()
    before = list(root.handlers)
    before_hook = sys.excepthook
    chained = []
    try:
        sys.excepthook = lambda *a: chained.append(a)
        assert maybe_install() is True
        assert sys.excepthook is er._logging_excepthook
        try:
            raise RuntimeError("final shard publish lacks current commit markers")
        except RuntimeError:
            sys.excepthook(*sys.exc_info())
        out = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.startswith("{")
        ]
        assert len(out) == 1
        entry = out[0]
        assert entry["severity"] == "CRITICAL"
        assert entry["message"].startswith("RuntimeError in adam.uncaught:")
        # Renderer styles CRITICAL from the footer (ER events carry no severity).
        assert entry["message"].splitlines()[-1] == "-- severity: critical"
        assert entry["serviceContext"]["service"] == "unit-test-ns/precovery-v2-shard"
        assert chained, "original excepthook must still run"
    finally:
        root.handlers = before
        sys.excepthook = before_hook
        er._original_excepthook = None


# ===== Request-carrying records (web-service flavor, from adam-api) =====


def _emit_req(monkeypatch, capsys, record):
    monkeypatch.setenv("ER_LOGGING_ENABLED", "true")
    monkeypatch.setenv("GARDEN_NAMESPACE", "unit-test-ns")
    monkeypatch.setenv("ER_SERVICE_NAME", "adam-api")
    from adam_dagster_shared.er_logging import ErrorReportingHandler

    ErrorReportingHandler().emit(record)
    out = capsys.readouterr().out.strip()
    return json.loads(out) if out else None


def test_request_records_group_by_route(monkeypatch, capsys):
    from adam_dagster_shared.er_logging import normalize_path

    assert (
        normalize_path("/v1/jobs/123e4567-e89b-12d3-a456-426614174000/results/42")
        == "/v1/jobs/{id}/results/{id}"
    )
    record = _record("Unhandled exception during GET x: boom", logger_name="api.api",
                     exc=ValueError("boom"))
    record.http_method = "GET"
    record.http_path = "/v1/jobs/9999"
    record.http_status = 500
    entry = _emit_req(monkeypatch, capsys, record)
    assert entry["message"].startswith("ValueError at GET /v1/jobs/{id}:")
    assert entry["context"]["reportLocation"]["functionName"] == "GET /v1/jobs/{id}"
    assert "-- request: GET /v1/jobs/9999 · status: 500" in entry["message"]
    assert entry["context"]["httpRequest"]["responseStatusCode"] == 500


def test_security_records_group_without_route(monkeypatch, capsys):
    record = _record("Invalid HTTP_HOST header.", logger_name="django.security.DisallowedHost",
                     exc=Exception("x"))
    record.http_method = "GET"
    record.http_path = "/boaform/admin/formLogin"
    record.http_status = 400
    entry = _emit_req(monkeypatch, capsys, record)
    # One group per security class, not one per scanner-probed path.
    assert entry["message"].startswith("Exception in django.security.DisallowedHost:")
    assert (
        entry["context"]["reportLocation"]["functionName"] == "django.security.DisallowedHost"
    )


def test_django_request_response_echo_is_skipped(monkeypatch, capsys):
    # Django logs "Internal Server Error: /path" (no exc_info) for every 5xx
    # response — a duplicate of the application handler's rich record.
    record = _record("Internal Server Error: /api/x", logger_name="django.request")
    record.status_code = 500
    assert _emit_req(monkeypatch, capsys, record) is None


def test_django_request_with_real_exception_is_kept(monkeypatch, capsys):
    record = _record("Internal Server Error: /api/x", logger_name="django.request",
                     exc=KeyError("boom"))
    entry = _emit_req(monkeypatch, capsys, record)
    assert entry["message"].startswith("KeyError in django.request:")
