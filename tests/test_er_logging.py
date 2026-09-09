"""Tests for the Error Reporting logging bridge (adam_dagster_shared.er_logging).

The handler's contract is a grouping-stability contract: ER groups stackless
entries by the first 3 message tokens + reportLocation.functionName, so the
"<Cls> in <logger>" / "<Cls> at <METHOD> /route>" headline and the
functionName ARE the fingerprint. These tests pin that contract.
"""

import json
import logging
import sys

import adam_dagster_shared.er_logging as er
from adam_dagster_shared.er_logging import ErrorReportingHandler, maybe_install, normalize_path


def _record(msg="boom", logger_name="adam_jobs.cutout_worker.loop", exc=None, **attrs):
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
    for key, value in attrs.items():
        setattr(record, key, value)
    return record


def _emit(monkeypatch, capsys, record, service="cutout-worker-ztf"):
    monkeypatch.setenv("ER_LOGGING_ENABLED", "true")
    monkeypatch.setenv("GARDEN_NAMESPACE", "unit-test-ns")
    monkeypatch.setenv("ER_SERVICE_NAME", service)
    ErrorReportingHandler().emit(record)
    out = capsys.readouterr().out.strip()
    return json.loads(out) if out else None


def test_disabled_without_env(monkeypatch, capsys):
    monkeypatch.delenv("ER_LOGGING_ENABLED", raising=False)
    ErrorReportingHandler().emit(_record())
    assert capsys.readouterr().out == ""


def test_headline_and_function_name_carry_the_grouping_key(monkeypatch, capsys):
    try:
        try:
            raise KeyError("inner")
        except KeyError as inner:
            raise ValueError("bad row") from inner
    except ValueError as chained:
        entry = _emit(monkeypatch, capsys, _record("unhandled error executing entry 42", exc=chained))
    assert entry["@type"].endswith("ReportedErrorEvent")
    assert entry["message"].startswith("ValueError in adam_jobs.cutout_worker.loop:")
    assert entry["context"]["reportLocation"]["functionName"] == "adam_jobs.cutout_worker.loop"
    assert entry["serviceContext"] == {"service": "unit-test-ns/cutout-worker-ztf", "version": "unknown"}
    # Every traceback header is rewritten (chained exceptions carry several):
    # one intact header would flip ER to frame-based grouping (review R6).
    # The frames themselves survive for humans.
    assert "Traceback (most recent call last):" not in entry["message"]
    assert entry["message"].count("Stack trace (most recent call last):") == 2
    assert "in test_headline_and_function_name_carry_the_grouping_key" in entry["message"]


def test_never_raises_on_hostile_record(monkeypatch, capsys):
    class Hostile:
        def __str__(self):
            raise RuntimeError("hostile __str__")

    assert _emit(monkeypatch, capsys, _record(msg=Hostile())) is None


def test_maybe_install_chains_excepthook_and_reports_uncaught_as_critical(monkeypatch, capsys):
    # A raise escaping main() never reaches a logging handler — the shard
    # entrypoint's real failure mode — so maybe_install chains sys.excepthook.
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
        assert maybe_install() is True  # idempotent
        assert len([h for h in root.handlers if isinstance(h, ErrorReportingHandler)]) == 1
        assert sys.excepthook is er._logging_excepthook
        try:
            raise RuntimeError("final shard publish lacks current commit markers")
        except RuntimeError:
            sys.excepthook(*sys.exc_info())
        out = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
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


def test_request_records_group_by_normalized_route(monkeypatch, capsys):
    assert (
        normalize_path("/v1/jobs/123e4567-e89b-12d3-a456-426614174000/results/42")
        == "/v1/jobs/{id}/results/{id}"
    )
    record = _record("Unhandled exception during GET x: boom", logger_name="api.api",
                     exc=ValueError("boom"), http_method="GET", http_path="/v1/jobs/9999", http_status=500)
    entry = _emit(monkeypatch, capsys, record, service="adam-api")
    assert entry["message"].startswith("ValueError at GET /v1/jobs/{id}:")
    assert entry["context"]["reportLocation"]["functionName"] == "GET /v1/jobs/{id}"
    assert "-- request: GET /v1/jobs/9999 · status: 500" in entry["message"]
    assert entry["context"]["httpRequest"]["responseStatusCode"] == 500


def test_security_records_group_per_class_not_per_probed_path(monkeypatch, capsys):
    record = _record("Invalid HTTP_HOST header.", logger_name="django.security.DisallowedHost",
                     exc=Exception("x"), http_method="GET", http_path="/boaform/admin/formLogin",
                     http_status=400)
    entry = _emit(monkeypatch, capsys, record, service="adam-api")
    assert entry["message"].startswith("Exception in django.security.DisallowedHost:")
    assert entry["context"]["reportLocation"]["functionName"] == "django.security.DisallowedHost"


def test_django_request_echo_is_skipped_unless_it_carries_an_exception(monkeypatch, capsys):
    # Django logs "Internal Server Error: /path" (no exc_info) for every 5xx
    # response — a duplicate of the application handler's rich record.
    echo = _record("Internal Server Error: /api/x", logger_name="django.request", status_code=500)
    assert _emit(monkeypatch, capsys, echo, service="adam-api") is None
    real = _record("Internal Server Error: /api/x", logger_name="django.request", exc=KeyError("boom"))
    assert _emit(monkeypatch, capsys, real, service="adam-api")["message"].startswith(
        "KeyError in django.request:"
    )
