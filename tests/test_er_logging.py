"""Tests for adam_dagster_shared.er_logging.

ER groups stackless entries by the first 3 message tokens + reportLocation.functionName,
so the "<Cls> in <logger>" / "<Cls> at <METHOD> /route>" headline IS the fingerprint.
"""

import json
import logging
import sys
import threading

import pytest

import adam_dagster_shared.er_logging as er
from adam_dagster_shared.er_logging import ErrorReportingHandler, normalize_path


@pytest.fixture(autouse=True)
def restore_excepthook():
    before = (sys.excepthook, er._original_excepthook, threading.excepthook, er._original_thread_excepthook)
    yield
    sys.excepthook, er._original_excepthook, threading.excepthook, er._original_thread_excepthook = before


def _record(msg="boom", logger_name="adam_jobs.cutout_worker.loop", exc=None, **attrs):
    record = logging.LogRecord(logger_name, logging.ERROR, __file__, 1, msg, (), None)
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
    # Every traceback header is rewritten (one intact header flips ER to frame-based grouping).
    assert "Traceback (most recent call last):" not in entry["message"]
    assert entry["message"].count("Stack trace (most recent call last):") == 2
    assert "in test_headline_and_function_name_carry_the_grouping_key" in entry["message"]

    class Hostile:
        def __str__(self):
            raise RuntimeError("hostile __str__")

    assert _emit(monkeypatch, capsys, _record(msg=Hostile())) is None, "a handler never raises"
    monkeypatch.delenv("ER_LOGGING_ENABLED")
    ErrorReportingHandler().emit(_record())
    assert capsys.readouterr().out == "", "inert without ER_LOGGING_ENABLED"


def test_handler_chains_excepthook_and_reports_uncaught_as_critical(monkeypatch, capsys):
    monkeypatch.setenv("ER_LOGGING_ENABLED", "true")
    monkeypatch.setenv("GARDEN_NAMESPACE", "unit-test-ns")
    monkeypatch.setenv("ER_SERVICE_NAME", "precovery-v2-shard")
    chained = []
    sys.excepthook = lambda *a: chained.append(a)
    root = logging.getLogger()
    before = list(root.handlers)
    try:
        root.addHandler(ErrorReportingHandler())
        ErrorReportingHandler()  # a second construction must not re-chain
        assert sys.excepthook is er._excepthook and er._original_excepthook is not er._excepthook
        try:
            raise RuntimeError("final shard publish lacks current commit markers")
        except RuntimeError:
            sys.excepthook(*sys.exc_info())
    finally:
        root.handlers = list(before)  # a copy: addHandler() must not mutate `before`
    out = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert len(out) == 1
    assert out[0]["severity"] == "CRITICAL"
    assert out[0]["message"].startswith("RuntimeError in adam.uncaught:")
    assert out[0]["message"].splitlines()[-1] == "-- severity: critical"  # renderer styles from this
    assert out[0]["serviceContext"]["service"] == "unit-test-ns/precovery-v2-shard"
    assert len(chained) == 1, "original excepthook must still run, once"
    # log-then-raise: the crash joins the logger's group instead of minting "adam.uncaught"
    try:
        root.addHandler(ErrorReportingHandler())
        try:
            raise ValueError("already logged")
        except ValueError as exc:
            logging.getLogger("adam_jobs.shard").exception("publish failed")
            assert er.already_reported(exc)
            sys.excepthook(*sys.exc_info())
    finally:
        root.handlers = list(before)  # a copy: addHandler() must not mutate `before`
    out = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert [o["message"].split(":")[0] for o in out] == ["ValueError in adam_jobs.shard"] * 2
    assert out[1]["severity"] == "CRITICAL"


def test_request_records_group_by_route_redact_tokens_and_skip_django_echoes(monkeypatch, capsys):
    assert normalize_path("/v1/jobs/123e4567-e89b-12d3-a456-426614174000/results/42") == "/v1/jobs/{id}/results/{id}"
    record = _record("Unhandled exception during GET x: boom", logger_name="api.api",
                     exc=ValueError("boom"), http_method="GET", http_path="/v1/jobs/9999", http_status=500)
    entry = _emit(monkeypatch, capsys, record, service="adam-api")
    assert entry["message"].startswith("ValueError at GET /v1/jobs/{id}:")
    assert entry["context"]["reportLocation"]["functionName"] == "GET /v1/jobs/{id}"
    assert "-- request: GET /v1/jobs/{id} · status: 500" in entry["message"]
    assert entry["context"]["httpRequest"]["responseStatusCode"] == 500

    token = "SyntheticShareToken_With-32CharsXX"  # secrets.token_urlsafe(24) shape
    path = f"/api/share/precovery_v2/{token}/resources/"
    record = _record(f"Unhandled exception during GET {path}: boom", logger_name="api.api",
                     exc=ValueError("boom"), http_method="GET", http_path=path, http_status=500)
    entry = _emit(monkeypatch, capsys, record, service="adam-api")
    assert token not in json.dumps(entry), "a sharing capability token must never reach ER or Slack"
    assert entry["context"]["httpRequest"]["url"] == "/api/share/precovery_v2/{token}/resources/"
    assert normalize_path("/v1/refresh-tokens/AbC123xyz_9876543210-QwErTy/") == "/v1/refresh-tokens/{token}/"

    # django.security: attacker-chosen paths must not mint one group per probe.
    record = _record("Invalid HTTP_HOST header.", logger_name="django.security.DisallowedHost",
                     exc=Exception("x"), http_method="GET", http_path="/boaform/admin/formLogin",
                     http_status=400)
    entry = _emit(monkeypatch, capsys, record, service="adam-api")
    assert entry["message"].startswith("Exception in django.security.DisallowedHost:")
    assert entry["context"]["reportLocation"]["functionName"] == "django.security.DisallowedHost"

    # Django's "Internal Server Error: /path" (no exc_info) duplicates the application record.
    echo = _record("Internal Server Error: /api/x", logger_name="django.request", status_code=500)
    assert _emit(monkeypatch, capsys, echo, service="adam-api") is None
    real = _record("Internal Server Error: /api/x", logger_name="django.request", exc=KeyError("boom"))
    assert _emit(monkeypatch, capsys, real, service="adam-api")["message"].startswith("KeyError in django.request:")


def test_thread_crashes_report_critical_and_long_messages_stay_bounded(monkeypatch, capsys):
    monkeypatch.setenv("ER_LOGGING_ENABLED", "true")
    monkeypatch.setenv("GARDEN_NAMESPACE", "unit-test-ns")
    monkeypatch.setenv("ER_SERVICE_NAME", "cutout-worker")
    threading.excepthook = lambda args: None  # keep pytest's stderr clean
    root = logging.getLogger()
    before = list(root.handlers)

    def crash():
        raise RuntimeError("publisher thread died")

    try:
        root.addHandler(ErrorReportingHandler())
        assert threading.excepthook is er._thread_excepthook
        t = threading.Thread(target=crash, name="publisher")
        t.start()
        t.join()
    finally:
        root.handlers = list(before)  # a copy: addHandler() must not mutate `before`
    out = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert len(out) == 1, [o["message"].splitlines()[0] + " / " + o["logging.googleapis.com/labels"]["logger"] for o in out]
    assert out[0]["severity"] == "CRITICAL"
    assert out[0]["message"].startswith("RuntimeError in adam.uncaught:")
    assert "in thread publisher" in out[0]["message"]

    def log_then_crash():
        try:
            raise ValueError("library failed")
        except ValueError:
            logging.getLogger("adam_jobs.publisher").exception("publish failed")
            raise

    try:
        root.addHandler(ErrorReportingHandler())
        t = threading.Thread(target=log_then_crash, name="publisher-2")
        t.start()
        t.join()
    finally:
        root.handlers = list(before)
    heads = [json.loads(line)["message"].splitlines()[0] for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert heads == ["ValueError in adam_jobs.publisher:"] * 2, "a log-then-raise in a thread keeps the logger's group"

    big = "ValueError in run:\n" + "x" * 200_000 + "\n-- run_id: abc"
    bounded = er.bounded(big)
    assert len(bounded) < 61_000 and bounded.startswith("ValueError in run:") and bounded.endswith("-- run_id: abc")
    assert "chars truncated" in bounded and er.bounded("short") == "short"
