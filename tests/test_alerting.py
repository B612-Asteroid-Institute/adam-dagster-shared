"""Tests for adam_dagster_shared.alerting.

Failure payloads are captured from production (2026-08); the renderer tests stub ER
and Slack at module boundaries; the last test runs a real ephemeral Dagster instance.
"""

import datetime as dt
import json
import logging
import sys
from types import SimpleNamespace

import dagster as dg
import pytest

from adam_dagster_shared.alerting import renderer, sensors
from adam_dagster_shared.alerting import slack as slack_mod
from adam_dagster_shared.alerting.classify import FailureContext, StepFailure, classify
from adam_dagster_shared.alerting.er_log_handler import ErrorReportingHandler
from adam_dagster_shared.alerting.renderer import parse_reported_message, render_group_blocks

K8S_DEATH_MSG = (
    "Step aims_observation_index_shards failed health check: Discovered failed "
    "Kubernetes job dagster-step-0bc35374201b422f01472fe0ed958ef7 for step "
    "aims_observation_index_shards."
)
DUPLICATE_GATE_MSG = (
    "RuntimeError: cross-dataset duplicate gate failed for 2015-02-12: 534 rows "
    "participate in same-station/close-time/close-position groups"
)
RECONCILIATION_MSG = (
    "dagster._core.definitions.events.Failure: Unified AIMS+MPC reconciliation "
    "mismatch detected for 2026-08-23..2026-08-23."
)
CRASH_RESUME_MSG = (
    "dagster_shared.check.functions.CheckError: Invariant failed. Description: "
    "Attempted to mark step aims_observation_index_shards as complete"
)
OOM_MSG = "Step failed health check: discovered failed Kubernetes job; container OOMKilled"
NS = "error-reporting-alerting"


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("GARDEN_NAMESPACE", "production")
    monkeypatch.setenv("ER_LOGGING_ENABLED", "true")
    monkeypatch.setenv("ALERTING_DRY_RUN", "true")

    def unexpected(*a, **kw):
        raise AssertionError("Unexpected external call")

    monkeypatch.setattr(renderer, "_er_get", unexpected)
    monkeypatch.setattr(renderer, "post_message", unexpected)
    monkeypatch.setattr(sensors, "post_message", unexpected)


def _ctx(tags=None, step_failures=None, run_failure=None, job="__ASSET_JOB"):
    return FailureContext("abc123", job, tags or {}, step_failures or [], run_failure)


def _entry(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 1, "the handler must emit exactly one JSON line per record"
    return json.loads(lines[0])


def group(gid="new", services=None, count="1", first_seen="2026-09-01T20:14:56Z"):
    return {
        "group": {"groupId": gid},
        "count": count,
        "firstSeenTime": first_seen,
        "affectedServices": [{"service": s} for s in (services or ["production/adam-api"])],
    }


def event(service="production/adam-api"):
    return {
        "message": "RuntimeError at GET /api/example:\nRuntimeError: failed",
        "serviceContext": {"service": service, "version": "test"},
    }


def tick(cursor=None):
    with dg.build_sensor_context(cursor=cursor) as ctx:
        renderer.alerting_er_group_renderer(ctx)
        return ctx.cursor


# ===== Classification =====


@pytest.mark.parametrize(
    "ctx,klass,user_facing,exception_contains",
    [
        (
            _ctx(tags={"dagster/backfill": "abc"},
                 step_failures=[StepFailure("aims_observation_index_shards", [""], [K8S_DEATH_MSG])]),
            "k8s-job-death", False, None,
        ),
        (
            _ctx(tags={"dagster/auto_materialize": "true"},
                 step_failures=[StepFailure(
                     "aims_observation_index_shards",
                     ["RetryRequestedFromPolicy", "RuntimeError"],
                     ["Exceeded max_retries of 0", DUPLICATE_GATE_MSG],
                 )]),
            "code-error", False, "duplicate gate",
        ),
        (
            _ctx(tags={"dagster/schedule_name": "recon"},
                 step_failures=[StepFailure("reconcile", ["Failure"], [RECONCILIATION_MSG])],
                 job="unified_aims_mpc_reconciliation_daily_job"),
            "quality-gate", False, None,
        ),
        (_ctx(run_failure=("CheckError", CRASH_RESUME_MSG)), "run-worker-crash", False, None),
        # The OOM marker wins over the generic k8s-death phrase it also matches.
        (_ctx(step_failures=[StepFailure("system", ["RuntimeError"], [OOM_MSG])]),
         "system-oom", False, None),
        (_ctx(tags={"external-user": "u@e.org"}, step_failures=[StepFailure("s", ["Exception"], [OOM_MSG])]),
         "user-job-failure", True, None),
        (_ctx(), "unknown", False, None),
    ],
    ids=["k8s-death", "retry-unwrap", "quality-gate", "crash-resume", "oom-precedence", "external-user", "empty"],
)
def test_classify_captured_production_failures(ctx, klass, user_facing, exception_contains):
    v = classify(ctx)
    assert (v.klass, v.user_facing) == (klass, user_facing)
    if exception_contains:
        assert v.exception and "RuntimeError" in v.exception and exception_contains in v.exception


# ===== Slack blocks =====


def _buttons(blocks):
    return [el for b in blocks if b.get("type") == "actions" for el in b["elements"]]


def test_digest_blocks_render_counts_and_er_section_with_button_links():
    summary = {
        "date_label": "Tue Aug 26", "total_failures": 155, "canceled": 217,
        "by_class": [("code-error", 90), ("k8s-job-death", 38)],
        "user_failures": 2, "user_count": 2,
        "er": {
            "groups": 7, "new_today": 2,
            "top": [
                (12, "ZtfFetchTimeout in adam_jobs.cutout_worker.executor", "cutout-worker-ztf", "Cg1"),
                (4, "OperationalError at POST /api/precovery/{id}", "adam-api", "Cg2"),
            ],
        },
    }
    fallback, blocks = slack_mod.digest_blocks(summary, "production")
    text = json.dumps(blocks)
    assert "155" in text and "217" in text and "155" in fallback
    assert "code-error 90" in text
    assert "7 active error groups in 24h \\u00b7 2 new" in text
    assert "\\u00d712 \\u2014 `ZtfFetchTimeout in adam_jobs.cutout_worker.executor` (cutout-worker-ztf)" in text
    # Links are URL buttons only (Slack unfurls links inside blocks regardless of unfurl_links).
    assert "<http" not in text
    assert "errors/detail/Cg2" in text and all(b["url"].startswith("https://") for b in _buttons(blocks))
    # ER unavailable (fail-open) renders an explicit line, never raises.
    summary["er"] = None
    assert "unavailable this run" in json.dumps(slack_mod.digest_blocks(summary, "production")[1])


def test_dry_run_without_token_secret_logs_instead_of_posting(monkeypatch):
    monkeypatch.delenv("ALERTING_SLACK_TOKEN_SECRET", raising=False)
    monkeypatch.delenv("ALERTING_DRY_RUN", raising=False)
    assert slack_mod.dry_run() is True
    monkeypatch.setenv("ALERTING_SLACK_TOKEN_SECRET", "projects/p/secrets/s/versions/latest")
    assert slack_mod.dry_run() is False
    monkeypatch.setenv("ALERTING_DRY_RUN", "true")
    lines = []
    log = SimpleNamespace(info=lines.append, warning=lines.append)
    assert slack_mod.post_message(log, "fallback", [{"type": "section"}]) is True
    assert lines[0].startswith("ALERTING_DRY_RUN_POST ")


# ===== Watchdog =====


def test_watchdog_threshold_from_p95_and_flagging_window():
    assert sensors.p95_seconds([]) is None
    assert sensors.p95_seconds(list(map(float, range(1, 101)))) == 96.0
    assert sensors.hung_threshold(63 * 60) == 3 * 63 * 60
    assert sensors.hung_threshold(30 * 60) == 2 * 3600  # floor
    assert sensors.hung_threshold(None) == 6 * 3600  # no history

    flag = sensors.watchdog_should_flag
    state = {"alerted": {}}
    now = 1_000_000.0
    assert not flag(state, "run-1", age=3600, threshold=2 * 3600, now=now) and state["alerted"] == {}
    assert flag(state, "run-1", age=8 * 3600, threshold=2 * 3600, now=now)
    assert not flag(state, "run-1", age=9 * 3600, threshold=2 * 3600, now=now + 600)  # re-alert window
    assert flag(state, "run-1", age=33 * 3600, threshold=2 * 3600, now=now + 25 * 3600)


def test_watchdog_can_see_oldest_run_during_more_than_200_started_runs(monkeypatch):
    now = dt.datetime.now(dt.timezone.utc)
    records = [
        SimpleNamespace(
            dagster_run=SimpleNamespace(run_id=f"fresh-{i}", job_name="system", tags={}),
            start_time=now.timestamp(),
            create_timestamp=now,
        )
        for i in range(200)
    ]
    records.append(
        SimpleNamespace(
            dagster_run=SimpleNamespace(run_id="hung-oldest", job_name="system", tags={}),
            start_time=now.timestamp() - 3 * 86400,
            create_timestamp=now - dt.timedelta(days=3),
        )
    )

    def get(*, filters, limit, **kw):
        if filters.statuses == [dg.DagsterRunStatus.SUCCESS]:
            return []
        return (list(reversed(records)) if kw.get("ascending") else records)[:limit]

    monkeypatch.setenv("ALERTING_WATCHDOG_POSTING", "false")
    with dg.DagsterInstance.ephemeral() as instance:
        monkeypatch.setattr(instance, "get_run_records", get)
        with dg.build_sensor_context(instance=instance) as ctx:
            sensors.alerting_hung_run_watchdog(ctx)
            state = json.loads(ctx.cursor)
    assert "hung-oldest" in state["alerted"]


# ===== Dagster log handler =====


def test_er_handler_strips_run_prefix_and_adds_footer(capsys):
    rec = logging.LogRecord(
        "dagster", logging.ERROR, "x.py", 1,
        "__ASSET_JOB - 1728932c-6614-443a-a9d6-aa958bcd107e - 1 - STEP_FAILURE - boom",
        None, None,
    )
    rec.dagster_meta = {
        "orig_message": "Execution of step \"aims_mpc_hot_archive\" failed.\nStep failed health check",
        "run_id": "1728932c-6614-443a-a9d6-aa958bcd107e",
        "step_key": "aims_mpc_hot_archive",
        "job_name": "__ASSET_JOB",
    }
    ErrorReportingHandler().emit(rec)
    entry = _entry(capsys)
    assert entry["@type"].endswith("ReportedErrorEvent") and entry["severity"] == "ERROR"
    assert entry["message"].startswith("Execution of step")  # grouping tokens: no run-id prefix
    assert "run_id: 1728932c" in entry["message"]
    assert entry["context"]["reportLocation"]["functionName"] == "aims_mpc_hot_archive"
    assert entry["logging.googleapis.com/labels"]["dagster_run_id"].startswith("1728932c")


def test_er_handler_neutralizes_exc_info_and_never_raises(capsys):
    try:
        raise ValueError("kaboom")
    except ValueError:
        rec = logging.LogRecord("app", logging.ERROR, "x.py", 1, "ctx", None, sys.exc_info())
    ErrorReportingHandler().emit(rec)
    entry = _entry(capsys)
    assert entry["message"].startswith("ValueError in run:") and "ValueError: kaboom" in entry["message"]
    assert "Traceback (most recent call last):" not in entry["message"]
    assert "Stack trace (most recent call last):" in entry["message"]

    class _Evil:
        def __str__(self):
            raise RuntimeError("unformattable")

    ErrorReportingHandler().emit(logging.LogRecord("app", logging.ERROR, "x.py", 1, _Evil(), None, None))
    assert capsys.readouterr().out == ""


# ===== Renderer: message parsing and cards =====


def test_parse_reported_message_dagster_api_and_long_bodies():
    p = parse_reported_message(
        "NotFound in barrage_unified_mirror:\n"
        'Execution of step "barrage_unified_mirror" failed.\n\n'
        "google.api_core.exceptions.NotFound: 404 Not found: Dataset x was not found\n"
        "Stack Trace:\n  File \"a.py\", line 1, in f\n"
        "-- run_id: f46c7b33-9f01-44ce-903a-b637c01f54a2 · job: __ASSET_JOB · step: barrage_unified_mirror"
    )
    assert p["headline"] == "NotFound in barrage_unified_mirror"
    assert p["meta"]["run_id"].startswith("f46c7b33") and p["meta"]["step"] == "barrage_unified_mirror"
    assert p["cause"] == "google.api_core.exceptions.NotFound: 404 Not found: Dataset x was not found"
    assert p["excerpt"].startswith("Stack Trace:")  # boilerplate, cause and footer stripped
    # Lowercase key:value lines and prose with pre-colon spaces are not causes.
    assert parse_reported_message("x:\nurl: https://a.example/b\nSteps failed: [1, 2]\n")["cause"] == ""

    p = parse_reported_message(
        "RuntimeError at GET /api/jobs/{id}:\n"
        "Unhandled exception during GET /api/jobs/42: boom\n"
        "-- request: GET /api/jobs/42 · status: 500 · user: kat@b612.ai · client: 10.1.2.3"
    )
    assert p["meta"] == {"request": "GET /api/jobs/42", "status": "500", "user": "kat@b612.ai", "client": "10.1.2.3"}
    assert p["excerpt"] == ""

    inner = "ValueError: inner cause line\n" + ("  File \"mid.py\", line 9, in f\n" * 80)
    p = parse_reported_message(f"OuterChainError in step_x:\n{inner}OuterChainError: the decisive final line\n")
    assert "⋯" in p["excerpt"] and "the decisive final line" in p["excerpt"]
    assert p["cause"] == "OuterChainError: the decisive final line"


def test_render_group_blocks_dagster_api_and_critical_cards():
    g = group("CNemo", [f"{NS}/adam-etl"], count="3")
    ev = {
        "serviceContext": {"service": f"{NS}/adam-etl", "version": "v-abc123"},
        "message": "TimeoutError in slow_step:\nsome context\nTimeoutError: BigQuery job exceeded 900s\n-- run_id: deadbeef · step: slow_step",
    }
    fallback, blocks = render_group_blocks(g, ev, NS, f"{NS}/adam-etl")
    text = json.dumps(blocks)
    assert "TimeoutError in slow_step" in text and "adam-etl" in fallback
    assert f"[dev \\u00b7 {NS}]" in text
    assert "> TimeoutError: BigQuery job exceeded 900s" in text
    assert "*Occurrences*\\n3 \\u00b7" in text and "`v-abc123`" in text
    assert "<http" not in text and "errors/detail/CNemo" in _buttons(blocks)[0]["url"]

    ev = {"message": "RuntimeError at GET /api/_error_probe/:\nboom\n-- request: GET /api/_error_probe/ · status: 500"}
    _, blocks = render_group_blocks(group("CApi1", [f"{NS}/adam-api"]), ev, NS, f"{NS}/adam-api")
    fields = [b for b in blocks if b.get("fields")][0]["fields"]
    assert any(f["text"] == "*Request*\n`GET /api/_error_probe/` → HTTP 500" for f in fields)

    ev = {"message": "ShardPublishError in adam.uncaught:\nboom\n-- severity: critical"}
    fallback, blocks = render_group_blocks(g, ev, NS, f"{NS}/adam-etl")
    assert blocks[0]["text"]["text"].startswith("🔥") and fallback.startswith("New CRITICAL error")


# ===== Renderer: delivery semantics =====


def test_first_error_after_empty_bootstrap_is_announced(monkeypatch):
    groups, posts = [], []
    monkeypatch.setattr(
        renderer, "_er_get",
        lambda path, params: {"errorGroupStats": groups} if path == "groupStats" else {"errorEvents": [event()]},
    )
    monkeypatch.setattr(renderer, "post_message", lambda *args: posts.append(args) or True)
    cursor = tick()
    groups.append(group())
    cursor = tick(cursor)
    tick(cursor)
    assert len(posts) == 1, "Empty successful bootstrap must not swallow the first real error"


@pytest.mark.parametrize("failure", ["slack_false", "event_fetch_exception"])
def test_failed_delivery_remains_pending_for_next_tick(monkeypatch, failure):
    posts = []
    broken = True

    def get(path, params):
        if path == "groupStats":
            return {"errorGroupStats": [group()]}
        if failure == "event_fetch_exception" and broken:
            raise TimeoutError("simulated ER outage")
        return {"errorEvents": [event()]}

    def post(*args):
        posts.append(args)
        return not broken

    monkeypatch.setattr(renderer, "_er_get", get)
    monkeypatch.setattr(renderer, "post_message", post)
    cursor = tick(json.dumps({"announced": {"old": 1}}))
    broken = False
    tick(cursor)
    assert len(posts) == (2 if failure == "slack_false" else 1), "Delivery must retry after recovery"


def test_failed_initial_fetch_does_not_initialize(monkeypatch):
    posts = []
    calls = {"n": 0}

    def get(path, params):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("ER down during bootstrap")
        return {"errorGroupStats": [group("preexisting")]} if path == "groupStats" else {"errorEvents": [event()]}

    monkeypatch.setattr(renderer, "_er_get", get)
    monkeypatch.setattr(renderer, "post_message", lambda *a: posts.append(a) or True)
    cursor = tick()
    cursor = tick(cursor)
    tick(cursor)
    assert posts == [], "a group that predates the first complete snapshot is adopted silently"


def test_renderer_fetches_next_page_for_new_low_count_groups(monkeypatch):
    posts = []

    def get(path, params):
        if path == "events":
            return {"errorEvents": [event()]}
        if params.get("pageToken") == "page-2":
            return {"errorGroupStats": [group("new-on-page-2")]}
        return {"errorGroupStats": [group(f"old-{i}") for i in range(100)], "nextPageToken": "page-2"}

    monkeypatch.setattr(renderer, "_er_get", get)
    monkeypatch.setattr(renderer, "post_message", lambda *a: posts.append(a) or True)
    tick(json.dumps({"announced": {f"old-{i}": 1 for i in range(100)}}))
    assert len(posts) == 1, "New group starved behind the default COUNT_DESC page"


def test_mixed_environment_group_does_not_post_preview_sample_as_production(monkeypatch):
    posts = []

    def get(path, params):
        if path == "groupStats":
            return {"errorGroupStats": [group(services=["production/adam-api", "preview/adam-api"])]}
        selected = params.get("serviceFilter.service") or "preview/adam-api"
        e = event(selected)
        e["message"] += "\n" + selected + " exception details"
        return {"errorEvents": [e]}

    monkeypatch.setattr(renderer, "_er_get", get)
    monkeypatch.setattr(renderer, "post_message", lambda *a: posts.append(a) or True)
    tick(json.dumps({"announced": {"old": 1}}))
    assert posts and "preview/adam-api exception details" not in str(posts)


def test_renderer_has_per_tick_backpressure(monkeypatch):
    posts = []
    monkeypatch.setattr(
        renderer, "_er_get",
        lambda path, params: {"errorGroupStats": [group(f"new-{i}") for i in range(30)]}
        if path == "groupStats"
        else {"errorEvents": [event()]},
    )
    monkeypatch.setattr(renderer, "post_message", lambda *a: posts.append(a) or True)
    cursor = tick(json.dumps({"announced": {"old": 1}}))
    assert len(posts) <= 10, "Thirty new groups must not post in one burst"
    for _ in range(3):
        cursor = tick(cursor)
    assert len(posts) == 30, "overflow drains on later ticks, not dropped"


# ===== Real Dagster: handler event path, extraction, digest sweep =====


def test_actual_dagster_step_failure_keeps_exception_and_run_link(capsys):
    @dg.op
    def deliberate_failure():
        raise ValueError("offline intentional integration failure")

    @dg.job
    def review_job():
        deliberate_failure()

    handler = {"class": "adam_dagster_shared.alerting.er_log_handler.ErrorReportingHandler", "level": "ERROR"}
    settings = {"python_logs": {"dagster_handler_config": {"handlers": {"er": handler}}}}
    with dg.DagsterInstance.ephemeral(settings=settings) as instance:
        result = review_job.execute_in_process(instance=instance, raise_on_error=False)
        from adam_dagster_shared.alerting.digest import build_digest_summary
        from adam_dagster_shared.alerting.extract import extract_failure_context

        failure = extract_failure_context(instance, instance.get_run_by_id(result.run_id))
        assert failure.step_failures[0].cls_chain[-1] == "ValueError"
        assert build_digest_summary(instance, dt.datetime.now(dt.timezone.utc))["total_failures"] == 1
    entries = [
        json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith('{"@type"')
    ]
    assert not result.success
    # One entry: the run-level "Steps failed: [...]" record is skipped as redundant.
    assert len(entries) == 1
    message = entries[0]["message"]
    assert message.startswith("ValueError in deliberate_failure:")
    assert "offline intentional integration failure" in message
    assert "Stack Trace:" in message and "Traceback (most recent call last):" not in message
    assert result.run_id in message
    assert entries[0]["context"]["reportLocation"]["functionName"] == "deliberate_failure"
