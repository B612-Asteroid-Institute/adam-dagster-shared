"""Reliability regression suite, adapted from the 2026-09-08 independent review.

Each test encodes a delivery/orchestration behavior the review demonstrated
was missing; together they are the acceptance bar for the renderer's
initialization, pending-delivery, pagination, scoping, and budget semantics,
plus reporter cursor boundaries and watchdog pagination. External ER/Slack
traffic is stubbed at module boundaries; the last three tests exercise a real
ephemeral Dagster instance, and the final one is the positive control for
the Dagster log handler's event path (handcrafted-record tests live in
test_alerting.py).
"""

import datetime as dt
import json
from types import SimpleNamespace

import dagster as dg
import pytest

from adam_dagster_shared.alerting import renderer, sensors


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("GARDEN_NAMESPACE", "production")
    monkeypatch.setenv("ER_LOGGING_ENABLED", "true")
    monkeypatch.setenv("ALERTING_DRY_RUN", "true")
    monkeypatch.setattr(renderer, "current_namespace", lambda: "production")
    monkeypatch.setattr(sensors, "current_namespace", lambda: "production")

    def unexpected(*a, **kw):
        raise AssertionError("Unexpected external call")

    monkeypatch.setattr(renderer, "_er_get", unexpected)
    monkeypatch.setattr(renderer, "post_message", unexpected)
    monkeypatch.setattr(sensors, "post_message", unexpected)


def group(gid="new", services=None):
    return {
        "group": {"groupId": gid},
        "count": "1",
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


def test_first_error_after_empty_bootstrap_is_announced(monkeypatch):
    groups = []
    posts = []
    monkeypatch.setattr(
        renderer,
        "_er_get",
        lambda path, params: {"errorGroupStats": groups}
        if path == "groupStats"
        else {"errorEvents": [event()]},
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
        if path == "groupStats":
            return {"errorGroupStats": [group("preexisting")]}
        return {"errorEvents": [event()]}

    monkeypatch.setattr(renderer, "_er_get", get)
    monkeypatch.setattr(renderer, "post_message", lambda *a: posts.append(a) or True)
    cursor = tick()
    cursor = tick(cursor)
    tick(cursor)
    # The group existed before we ever completed a snapshot: adopted, silent.
    assert posts == []


def test_renderer_fetches_next_page_for_new_low_count_groups(monkeypatch):
    posts = []

    def get(path, params):
        if path == "events":
            return {"errorEvents": [event()]}
        if params.get("pageToken") == "page-2":
            return {"errorGroupStats": [group("new-on-page-2")]}
        return {
            "errorGroupStats": [group(f"old-{i}") for i in range(100)],
            "nextPageToken": "page-2",
        }

    monkeypatch.setattr(renderer, "_er_get", get)
    monkeypatch.setattr(renderer, "post_message", lambda *a: posts.append(a) or True)
    tick(json.dumps({"announced": {f"old-{i}": 1 for i in range(100)}}))
    assert len(posts) == 1, "New group starved behind the default COUNT_DESC page"


def test_mixed_environment_group_does_not_post_preview_sample_as_production(monkeypatch):
    posts = []

    def get(path, params):
        if path == "groupStats":
            return {
                "errorGroupStats": [group(services=["production/adam-api", "preview/adam-api"])]
            }
        selected = (
            "production/adam-api"
            if params.get("serviceFilter.service") == "production/adam-api"
            else "preview/adam-api"
        )
        e = event(selected)
        e["message"] += "\n" + selected + " exception details"
        return {"errorEvents": [e]}

    monkeypatch.setattr(renderer, "_er_get", get)
    monkeypatch.setattr(renderer, "post_message", lambda *a: posts.append(a) or True)
    tick(json.dumps({"announced": {"old": 1}}))
    assert "preview/adam-api exception details" not in str(posts)


def test_renderer_has_per_tick_backpressure(monkeypatch):
    posts = []
    monkeypatch.setattr(
        renderer,
        "_er_get",
        lambda path, params: {"errorGroupStats": [group(f"new-{i}") for i in range(30)]}
        if path == "groupStats"
        else {"errorEvents": [event()]},
    )
    monkeypatch.setattr(renderer, "post_message", lambda *a: posts.append(a) or True)
    cursor = tick(json.dumps({"announced": {"old": 1}}))
    assert len(posts) <= 10, "Thirty new groups must not post in one burst"
    # Overflow is preserved, not dropped: later ticks drain it.
    for _ in range(3):
        cursor = tick(cursor)
    assert len(posts) == 30


def test_failure_reporter_consumes_all_runs_with_same_update_timestamp(monkeypatch):
    when = dt.datetime.now(dt.timezone.utc)
    records = [
        SimpleNamespace(
            dagster_run=SimpleNamespace(run_id=f"run-{i}", job_name="system", tags={}),
            update_timestamp=when,
        )
        for i in range(26)
    ]

    def get(*, filters, limit, **kw):
        # Matches Dagster SqlRunStorage's strictly greater-than updated_after.
        return [r for r in records if r.update_timestamp > filters.updated_after][:limit]

    monkeypatch.setenv("ALERTING_SENSOR_POSTING", "false")
    with dg.DagsterInstance.ephemeral() as instance:
        monkeypatch.setattr(instance, "get_run_records", get)
        from adam_dagster_shared.alerting.classify import FailureContext

        monkeypatch.setattr(
            sensors, "extract_failure_context", lambda instance, run: FailureContext(run.run_id, "system", {})
        )
        cursor = None
        for _ in range(2):
            with dg.build_sensor_context(instance=instance, cursor=cursor) as ctx:
                sensors.alerting_failure_reporter(ctx)
                cursor = ctx.cursor
    assert len(json.loads(cursor)["recent_ids"]) == 26


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

    monkeypatch.setenv("ALERTING_SENSOR_POSTING", "false")
    with dg.DagsterInstance.ephemeral() as instance:
        monkeypatch.setattr(instance, "get_run_records", get)
        with dg.build_sensor_context(instance=instance) as ctx:
            sensors.alerting_hung_run_watchdog(ctx)
            state = json.loads(ctx.cursor)
    assert "hung-oldest" in state["alerted"]


def test_actual_dagster_step_failure_keeps_exception_and_run_link(capsys):
    """Positive control: exercise Dagster itself, not a handcrafted LogRecord."""

    @dg.op
    def deliberate_failure():
        raise ValueError("offline intentional integration failure")

    @dg.job
    def review_job():
        deliberate_failure()

    with dg.DagsterInstance.ephemeral(
        settings={
            "python_logs": {
                "dagster_handler_config": {
                    "handlers": {
                        "er": {
                            "class": "adam_dagster_shared.alerting.er_log_handler.ErrorReportingHandler",
                            "level": "ERROR",
                        }
                    }
                }
            }
        }
    ) as instance:
        result = review_job.execute_in_process(instance=instance, raise_on_error=False)
        from adam_dagster_shared.alerting.digest import build_digest_summary
        from adam_dagster_shared.alerting.extract import extract_failure_context

        failure = extract_failure_context(instance, instance.get_run_by_id(result.run_id))
        assert failure.step_failures[0].innermost_cls == "ValueError"
        assert build_digest_summary(instance, dt.datetime.now(dt.timezone.utc))["total_failures"] == 1
    entries = [
        json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith('{"@type"')
    ]
    assert not result.success
    # Exactly one entry: the run-level "Steps failed: [...]" summary Dagster
    # also logs at ERROR is skipped as redundant with the STEP_FAILURE record
    # (verified by mutation: disabling the skip yields 2).
    assert len(entries) == 1
    message = entries[0]["message"]
    # Headline = (exception class, step) — the grouping key — with the
    # human-readable stack kept in Dagster's ER-unparsable "Stack Trace:" form.
    assert message.startswith("ValueError in deliberate_failure:")
    assert "offline intentional integration failure" in message
    assert "Stack Trace:" in message and "Traceback (most recent call last):" not in message
    assert result.run_id in message
    assert entries[0]["context"]["reportLocation"]["functionName"] == "deliberate_failure"
