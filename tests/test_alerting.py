"""Tests for adam_dagster_shared.alerting.

Fixtures are verbatim (or lightly trimmed) failure payloads captured from the
production event log during the 2026-07-29..08-28 measurement window, so the
classifier is exercised against the shapes it will actually see.

Delivery/orchestration semantics of the sensors (bootstrap, pending retry,
pagination, budget) live in test_review_regressions.py; the real-Dagster
integration test there also covers the Dagster handler's event path.
"""

import json
import logging
import sys

import pytest

from adam_dagster_shared.alerting import sensors as sensors_mod
from adam_dagster_shared.alerting import slack as slack_mod
from adam_dagster_shared.alerting.classify import FailureContext, StepFailure, classify
from adam_dagster_shared.alerting.er_log_handler import ErrorReportingHandler
from adam_dagster_shared.alerting.renderer import parse_reported_message, render_group_blocks
from adam_dagster_shared.alerting.signatures import fingerprint

# ===== Captured payload fragments (2026-08 production) =====

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


def _ctx(tags=None, step_failures=None, run_failure=None, job="__ASSET_JOB"):
    return FailureContext(
        run_id="abc123",
        job_name=job,
        tags=tags or {},
        step_failures=step_failures or [],
        run_failure=run_failure,
    )


def _emit(record):
    ErrorReportingHandler().emit(record)


def _entry(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 1, "the handler must emit exactly one JSON line per record"
    return json.loads(lines[0])


# ===== Signatures / classification (feed the digest's by-class breakdown) =====


def test_fingerprint_ignores_varying_tokens_but_not_step():
    same = fingerprint("__ASSET_JOB", "shards", "RuntimeError", DUPLICATE_GATE_MSG)
    # Dates, counts and k8s job hashes vary per partition; the signature must not.
    assert same == fingerprint(
        "__ASSET_JOB", "shards", "RuntimeError",
        DUPLICATE_GATE_MSG.replace("2015-02-12", "2016-01-01").replace("534", "9"),
    )
    assert fingerprint("j", "s", "K8sJobDeath", K8S_DEATH_MSG) == fingerprint(
        "j", "s", "K8sJobDeath", K8S_DEATH_MSG.replace("0bc35374201b422f01472fe0ed958ef7", "7862c4ad")
    )
    assert same != fingerprint("__ASSET_JOB", "other_step", "RuntimeError", DUPLICATE_GATE_MSG)


@pytest.mark.parametrize(
    "ctx,klass,user_facing,exception_contains",
    [
        # k8s job death: infrastructure noise, counted as its own class.
        (
            _ctx(tags={"dagster/backfill": "abc"},
                 step_failures=[StepFailure("aims_observation_index_shards", [""], [K8S_DEATH_MSG])]),
            "k8s-job-death", False, None,
        ),
        # Retry wrapper unwrapped to the real exception.
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
        # Review R12: the OOM marker wins over the generic k8s-death marker.
        (_ctx(step_failures=[StepFailure("system", ["RuntimeError"], [OOM_MSG])]),
         "system-oom", False, None),
        # External-user runs are the user's problem, whatever the failure.
        (_ctx(tags={"external-user": "u@e.org"}, step_failures=[StepFailure("s", ["Exception"], [OOM_MSG])]),
         "user-job-failure", True, None),
        # Empty context fails open rather than raising.
        (_ctx(), "unknown", False, None),
    ],
    ids=["k8s-death", "retry-unwrap", "quality-gate", "crash-resume", "oom-precedence", "external-user", "empty"],
)
def test_classify_captured_production_failures(ctx, klass, user_facing, exception_contains):
    v = classify(ctx)
    assert (v.klass, v.user_facing) == (klass, user_facing)
    if exception_contains:
        assert v.exception and "RuntimeError" in v.exception and exception_contains in v.exception


# ===== Slack digest =====


def test_digest_blocks_render_counts_and_er_section():
    summary = {
        "date_label": "Tue Aug 26",
        "total_failures": 155,
        "canceled": 217,
        "by_class": [("code-error", 90), ("k8s-job-death", 38)],
        "user_failures": 2,
        "user_count": 2,
        "top_signatures": [("aabb", 41, "__ASSET_JOB: RuntimeError: duplicate gate")],
        "er": {
            "groups": 7,
            "new_today": 2,
            "top": [
                (12, "ZtfFetchTimeout in adam_jobs.cutout_worker.executor", "cutout-worker-ztf", "Cg1"),
                (4, "OperationalError at POST /api/precovery/{id}", "adam-api", "Cg2"),
            ],
            "project": "moeyens-thor-dev",
        },
    }
    fallback, blocks = slack_mod.digest_blocks(summary, "production")
    text = str(blocks)
    assert "155" in text and "217" in text and "155" in fallback
    assert "duplicate gate" in text
    assert "7 active error groups in 24h · 2 new" in text
    assert "×12 — `ZtfFetchTimeout in adam_jobs.cutout_worker.executor` (cutout-worker-ztf)" in text
    assert "errors/detail/Cg2" in text
    # ER unavailable (fail-open): renders without the section, never raises.
    summary["er"] = None
    _, blocks2 = slack_mod.digest_blocks(summary, "production")
    assert "active error group" not in str(blocks2)


def test_no_mrkdwn_links_anywhere_links_are_buttons():
    # Slack ignores unfurl_links=false for links inside blocks (measured:
    # every console link grew a "Google Cloud Platform" preview card). URL
    # buttons never unfurl, so no rendered text may contain "<http".
    group = {
        "group": {"groupId": "CBtn"},
        "count": "2",
        "firstSeenTime": "2026-09-08T09:00:00Z",
        "affectedServices": [{"service": "error-reporting-alerting/adam-api"}],
    }
    event = {
        "serviceContext": {"service": "error-reporting-alerting/adam-api", "version": "v-1"},
        "message": "KeyError at GET /api/x:\nboom\n-- request: GET /api/x · status: 500",
    }
    _, card_blocks = render_group_blocks(group, event, "error-reporting-alerting")
    digest_summary = {
        "date_label": "Tue Sep 08", "total_failures": 1, "canceled": 0,
        "by_class": [("code-error", 1)], "user_failures": 0, "user_count": 0,
        "top_signatures": [("aabb", 1, "x")],
        "er": {"groups": 1, "new_today": 1, "project": "moeyens-thor-dev",
               "top": [(3, "KeyError at GET /api/x", "adam-api", "Cg9")]},
    }
    _, digest = slack_mod.digest_blocks(digest_summary, "production")
    for blocks in (card_blocks, digest):
        assert "<http" not in json.dumps(blocks)
        buttons = [
            el
            for b in blocks
            if b.get("type") == "actions"
            for el in b.get("elements", [])
            if el.get("type") == "button"
        ]
        assert buttons and all(el["url"].startswith("https://") for el in buttons)
    # Group deep-links survive as button urls.
    assert "errors/detail/Cg9" in json.dumps(digest)
    assert "errors/detail/CBtn" in json.dumps(card_blocks)


def test_dry_run_without_token_secret_logs_instead_of_posting(monkeypatch):
    monkeypatch.delenv("ALERTING_SLACK_TOKEN_SECRET", raising=False)
    monkeypatch.delenv("ALERTING_DRY_RUN", raising=False)
    assert slack_mod.dry_run() is True
    monkeypatch.setenv("ALERTING_SLACK_TOKEN_SECRET", "projects/p/secrets/s/versions/latest")
    assert slack_mod.dry_run() is False
    monkeypatch.setenv("ALERTING_DRY_RUN", "true")
    assert slack_mod.dry_run() is True

    class _Log:
        lines = []

        def info(self, msg):
            self.lines.append(msg)

        warning = info

    log = _Log()
    assert slack_mod.post_message(log, "fallback", [{"type": "section"}]) is True
    assert any(line.startswith("ALERTING_DRY_RUN_POST ") for line in log.lines)


# ===== Watchdog =====


def test_watchdog_threshold_from_p95_and_flagging_window(monkeypatch):
    assert sensors_mod.p95_seconds([]) is None
    assert sensors_mod.p95_seconds([60.0]) == 60.0
    assert sensors_mod.p95_seconds(list(map(float, range(1, 101)))) == 96.0
    # Measured: run_parameterized_precovery p95 = 63 min => threshold ~3.15h;
    # thin history floors at 2h; no history defaults to 6h.
    assert sensors_mod.hung_threshold(63 * 60) == 3 * 63 * 60
    assert sensors_mod.hung_threshold(30 * 60) == 2 * 3600
    assert sensors_mod.hung_threshold(None) == 6 * 3600

    # Found live 2026-09-02: the posting gate sat inside the detection
    # condition, so verdict-only mode recorded nothing for an 18h-stuck run.
    monkeypatch.setenv("ALERTING_SENSOR_POSTING", "false")
    flag = sensors_mod.watchdog_should_flag
    state = {"alerted": {}}
    now = 1_000_000.0
    assert not flag(state, "run-1", age=3600, threshold=2 * 3600, now=now) and state["alerted"] == {}
    assert flag(state, "run-1", age=8 * 3600, threshold=2 * 3600, now=now)
    assert state["alerted"]["run-1"] == now
    # Within the re-alert window: no second flag; after it: flags again.
    assert not flag(state, "run-1", age=9 * 3600, threshold=2 * 3600, now=now + 600)
    assert flag(state, "run-1", age=33 * 3600, threshold=2 * 3600, now=now + 25 * 3600)


# ===== Dagster Error Reporting log handler =====


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
    _emit(rec)
    entry = _entry(capsys)
    assert entry["@type"].endswith("ReportedErrorEvent")
    assert entry["severity"] == "ERROR"
    assert "service" in entry["serviceContext"]
    # Grouping-stable: first tokens come from the un-prefixed message.
    assert entry["message"].startswith("Execution of step")
    assert "1728932c" not in entry["message"].splitlines()[0]
    # Context is present but only in the footer / labels / reportLocation.
    assert "run_id: 1728932c" in entry["message"]
    assert entry["context"]["reportLocation"]["functionName"] == "aims_mpc_hot_archive"
    assert entry["logging.googleapis.com/labels"]["dagster_run_id"].startswith("1728932c")


def test_er_handler_neutralizes_exc_info_traceback(capsys):
    try:
        raise ValueError("kaboom")
    except ValueError:
        rec = logging.LogRecord("app", logging.ERROR, "x.py", 1, "ctx", None, sys.exc_info())
    _emit(rec)
    entry = _entry(capsys)
    assert "ValueError: kaboom" in entry["message"]
    # An ER-parseable traceback would flip the group to frame-based keys
    # (review finding R6).
    assert "Traceback (most recent call last):" not in entry["message"]
    assert "Stack trace (most recent call last):" in entry["message"]


def test_er_handler_never_raises_on_bad_record():
    class _Evil:
        def __str__(self):
            raise RuntimeError("unformattable")

    _emit(logging.LogRecord("app", logging.ERROR, "x.py", 1, _Evil(), None, None))


# ===== ER group renderer: message parsing =====


def test_renderer_parses_dagster_message_into_headline_cause_and_excerpt():
    msg = (
        "NotFound in barrage_unified_mirror:\n"
        'Execution of step "barrage_unified_mirror" failed.\n\n'
        "google.api_core.exceptions.NotFound: 404 Not found: Dataset x was not found\n"
        "Stack Trace:\n  File \"a.py\", line 1, in f\n"
        "-- run_id: f46c7b33-9f01-44ce-903a-b637c01f54a2 · job: __ASSET_JOB · step: barrage_unified_mirror"
    )
    p = parse_reported_message(msg)
    assert p["headline"] == "NotFound in barrage_unified_mirror"
    assert p["run_id"].startswith("f46c7b33")
    assert p["step"] == "barrage_unified_mirror"
    assert p["cause"] == "google.api_core.exceptions.NotFound: 404 Not found: Dataset x was not found"
    # Boilerplate and the footer are stripped; the cause is deduplicated out
    # of the excerpt so the stack is what remains.
    assert "Execution of step" not in p["excerpt"] and "run_id:" not in p["excerpt"]
    assert p["excerpt"].startswith("Stack Trace:")
    # Lowercase key:value lines and prose with pre-colon spaces are not causes.
    assert parse_reported_message("x:\nurl: https://a.example/b\nSteps failed: [1, 2]\n")["cause"] == ""


def test_renderer_parses_api_footer_generically():
    msg = (
        "RuntimeError at GET /api/jobs/{id}:\n"
        "Unhandled exception during GET /api/jobs/42: boom\n"
        "Stack trace (most recent call last):\n  File \"x.py\", line 1, in f\n"
        "-- request: GET /api/jobs/42 · status: 500 · user: kat@b612.ai · client: 10.1.2.3"
    )
    p = parse_reported_message(msg)
    assert p["headline"] == "RuntimeError at GET /api/jobs/{id}"
    assert p["meta"]["request"] == "GET /api/jobs/42"
    assert p["meta"]["status"] == "500"
    assert p["meta"]["user"] == "kat@b612.ai"
    assert p["run_id"] == "" and p["step"] == ""
    assert "client:" not in p["excerpt"]


def test_renderer_long_bodies_keep_head_and_tail():
    inner = "ValueError: inner cause line\n" + ("  File \"mid.py\", line 9, in f\n" * 80)
    final = "OuterChainError: the decisive final line"
    p = parse_reported_message(f"OuterChainError in step_x:\n{inner}{final}\n")
    assert "⋯" in p["excerpt"]
    assert "the decisive final line" in p["excerpt"]
    # The cause is the last exception line, whatever the class is called.
    assert p["cause"] == final


# ===== ER group renderer: Slack card =====


def test_renderer_dagster_card_carries_what_failed_and_critical_styling():
    group = {
        "group": {"groupId": "CNemo"},
        "count": "3",
        "firstSeenTime": "2026-09-01T20:14:56Z",
        "affectedServices": [{"service": "error-reporting-alerting/adam-etl"}],
    }
    event = {
        "serviceContext": {"service": "error-reporting-alerting/adam-etl", "version": "v-abc123"},
        "message": (
            "TimeoutError in slow_step:\nsome context\n"
            "TimeoutError: BigQuery job exceeded 900s\n"
            "-- run_id: deadbeef · step: slow_step"
        ),
    }
    fallback, blocks = render_group_blocks(group, event, "error-reporting-alerting")
    text = str(blocks)
    assert "TimeoutError in slow_step" in text
    assert "[dev · error-reporting-alerting]" in text
    assert "> TimeoutError: BigQuery job exceeded 900s" in text
    assert "*Occurrences*\\n3 ·" in text or "*Occurrences*\n3 ·" in text
    assert "`v-abc123`" in text  # Build field
    assert "console.cloud.google.com/errors/detail/CNemo" in text
    assert "adam-etl" in fallback

    # CRITICAL rides in the footer (ER events expose no severity) and
    # changes the headline styling.
    event = {"message": "ShardPublishError in adam_jobs.uncaught:\nboom\n-- severity: critical"}
    fallback, blocks = render_group_blocks(group, event, "error-reporting-alerting")
    assert "🔥" in str(blocks) and "New CRITICAL error" in str(blocks)
    assert "New CRITICAL error" in fallback


def test_renderer_api_card_carries_request_context_in_fields_grid():
    group = {
        "group": {"groupId": "CApi1"},
        "count": "1",
        "firstSeenTime": "2026-09-02T01:00:00Z",
        "affectedServices": [{"service": "error-reporting-alerting/adam-api"}],
    }
    event = {
        "message": (
            "RuntimeError at GET /api/_error_probe/:\nboom\n"
            "-- request: GET /api/_error_probe/ · status: 500"
        )
    }
    fallback, blocks = render_group_blocks(group, event, "error-reporting-alerting")
    text = str(blocks)
    assert "RuntimeError at GET /api/_error_probe/" in text
    assert "`GET /api/_error_probe/` → HTTP 500" in text
    assert "adam-api" in text and "adam-api" in fallback
    fields_blocks = [b for b in blocks if b.get("fields")]
    assert fields_blocks and any("*Request*" in f["text"] for f in fields_blocks[0]["fields"])
