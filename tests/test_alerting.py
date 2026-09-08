"""Tests for adam_dagster_shared.alerting.

Fixtures are verbatim (or lightly trimmed) failure payloads captured from the
production event log during the 2026-07-29..08-28 measurement window, so the
classifier is exercised against the shapes it will actually see.
"""

import json
import time

from adam_dagster_shared.alerting import classify as classify_mod
from adam_dagster_shared.alerting import digest as digest_mod
from adam_dagster_shared.alerting import sensors as sensors_mod
from adam_dagster_shared.alerting import slack as slack_mod
from adam_dagster_shared.alerting.classify import FailureContext, StepFailure, Tier, classify
from adam_dagster_shared.alerting.signatures import fingerprint, normalize_message

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


def _ctx(tags=None, step_failures=None, run_failure=None, job="__ASSET_JOB"):
    return FailureContext(
        run_id="abc123",
        job_name=job,
        tags=tags or {},
        step_failures=step_failures or [],
        run_failure=run_failure,
    )


# ===== Normalization / fingerprints =====


def test_normalize_strips_varying_tokens():
    a = normalize_message("RuntimeError: duplicate gate failed for 2015-02-12: 534 rows")
    b = normalize_message("RuntimeError: duplicate gate failed for 2019-07-30: 12 rows")
    assert a == b
    assert "<date>" in a and "<n>" in a


def test_normalize_strips_k8s_job_hashes():
    a = normalize_message(K8S_DEATH_MSG)
    b = normalize_message(K8S_DEATH_MSG.replace("0bc35374201b422f01472fe0ed958ef7", "7862c4ad26c1"))
    assert a == b


def test_fingerprint_stable_across_partitions_but_not_steps():
    fp1 = fingerprint("__ASSET_JOB", "shards", "RuntimeError", DUPLICATE_GATE_MSG)
    fp2 = fingerprint(
        "__ASSET_JOB", "shards", "RuntimeError",
        DUPLICATE_GATE_MSG.replace("2015-02-12", "2016-01-01").replace("534", "9"),
    )
    fp3 = fingerprint("__ASSET_JOB", "other_step", "RuntimeError", DUPLICATE_GATE_MSG)
    assert fp1 == fp2
    assert fp1 != fp3


# ===== Classification =====


def test_k8s_job_death_is_digest_tier():
    ctx = _ctx(
        tags={"dagster/backfill": "abc"},
        step_failures=[StepFailure("aims_observation_index_shards", [""], [K8S_DEATH_MSG])],
    )
    v = classify(ctx)
    assert v.tier is Tier.DIGEST
    assert v.klass == "k8s-job-death"


def test_retry_exhausted_code_error_is_notify_with_real_exception():
    ctx = _ctx(
        tags={"dagster/auto_materialize": "true"},
        step_failures=[
            StepFailure(
                "aims_observation_index_shards",
                ["RetryRequestedFromPolicy", "RuntimeError"],
                ["Exceeded max_retries of 0", DUPLICATE_GATE_MSG],
            )
        ],
    )
    v = classify(ctx)
    assert v.tier is Tier.NOTIFY
    assert v.klass == "code-error"
    assert v.exception is not None and "RuntimeError" in v.exception
    assert "duplicate gate" in v.exception


def test_external_user_failure_is_user_facing():
    ctx = _ctx(
        tags={"external-user": "someone@example.org"},
        step_failures=[StepFailure("run_precovery", ["SystemExit"], ["SystemExit: 15"])],
        job="run_parameterized_precovery",
    )
    v = classify(ctx)
    assert v.tier is Tier.USER_FACING


def test_quality_gate_failure_is_notify():
    ctx = _ctx(
        tags={"dagster/schedule_name": "recon"},
        step_failures=[StepFailure("reconcile", ["Failure"], [RECONCILIATION_MSG])],
        job="unified_aims_mpc_reconciliation_daily_job",
    )
    v = classify(ctx)
    assert v.tier is Tier.NOTIFY
    assert v.klass == "quality-gate"


def test_crash_resume_artifact_is_digest():
    ctx = _ctx(run_failure=("CheckError", CRASH_RESUME_MSG))
    v = classify(ctx)
    assert v.tier is Tier.DIGEST
    assert v.klass == "run-worker-crash"


def test_empty_context_fails_open_to_notify():
    v = classify(_ctx())
    assert v.tier is Tier.NOTIFY
    assert v.klass == "unknown"


def test_system_oom_is_page_but_user_oom_is_user_facing():
    oom = StepFailure("step", ["Exception"], ["container OOMKilled by node"])
    assert classify(_ctx(step_failures=[oom])).tier is Tier.PAGE
    assert (
        classify(_ctx(tags={"external-user": "u@e.org"}, step_failures=[oom])).tier
        is Tier.USER_FACING
    )


# ===== Cooldown / escalation / breaker =====


def test_digest_blocks_render_er_cross_surface_section():
    summary = {
        "date_label": "Wed Sep 02",
        "total_failures": 3,
        "canceled": 0,
        "by_class": [("code-error", 3)],
        "user_failures": 0,
        "user_count": 0,
        "top_signatures": [],
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
    assert "7 active error groups in 24h · 2 new" in text
    assert "×12 — `ZtfFetchTimeout in adam_jobs.cutout_worker.executor` (cutout-worker-ztf)" in text
    assert "errors/detail/Cg2" in text
    # Absent ER data renders without the section, never raises.
    summary["er"] = None
    _, blocks2 = slack_mod.digest_blocks(summary, "production")
    assert "active error group" not in str(blocks2)


def test_er_handler_critical_severity_rides_in_footer(capsys):
    import logging as _logging

    from adam_dagster_shared.alerting.er_log_handler import ErrorReportingHandler

    record = _logging.LogRecord(
        name="x", level=_logging.CRITICAL, pathname=__file__, lineno=1,
        msg="catastrophic thing", args=(), exc_info=None,
    )
    ErrorReportingHandler().emit(record)
    entry = json.loads(capsys.readouterr().out.strip())
    assert entry["severity"] == "CRITICAL"
    assert "severity: critical" in entry["message"].splitlines()[-1]


def test_no_mrkdwn_links_anywhere_links_are_buttons():
    # Slack ignores unfurl_links=false for links inside blocks (measured:
    # every console link grew a "Google Cloud Platform" preview card). URL
    # buttons never unfurl, so no rendered text may contain "<http".
    from adam_dagster_shared.alerting.renderer import render_group_blocks

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
        "top_signatures": [("aabb", 1, "x")], "baseline_median_per_day": None,
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


def test_digest_blocks_render_counts():
    summary = {
        "date_label": "Tue Aug 26",
        "total_failures": 155,
        "canceled": 217,
        "by_class": [("code-error", 90), ("k8s-job-death", 38)],
        "user_failures": 2,
        "user_count": 2,
        "top_signatures": [("aabb", 41, "__ASSET_JOB: RuntimeError: duplicate gate")],
    }
    fallback, blocks = slack_mod.digest_blocks(summary, "production")
    text = str(blocks)
    assert "155" in text and "217" in text
    assert "duplicate gate" in text
    assert "155" in fallback


# ===== Watchdog helpers =====


def test_hung_threshold_uses_p95_with_floor():
    # Measured: run_parameterized_precovery p95 = 63 min => threshold ~3.15h.
    assert sensors_mod.hung_threshold(63 * 60) == 3 * 63 * 60
    # Thin history: 30-min p95 floors at 2h.
    assert sensors_mod.hung_threshold(30 * 60) == 2 * 3600
    assert sensors_mod.hung_threshold(None) == 6 * 3600


def test_p95_of_small_samples():
    assert sensors_mod.p95_seconds([]) is None
    assert sensors_mod.p95_seconds([60.0]) == 60.0
    assert sensors_mod.p95_seconds(list(map(float, range(1, 101)))) == 96.0


# ===== Dry-run guard =====


def test_dry_run_forced_without_token_secret(monkeypatch):
    monkeypatch.delenv("ALERTING_SLACK_TOKEN_SECRET", raising=False)
    monkeypatch.delenv("ALERTING_DRY_RUN", raising=False)
    assert slack_mod.dry_run() is True
    monkeypatch.setenv("ALERTING_SLACK_TOKEN_SECRET", "projects/p/secrets/s/versions/latest")
    assert slack_mod.dry_run() is False
    monkeypatch.setenv("ALERTING_DRY_RUN", "true")
    assert slack_mod.dry_run() is True


def test_dry_run_post_logs_payload(monkeypatch):
    monkeypatch.delenv("ALERTING_SLACK_TOKEN_SECRET", raising=False)
    monkeypatch.delenv("ALERTING_DRY_RUN", raising=False)

    class _Log:
        def __init__(self):
            self.lines = []

        def info(self, msg):
            self.lines.append(msg)

        def warning(self, msg):
            self.lines.append("WARN " + msg)

    log = _Log()
    ok = slack_mod.post_message(log, "fallback", [{"type": "section"}])
    assert ok is True
    assert any(line.startswith("ALERTING_DRY_RUN_POST ") for line in log.lines)


# ===== Error Reporting log handler =====


def test_er_handler_keeps_dagster_stack_header_unparsed():
    # Deliberate: a parsed stack made ER group ALL op errors together (the
    # top frames are shared dagster/client machinery — measured live when a
    # NotFound in mpc_obs_identity and one in unified_aims_source_mirror
    # merged into one group). Keeping "Stack Trace:" unparsable forces ER's
    # token+functionName grouping, which is per (exception class, step).
    from adam_dagster_shared.alerting.er_log_handler import _innermost_cls

    class _Err:
        cls_name = "google.api_core.exceptions.NotFound"
        cause = None

    assert _innermost_cls(_Err()) == "NotFound"


def test_er_handler_emits_single_json_line(capsys):
    import logging as _logging

    from adam_dagster_shared.alerting.er_log_handler import ErrorReportingHandler

    h = ErrorReportingHandler()
    rec = _logging.LogRecord("dagster", _logging.ERROR, "x.py", 1,
                             "step failed\nStack Trace:\n  File \"a.py\", line 1, in f",
                             None, None)
    h.emit(rec)
    out = capsys.readouterr().out
    import json as _json

    lines = [l for l in out.splitlines() if l.strip()]
    assert len(lines) == 1
    entry = _json.loads(lines[0])
    assert entry["severity"] == "ERROR"
    assert "Stack Trace:" in entry["message"]  # kept unparsable on purpose
    assert "service" in entry["serviceContext"]


def test_er_handler_includes_exc_info_traceback(capsys):
    import logging as _logging

    from adam_dagster_shared.alerting.er_log_handler import ErrorReportingHandler

    h = ErrorReportingHandler()
    try:
        raise ValueError("kaboom")
    except ValueError:
        import sys as _sys

        rec = _logging.LogRecord("app", _logging.ERROR, "x.py", 1, "ctx", None, _sys.exc_info())
    h.emit(rec)
    entry = __import__("json").loads(capsys.readouterr().out.strip())
    assert "ValueError: kaboom" in entry["message"]
    # The header is neutralized: an ER-parseable traceback would flip the
    # group to frame-based keys (review finding R6).
    assert "Traceback (most recent call last):" not in entry["message"]
    assert "Stack trace (most recent call last):" in entry["message"]


def test_er_handler_never_raises_on_bad_record(capsys):
    import logging as _logging

    from adam_dagster_shared.alerting.er_log_handler import ErrorReportingHandler

    h = ErrorReportingHandler()

    class _Evil:
        def __str__(self):
            raise RuntimeError("unformattable")

    rec = _logging.LogRecord("app", _logging.ERROR, "x.py", 1, _Evil(), None, None)
    h.emit(rec)  # must not raise


def test_er_handler_strips_run_prefix_and_adds_footer(capsys):
    import logging as _logging

    from adam_dagster_shared.alerting.er_log_handler import ErrorReportingHandler

    h = ErrorReportingHandler()
    rec = _logging.LogRecord(
        "dagster", _logging.ERROR, "x.py", 1,
        "__ASSET_JOB - 1728932c-6614-443a-a9d6-aa958bcd107e - 1 - STEP_FAILURE - boom",
        None, None,
    )
    rec.dagster_meta = {
        "orig_message": "Execution of step \"aims_mpc_hot_archive\" failed.\nStep failed health check",
        "run_id": "1728932c-6614-443a-a9d6-aa958bcd107e",
        "step_key": "aims_mpc_hot_archive",
        "job_name": "__ASSET_JOB",
    }
    h.emit(rec)
    entry = __import__("json").loads(capsys.readouterr().out.strip())
    # Grouping-stable: first tokens come from the un-prefixed message.
    assert entry["message"].startswith("Execution of step")
    assert "1728932c" not in entry["message"].splitlines()[0]
    # Context is present but only in the footer / labels / reportLocation.
    assert "run_id: 1728932c" in entry["message"]
    assert entry["context"]["reportLocation"]["functionName"] == "aims_mpc_hot_archive"
    assert entry["@type"].endswith("ReportedErrorEvent")
    assert entry["logging.googleapis.com/labels"]["dagster_run_id"].startswith("1728932c")


def test_er_handler_prefix_regex_fallback_without_meta(capsys):
    import logging as _logging

    from adam_dagster_shared.alerting.er_log_handler import ErrorReportingHandler

    h = ErrorReportingHandler()
    rec = _logging.LogRecord(
        "dagster", _logging.ERROR, "x.py", 1,
        "__ASSET_JOB - deadbeef-dead-dead-dead-deadbeefdead - 2 - STEP_FAILURE - the failure text",
        None, None,
    )
    h.emit(rec)
    entry = __import__("json").loads(capsys.readouterr().out.strip())
    assert entry["message"].startswith("STEP_FAILURE - the failure text")


def test_er_handler_headline_leads_with_exception_class(capsys):
    import logging as _logging

    from adam_dagster_shared.alerting.er_log_handler import ErrorReportingHandler

    class _FakeError:
        cls_name = "google.api_core.exceptions.NotFound"
        cause = None

    class _Data:
        error = _FakeError()
        error_display_string = (
            'google.api_core.exceptions.NotFound: 404 Not found: Dataset x\n'
            'Stack Trace:\n  File "/code/adam_etl/dag/assets/x.py", line 9, in f\n    q()'
        )

    class _Event:
        event_specific_data = _Data()
        event_type_value = "STEP_FAILURE"

    h = ErrorReportingHandler()
    rec = _logging.LogRecord("dagster", _logging.ERROR, "x.py", 1, "ignored", None, None)
    rec.dagster_meta = {"orig_message": 'Execution of step "mpc_obs_identity" failed.',
                        "run_id": "r1", "step_key": "mpc_obs_identity", "job_name": "__ASSET_JOB"}
    rec.dagster_event = _Event()
    h.emit(rec)
    entry = __import__("json").loads(capsys.readouterr().out.strip())
    assert entry["message"].startswith("NotFound in mpc_obs_identity:")
    assert "NotFound: 404" in entry["message"]
    assert "Stack Trace:" in entry["message"]  # human-readable, ER-unparsable


def test_er_handler_skips_redundant_run_failure_summary(capsys):
    import logging as _logging

    from adam_dagster_shared.alerting.er_log_handler import ErrorReportingHandler

    class _Event:
        event_specific_data = None
        event_type_value = "PIPELINE_FAILURE"

    h = ErrorReportingHandler()
    rec = _logging.LogRecord("dagster", _logging.ERROR, "x.py", 1, "ignored", None, None)
    rec.dagster_meta = {"orig_message": 'Execution of run for "__ASSET_JOB" failed. Steps failed: [...]',
                        "run_id": "r1", "job_name": "__ASSET_JOB"}
    rec.dagster_event = _Event()
    h.emit(rec)
    assert capsys.readouterr().out.strip() == ""


# ===== ER group renderer =====


def test_renderer_parses_handler_message():
    from adam_dagster_shared.alerting.renderer import parse_reported_message

    msg = (
        "Failure in aims_frames_lsst_dp2:\n"
        'Execution of step "aims_frames_lsst_dp2" failed.\n\n'
        "dagster._core.definitions.events.Failure: requires the explicit DP2 confirmation\n"
        "Stack Trace:\n  File \"x.py\", line 1, in f\n"
        "-- run_id: f46c7b33-9f01-44ce-903a-b637c01f54a2 · job: __ASSET_JOB · step: aims_frames_lsst_dp2"
    )
    p = parse_reported_message(msg)
    assert p["headline"] == "Failure in aims_frames_lsst_dp2"
    assert p["run_id"].startswith("f46c7b33")
    assert p["step"] == "aims_frames_lsst_dp2"
    # The cause line moves to the quote; the excerpt holds the rest.
    assert "requires the explicit DP2" in p["cause"]
    assert "run_id:" not in p["excerpt"]


def test_renderer_blocks_carry_what_failed(monkeypatch):
    from adam_dagster_shared.alerting.renderer import render_group_blocks

    group = {
        "group": {"groupId": "CNemo"},
        "count": "3",
        "firstSeenTime": "2026-09-01T20:14:56Z",
        "affectedServices": [{"service": "error-reporting-alerting/adam-etl"}],
    }
    event = {"message": "Failure in aims_frames_lsst_dp2:\nbody text\n-- run_id: abc12345 · step: aims_frames_lsst_dp2"}
    fallback, blocks = render_group_blocks(group, event, "error-reporting-alerting")
    text = str(blocks)
    assert "Failure in aims_frames_lsst_dp2" in text
    assert "[dev · error-reporting-alerting]" in text
    assert "*Occurrences*\\n3 ·" in text or "*Occurrences*\n3 ·" in text
    assert "console.cloud.google.com/errors/detail/CNemo" in text
    assert "adam-etl" in fallback


def test_watchdog_flags_and_records_independent_of_posting(monkeypatch):
    # Found live 2026-09-02: the posting gate sat inside the detection
    # condition, so verdict-only mode recorded nothing for an 18h-stuck run.
    from adam_dagster_shared.alerting.sensors import watchdog_should_flag

    monkeypatch.setenv("ALERTING_SENSOR_POSTING", "false")
    state = {"alerted": {}}
    now = 1_000_000.0
    assert watchdog_should_flag(state, "run-1", age=8 * 3600, threshold=2 * 3600, now=now)
    assert state["alerted"]["run-1"] == now
    # Within the re-alert window: no second flag.
    assert not watchdog_should_flag(state, "run-1", age=9 * 3600, threshold=2 * 3600, now=now + 600)
    # After the window: flags again.
    assert watchdog_should_flag(
        state, "run-1", age=33 * 3600, threshold=2 * 3600, now=now + 25 * 3600
    )


def test_watchdog_does_not_flag_below_threshold():
    from adam_dagster_shared.alerting.sensors import watchdog_should_flag

    state = {"alerted": {}}
    assert not watchdog_should_flag(state, "run-2", age=3600, threshold=2 * 3600, now=1.0)
    assert state["alerted"] == {}


def test_renderer_parses_api_footer_generically():
    from adam_dagster_shared.alerting.renderer import parse_reported_message

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


def test_renderer_extracts_cause_and_strips_boilerplate():
    from adam_dagster_shared.alerting.renderer import parse_reported_message

    msg = (
        "NotFound in barrage_unified_mirror:\n"
        'Execution of step "barrage_unified_mirror" failed.\n\n'
        "google.api_core.exceptions.NotFound: 404 Not found: Dataset x was not found\n"
        "Stack Trace:\n  File \"a.py\", line 1, in f\n"
        "-- run_id: abc · job: __ASSET_JOB · step: barrage_unified_mirror"
    )
    p = parse_reported_message(msg)
    assert p["cause"] == "google.api_core.exceptions.NotFound: 404 Not found: Dataset x was not found"
    assert "Execution of step" not in p["excerpt"]
    # Cause deduplicated out of the excerpt; the stack remains.
    assert p["excerpt"].startswith("Stack Trace:")


def test_renderer_cause_matches_arbitrary_class_names_and_dedups():
    from adam_dagster_shared.alerting.renderer import parse_reported_message

    msg = (
        "ShardManifestDivergence in adam_jobs.uncaught:\n"
        "ShardManifestDivergence: final shard publish lacks current commit markers\n"
        "-- severity: critical"
    )
    p = parse_reported_message(msg)
    assert p["cause"] == "ShardManifestDivergence: final shard publish lacks current commit markers"
    # Cause was the excerpt's only line — deduplicated away.
    assert p["excerpt"] == ""
    # Lowercase key:value lines and prose with pre-colon spaces never match.
    p2 = parse_reported_message("x:\nurl: https://a.example/b\nSteps failed: [1, 2]\n")
    assert p2["cause"] == ""


def test_renderer_long_bodies_keep_head_and_tail():
    from adam_dagster_shared.alerting.renderer import parse_reported_message

    inner = "ValueError: inner cause line\n" + ("  File \"mid.py\", line 9, in f\n" * 80)
    final = "OuterChainError: the decisive final line"
    p = parse_reported_message(f"OuterChainError in step_x:\n{inner}{final}\n")
    assert "⋯" in p["excerpt"]
    assert "the decisive final line" in p["excerpt"]
    assert p["cause"] == final


def test_renderer_blocks_quote_cause_and_show_version():
    from adam_dagster_shared.alerting.renderer import render_group_blocks

    group = {
        "group": {"groupId": "CVer"},
        "count": "2",
        "firstSeenTime": "2026-09-02T16:00:00Z",
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
    assert "> TimeoutError: BigQuery job exceeded 900s" in text
    assert "`v-abc123`" in text  # Build field


def test_renderer_blocks_critical_severity():
    from adam_dagster_shared.alerting.renderer import render_group_blocks

    group = {
        "group": {"groupId": "CCrit"},
        "count": "1",
        "firstSeenTime": "2026-09-02T16:00:00Z",
        "affectedServices": [{"service": "error-reporting-alerting/precovery-v2-shard"}],
    }
    event = {
        "message": (
            "ShardPublishError in adam_jobs.uncaught:\nboom\n-- severity: critical"
        )
    }
    fallback, blocks = render_group_blocks(group, event, "error-reporting-alerting")
    text = str(blocks)
    assert "🔥" in text and "New CRITICAL error" in text
    assert "New CRITICAL error" in fallback


def test_renderer_blocks_carry_api_request_context():
    from adam_dagster_shared.alerting.renderer import render_group_blocks

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
    assert "adam-api" in text
    assert "adam-api" in fallback
    # Facts render as a two-column fields grid.
    fields_blocks = [b for b in blocks if b.get("fields")]
    assert fields_blocks and any("*Request*" in f["text"] for f in fields_blocks[0]["fields"])
