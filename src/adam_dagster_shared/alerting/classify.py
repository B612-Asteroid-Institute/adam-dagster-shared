"""Pure failure classification: FailureContext -> Verdict.

The rule set is seeded from the 2026-07-29..08-28 production measurement
(1,225 FAILURE runs; see cloud_errors_2/INVENTORY_2026-08-28.md): every rule
below matched a real observed signature class. Rules are pure functions over
already-extracted data so they unit-test against captured payloads without a
Dagster instance. Anything unmatched fails open to NOTIFY/unknown — the
classifier must never be the reason an alert goes missing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .signatures import fingerprint


class Tier(Enum):
    PAGE = "PAGE"
    NOTIFY = "NOTIFY"
    DIGEST = "DIGEST"
    USER_FACING = "USER_FACING"


@dataclass
class StepFailure:
    step_key: str
    # Outermost-to-innermost exception class names and messages, as walked
    # from SerializableErrorInfo.cause chains.
    cls_chain: list[str] = field(default_factory=list)
    msg_chain: list[str] = field(default_factory=list)

    @property
    def innermost_cls(self) -> str | None:
        return self.cls_chain[-1] if self.cls_chain else None

    @property
    def innermost_msg(self) -> str:
        return self.msg_chain[-1] if self.msg_chain else ""


@dataclass
class FailureContext:
    run_id: str
    job_name: str
    tags: dict[str, str]
    step_failures: list[StepFailure] = field(default_factory=list)
    # (cls_name, message) from a RUN_FAILURE event when no step failure exists.
    run_failure: tuple[str | None, str] | None = None


@dataclass
class Verdict:
    tier: Tier
    klass: str  # short kebab-case class label
    reason: str  # one-line human explanation
    signature: str  # fingerprint for cooldown/dedup/recurrence
    step_key: str | None = None
    exception: str | None = None  # "Cls: first message line" for display


def _origin(tags: dict[str, str]) -> str:
    if "external-user" in tags:
        return "user-api"
    if "dagster/backfill" in tags:
        return "backfill"
    if tags.get("dagster/auto_materialize") or tags.get("dagster/from_automation_condition"):
        return "automation"
    if "dagster/schedule_name" in tags:
        return "schedule"
    if "dagster/sensor_name" in tags:
        return "sensor"
    return "manual"


# Retry wrappers carry no identity of their own; classification looks through
# them to the real exception underneath.
_RETRY_WRAPPERS = {"RetryRequestedFromPolicy", "RetryRequested"}
# Infra-death phrasing from dagster-k8s health checks (348/1225 measured runs).
_K8S_DEATH_MARKERS = ("failed health check", "discovered failed kubernetes job")
_OOM_MARKERS = ("oomkilled", "out of memory")
# Run-level artifacts of the crash-resume path (149/1225 measured runs).
_CRASH_RESUME_MARKERS = ("attempted to mark step", "dagsterk8sunrecoverableapierror")


def _real_exception(sf: StepFailure) -> tuple[str | None, str]:
    """Innermost (cls, msg), looking through retry-policy wrappers."""
    pairs = list(zip(sf.cls_chain, sf.msg_chain))
    for cls, msg in reversed(pairs):
        if cls not in _RETRY_WRAPPERS:
            return cls, msg
    return sf.innermost_cls, sf.innermost_msg


def _display(cls: str | None, msg: str) -> str:
    first = (msg or "").strip().splitlines()[0] if (msg or "").strip() else ""
    return f"{cls}: {first}"[:400] if cls else first[:400]


def classify(ctx: FailureContext) -> Verdict:
    origin = _origin(ctx.tags)
    all_msgs = " | ".join(
        m for sf in ctx.step_failures for m in sf.msg_chain
    ).lower()

    # Rule: the user's own failure. Routed to the digest count (adam-api's
    # existing per-job Slack path owns the user-visible message); never posted
    # to the infra channel individually.
    if origin == "user-api":
        sf = ctx.step_failures[0] if ctx.step_failures else None
        cls, msg = _real_exception(sf) if sf else (None, "")
        return Verdict(
            tier=Tier.USER_FACING,
            klass="user-job-failure",
            reason=f"external-user run failed ({ctx.tags.get('external-user', '?')})",
            signature=fingerprint(ctx.job_name, sf.step_key if sf else None, cls, msg),
            step_key=sf.step_key if sf else None,
            exception=_display(cls, msg),
        )

    # Rule: k8s job death (spot preemption / eviction / node reclaim). No
    # exception chain exists — the pod is simply gone. Self-heals via retry
    # policies and automation re-requests; digest-tier unless rates spike
    # (rate excursions are the digest's job to surface).
    if any(marker in all_msgs for marker in _K8S_DEATH_MARKERS):
        sf = ctx.step_failures[0]
        return Verdict(
            tier=Tier.DIGEST,
            klass="k8s-job-death",
            reason="step pod died (spot/eviction/reclaim), retries exhausted",
            signature=fingerprint(ctx.job_name, sf.step_key, "K8sJobDeath", ""),
            step_key=sf.step_key,
            exception=_display(None, sf.innermost_msg),
        )

    # Rule: system OOM (user OOM is caught by the user-api rule above).
    if any(marker in all_msgs for marker in _OOM_MARKERS):
        sf = ctx.step_failures[0] if ctx.step_failures else None
        cls, msg = _real_exception(sf) if sf else (None, "oom")
        return Verdict(
            tier=Tier.PAGE,
            klass="system-oom",
            reason="OOM kill on a system workload (rare by design: requests-only, ~2x headroom)",
            signature=fingerprint(ctx.job_name, sf.step_key if sf else None, "OOM", msg),
            step_key=sf.step_key if sf else None,
            exception=_display(cls, msg),
        )

    # Rule: interrupted mid-execution (evictions/terminations surfacing as
    # DagsterExecutionInterruptedError, 38/1225 measured). Infra class.
    if any("DagsterExecutionInterruptedError" in sf.cls_chain for sf in ctx.step_failures):
        sf = next(s for s in ctx.step_failures if "DagsterExecutionInterruptedError" in s.cls_chain)
        return Verdict(
            tier=Tier.DIGEST,
            klass="interrupted",
            reason="step interrupted (eviction/termination)",
            signature=fingerprint(ctx.job_name, sf.step_key, "Interrupted", ""),
            step_key=sf.step_key,
            exception=_display(*_real_exception(sf)),
        )

    # Rule: deliberate data-quality gates. `dagster.Failure` raised on purpose
    # (e.g. "Unified AIMS+MPC reconciliation mismatch detected") — these are
    # designed alarms and today surface nowhere.
    for sf in ctx.step_failures:
        cls, msg = _real_exception(sf)
        if cls == "Failure":
            return Verdict(
                tier=Tier.NOTIFY,
                klass="quality-gate",
                reason="a deliberate data-quality gate fired",
                signature=fingerprint(ctx.job_name, sf.step_key, cls, msg),
                step_key=sf.step_key,
                exception=_display(cls, msg),
            )

    # Rule: real code/data exception (610/1225 measured, mostly wrapped in
    # RetryRequestedFromPolicy after exhausting the 3x policy). NOTIFY on a
    # new signature; the sensor escalates to PAGE when one signature clusters.
    if ctx.step_failures:
        sf = ctx.step_failures[0]
        cls, msg = _real_exception(sf)
        return Verdict(
            tier=Tier.NOTIFY,
            klass="code-error",
            reason="step raised and exhausted retries" if set(sf.cls_chain) & _RETRY_WRAPPERS
            else "step raised",
            signature=fingerprint(ctx.job_name, sf.step_key, cls, msg),
            step_key=sf.step_key,
            exception=_display(cls, msg),
        )

    # Rule: run-level failure with no step failure — crash-resume artifacts
    # and k8s API errors from the run worker itself.
    if ctx.run_failure is not None:
        cls, msg = ctx.run_failure
        blob = f"{cls or ''} {msg}".lower()
        if any(marker in blob for marker in _CRASH_RESUME_MARKERS):
            return Verdict(
                tier=Tier.DIGEST,
                klass="run-worker-crash",
                reason="run worker crash/resume artifact",
                signature=fingerprint(ctx.job_name, None, cls, msg),
                exception=_display(cls, msg),
            )
        return Verdict(
            tier=Tier.NOTIFY,
            klass="run-failure",
            reason="run failed without a step failure",
            signature=fingerprint(ctx.job_name, None, cls, msg),
            exception=_display(cls, msg),
        )

    # Fail open: no events captured at all. Never drop it silently.
    return Verdict(
        tier=Tier.NOTIFY,
        klass="unknown",
        reason="no failure events could be extracted (fail-open)",
        signature=fingerprint(ctx.job_name, None, None, ""),
    )


def origin_label(tags: dict[str, str]) -> str:
    """Public accessor used by message rendering and the digest."""
    return _origin(tags)
