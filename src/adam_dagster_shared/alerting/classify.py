"""Pure failure classification: FailureContext -> Verdict.

The rule set is seeded from the 2026-07-29..08-28 production measurement
(1,225 FAILURE runs; see cloud_errors_2/INVENTORY_2026-08-28.md): every rule
below matched a real observed signature class. Rules are pure functions over
already-extracted data so they unit-test against captured payloads without a
Dagster instance. Anything unmatched fails open to "unknown" — the classifier
must never be the reason a failure goes uncounted.

Verdicts feed the daily digest (per-class counts, top signatures, user-run
count) and the audit log. Individual Slack posting is Error Reporting's job.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .signatures import fingerprint


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
    klass: str  # short kebab-case class label
    signature: str  # fingerprint grouping runs of one failure class
    # An external user's own run: counted separately in the digest (adam-api's
    # per-job Slack path owns the user-visible message).
    user_facing: bool = False
    step_key: str | None = None
    exception: str | None = None  # "Cls: first message line" for display


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
    all_msgs = " | ".join(
        m for sf in ctx.step_failures for m in sf.msg_chain
    ).lower()

    # Rule: the user's own failure, whatever the cause.
    if "external-user" in ctx.tags:
        sf = ctx.step_failures[0] if ctx.step_failures else None
        cls, msg = _real_exception(sf) if sf else (None, "")
        return Verdict(
            klass="user-job-failure",
            signature=fingerprint(ctx.job_name, sf.step_key if sf else None, cls, msg),
            user_facing=True,
            step_key=sf.step_key if sf else None,
            exception=_display(cls, msg),
        )

    def _step_matching(markers) -> "StepFailure | None":
        for sf in ctx.step_failures:
            if any(m in " | ".join(sf.msg_chain).lower() for m in markers):
                return sf
        return None

    # Rule: system OOM (rare by design: requests-only, ~2x headroom). Checked
    # BEFORE the generic k8s-death markers: an OOM-killed pod also matches the
    # broad failed-job phrases, and the specific evidence must win over the
    # generic fallback (review finding R12).
    if any(marker in all_msgs for marker in _OOM_MARKERS):
        sf = _step_matching(_OOM_MARKERS) or (ctx.step_failures[0] if ctx.step_failures else None)
        cls, msg = _real_exception(sf) if sf else (None, "oom")
        return Verdict(
            klass="system-oom",
            signature=fingerprint(ctx.job_name, sf.step_key if sf else None, "OOM", msg),
            step_key=sf.step_key if sf else None,
            exception=_display(cls, msg),
        )

    # Rule: k8s job death (eviction / node reclaim / preemption — the phrase
    # alone does not establish which). No exception chain exists — the pod is
    # simply gone. Self-heals via retry policies and automation re-requests.
    if any(marker in all_msgs for marker in _K8S_DEATH_MARKERS):
        sf = _step_matching(_K8S_DEATH_MARKERS) or ctx.step_failures[0]
        return Verdict(
            klass="k8s-job-death",
            signature=fingerprint(ctx.job_name, sf.step_key, "K8sJobDeath", ""),
            step_key=sf.step_key,
            exception=_display(None, sf.innermost_msg),
        )

    # Rule: interrupted mid-execution (evictions/terminations surfacing as
    # DagsterExecutionInterruptedError, 38/1225 measured). Infra class.
    if any("DagsterExecutionInterruptedError" in sf.cls_chain for sf in ctx.step_failures):
        sf = next(s for s in ctx.step_failures if "DagsterExecutionInterruptedError" in s.cls_chain)
        return Verdict(
            klass="interrupted",
            signature=fingerprint(ctx.job_name, sf.step_key, "Interrupted", ""),
            step_key=sf.step_key,
            exception=_display(*_real_exception(sf)),
        )

    # Rule: deliberate data-quality gates. `dagster.Failure` raised on purpose
    # (e.g. "Unified AIMS+MPC reconciliation mismatch detected") — designed
    # alarms, counted as their own class.
    for sf in ctx.step_failures:
        cls, msg = _real_exception(sf)
        if cls == "Failure":
            return Verdict(
                klass="quality-gate",
                signature=fingerprint(ctx.job_name, sf.step_key, cls, msg),
                step_key=sf.step_key,
                exception=_display(cls, msg),
            )

    # Rule: real code/data exception (610/1225 measured, mostly wrapped in
    # RetryRequestedFromPolicy after exhausting the 3x policy).
    if ctx.step_failures:
        sf = ctx.step_failures[0]
        cls, msg = _real_exception(sf)
        return Verdict(
            klass="code-error",
            signature=fingerprint(ctx.job_name, sf.step_key, cls, msg),
            step_key=sf.step_key,
            exception=_display(cls, msg),
        )

    # Rule: run-level failure with no step failure — crash-resume artifacts
    # and k8s API errors from the run worker itself.
    if ctx.run_failure is not None:
        cls, msg = ctx.run_failure
        blob = f"{cls or ''} {msg}".lower()
        klass = "run-worker-crash" if any(m in blob for m in _CRASH_RESUME_MARKERS) else "run-failure"
        return Verdict(
            klass=klass,
            signature=fingerprint(ctx.job_name, None, cls, msg),
            exception=_display(cls, msg),
        )

    # Fail open: no events captured at all. Never drop it silently.
    return Verdict(klass="unknown", signature=fingerprint(ctx.job_name, None, None, ""))
