"""Pure failure classification: FailureContext -> Verdict.

Rules were seeded from the 2026-08 production measurement (cloud_errors_2/
INVENTORY_2026-08-28.md); anything unmatched fails open to "unknown".
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class StepFailure:
    step_key: str
    cls_chain: list[str] = field(default_factory=list)  # outermost..innermost
    msg_chain: list[str] = field(default_factory=list)


@dataclass
class FailureContext:
    run_id: str
    job_name: str
    tags: dict[str, str]
    step_failures: list[StepFailure] = field(default_factory=list)
    run_failure: tuple[str | None, str] | None = None  # (cls, msg) from RUN_FAILURE, no step failed


@dataclass
class Verdict:
    klass: str
    user_facing: bool = False  # an external user's own run; counted separately in the digest
    step_key: str | None = None
    exception: str | None = None  # "Cls: first message line"


_RETRY_WRAPPERS = {"RetryRequestedFromPolicy", "RetryRequested"}
_K8S_DEATH_MARKERS = ("failed health check", "discovered failed kubernetes job")
_OOM_MARKERS = ("oomkilled", "out of memory")
_CRASH_RESUME_MARKERS = ("attempted to mark step", "dagsterk8sunrecoverableapierror")


def _real_exception(sf: StepFailure) -> tuple[str | None, str]:
    """Innermost (cls, msg), looking through retry-policy wrappers."""
    for cls, msg in reversed(list(zip(sf.cls_chain, sf.msg_chain))):
        if cls not in _RETRY_WRAPPERS:
            return cls, msg
    return (sf.cls_chain[-1] if sf.cls_chain else None), (sf.msg_chain[-1] if sf.msg_chain else "")


def _display(cls: str | None, msg: str) -> str:
    first = (msg or "").strip().splitlines()[0] if (msg or "").strip() else ""
    return f"{cls}: {first}"[:400] if cls else first[:400]


def _verdict(klass: str, sf: StepFailure | None, **kw) -> Verdict:
    cls, msg = _real_exception(sf) if sf else (None, "")
    return Verdict(klass, step_key=sf.step_key if sf else None, exception=_display(cls, msg), **kw)


def classify(ctx: FailureContext) -> Verdict:
    steps = ctx.step_failures
    all_msgs = " | ".join(m for sf in steps for m in sf.msg_chain).lower()

    def step_matching(markers) -> StepFailure | None:
        return next(
            (sf for sf in steps if any(m in " | ".join(sf.msg_chain).lower() for m in markers)),
            None,
        )

    if "external-user" in ctx.tags:  # the user's own failure, whatever the cause
        return _verdict("user-job-failure", steps[0] if steps else None, user_facing=True)
    # OOM before the generic k8s-death phrases: an OOM-killed pod matches both.
    if any(m in all_msgs for m in _OOM_MARKERS):
        return _verdict("system-oom", step_matching(_OOM_MARKERS) or steps[0])
    if any(m in all_msgs for m in _K8S_DEATH_MARKERS):  # eviction / preemption; no exception chain
        return _verdict("k8s-job-death", step_matching(_K8S_DEATH_MARKERS) or steps[0])
    interrupted = next((sf for sf in steps if "DagsterExecutionInterruptedError" in sf.cls_chain), None)
    if interrupted:
        return _verdict("interrupted", interrupted)
    for sf in steps:  # dagster.Failure raised on purpose = a data-quality gate
        if _real_exception(sf)[0] == "Failure":
            return _verdict("quality-gate", sf)
    if steps:
        return _verdict("code-error", steps[0])
    if ctx.run_failure is not None:  # run-level failure with no step failure
        cls, msg = ctx.run_failure
        crash = any(m in f"{cls or ''} {msg}".lower() for m in _CRASH_RESUME_MARKERS)
        return Verdict("run-worker-crash" if crash else "run-failure", exception=_display(cls, msg))
    return Verdict("unknown")
