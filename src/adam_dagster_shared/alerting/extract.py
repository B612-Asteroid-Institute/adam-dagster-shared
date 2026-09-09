"""Build a FailureContext from a run's event log (instance-facing side).

Mirrors the cause-chain walk proven in adam-jobs' notify_api_on_run_failure
sensor: the innermost SerializableErrorInfo carries the real exception; outer
layers are Dagster execution wrappers. Every accessor is defensive — event
shapes vary across failure modes, and extraction problems must degrade to an
emptier context (which classify() fails open on), never to a raised error.
"""

from __future__ import annotations

from dagster import DagsterEventType

from .classify import FailureContext, StepFailure

# One run's failure events are small (a handful of steps at most); this cap
# only guards against pathological runs with hundreds of failed steps.
_MAX_STEP_FAILURES = 20


def _walk_error_chain(error) -> tuple[list[str], list[str]]:
    """SerializableErrorInfo -> ([cls outermost..innermost], [msg ...])."""
    cls_chain: list[str] = []
    msg_chain: list[str] = []
    seen = 0
    node = error
    while node is not None and seen < 10:
        cls_chain.append(getattr(node, "cls_name", None) or "")
        msg_chain.append((getattr(node, "message", None) or "").strip())
        node = getattr(node, "cause", None)
        seen += 1
    return cls_chain, msg_chain


def extract_failure_context(instance, run) -> FailureContext:
    """`run` is a DagsterRun (has run_id/job_name/tags)."""
    ctx = FailureContext(
        run_id=run.run_id,
        job_name=getattr(run, "job_name", None) or getattr(run, "pipeline_name", "") or "",
        tags=dict(run.tags or {}),
    )
    try:
        records = instance.get_records_for_run(
            run_id=run.run_id,
            of_type={DagsterEventType.STEP_FAILURE, DagsterEventType.RUN_FAILURE},
        ).records
    except Exception:
        return ctx  # classify() fails open on an empty context

    for record in records:
        try:
            evt = record.event_log_entry.dagster_event
            if evt is None:
                continue
            data = evt.event_specific_data
            error = getattr(data, "error", None) if data is not None else None
            if evt.event_type == DagsterEventType.STEP_FAILURE:
                if len(ctx.step_failures) >= _MAX_STEP_FAILURES:
                    continue
                cls_chain, msg_chain = _walk_error_chain(error)
                # Health-check deaths sometimes carry the detail only in the
                # event message, with an empty/absent error payload.
                if not any(msg_chain) and evt.message:
                    msg_chain = [evt.message.strip()]
                    cls_chain = cls_chain or [""]
                ctx.step_failures.append(
                    StepFailure(
                        step_key=evt.step_key or "",
                        cls_chain=cls_chain,
                        msg_chain=msg_chain,
                    )
                )
            elif evt.event_type == DagsterEventType.RUN_FAILURE and ctx.run_failure is None:
                if error is not None:
                    cls_chain, msg_chain = _walk_error_chain(error)
                    ctx.run_failure = (
                        cls_chain[-1] if cls_chain else None,
                        msg_chain[-1] if msg_chain else "",
                    )
                elif evt.message:
                    ctx.run_failure = (None, evt.message.strip())
        except Exception:
            continue  # skip malformed records, keep the rest

    return ctx
