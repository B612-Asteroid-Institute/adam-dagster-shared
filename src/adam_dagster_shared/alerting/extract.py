"""Build a FailureContext from a run's event log.

Every accessor is defensive: extraction problems degrade to an emptier context
(which classify() fails open on), never to a raised error.
"""

from __future__ import annotations

from dagster import DagsterEventType

from .classify import FailureContext, StepFailure

_MAX_STEP_FAILURES = 20


def _walk_error_chain(error) -> tuple[list[str], list[str]]:
    """SerializableErrorInfo -> ([cls outermost..innermost], [msg ...])."""
    cls_chain: list[str] = []
    msg_chain: list[str] = []
    node = error
    for _ in range(10):
        if node is None:
            break
        cls_chain.append(getattr(node, "cls_name", None) or "")
        msg_chain.append((getattr(node, "message", None) or "").strip())
        node = getattr(node, "cause", None)
    return cls_chain, msg_chain


def extract_failure_context(instance, run) -> FailureContext:
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
        return ctx

    for record in records:
        try:
            evt = record.event_log_entry.dagster_event
            if evt is None:
                continue
            error = getattr(evt.event_specific_data, "error", None)
            if evt.event_type == DagsterEventType.STEP_FAILURE:
                if len(ctx.step_failures) >= _MAX_STEP_FAILURES:
                    continue
                cls_chain, msg_chain = _walk_error_chain(error)
                if not any(msg_chain) and evt.message:  # health-check deaths carry no error payload
                    msg_chain = [evt.message.strip()]
                    cls_chain = cls_chain or [""]
                ctx.step_failures.append(StepFailure(evt.step_key or "", cls_chain, msg_chain))
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
            continue
    return ctx
