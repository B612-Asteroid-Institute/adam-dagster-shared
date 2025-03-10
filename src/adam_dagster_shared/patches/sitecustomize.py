print("Loading sitecustomize.py for adam-impact...")

import base64
import logging
import zlib
from typing import Mapping, Sequence

import dagster._cli.api
from dagster import DagsterRun
from dagster._core.execution.plan.state import _derive_state_from_logs
from dagster._core.instance import DagsterInstance
from dagster._grpc.types import ExecuteStepArgs, KnownExecutionState, serialize_value

logger = logging.getLogger(__name__)


def patch__execute_step_command_body():
    original_execute_step_command_body = dagster._cli.api._execute_step_command_body

    def wrapped_execute_step_command_body(
        args: ExecuteStepArgs, instance: DagsterInstance, dagster_run: DagsterRun
    ):
        _, known_state = _derive_state_from_logs(instance, dagster_run)

        print(f"args: {args}")
        print(f"instance: {instance}")
        print(f"dagster_run: {dagster_run}")
        print(f"known_state: {known_state}")

        updated_args = ExecuteStepArgs(
            job_origin=args.job_origin,
            run_id=args.run_id,
            step_keys_to_execute=args.step_keys_to_execute,
            instance_ref=args.instance_ref,
            retry_mode=args.retry_mode,
            known_state=known_state,
            should_verify_step=args.should_verify_step,
            print_serialized_events=args.print_serialized_events,
        )

        return original_execute_step_command_body(updated_args, instance, dagster_run)

    dagster._cli.api._execute_step_command_body = wrapped_execute_step_command_body
    print("patched execute_step_command_body")

def patch_get_command_env():
    def get_command_env(self) -> Sequence[Mapping[str, str]]:
        known_state = KnownExecutionState(
            previous_retry_attempts=None,
            dynamic_mappings=None,
            step_output_versions=None,
            ready_outputs=None,
            parent_state=None,
        )
        self_copy = ExecuteStepArgs(
            job_origin=self.job_origin,
            run_id=self.run_id,
            step_keys_to_execute=self.step_keys_to_execute,
            instance_ref=self.instance_ref,
            retry_mode=self.retry_mode,
            known_state=known_state,  # <-- This is what we're overriding
            should_verify_step=self.should_verify_step,
            print_serialized_events=self.print_serialized_events,
        )

        serialized_args = serialize_value(self_copy)
        logger.info(f"serialized_args: {serialized_args}")
        print(f"serialized_args: {serialized_args}")
        compressed_args = base64.b64encode(
            zlib.compress(serialized_args.encode())
        ).decode()

        return [{
            "name": "DAGSTER_COMPRESSED_EXECUTE_STEP_ARGS",
            "value": compressed_args,
        }]

    ExecuteStepArgs.get_command_env = get_command_env
    print("patched get_command_env")


def patch_ActiveExecution_mark_complete():
    def _mark_complete(self, step_key: str) -> None:
        try:
            self._in_flight.remove(step_key)
        except KeyError:
            logger.warning(f"Step {step_key} not in in_flight")

    dagster._core.execution.plan.active.ActiveExecution._mark_complete = _mark_complete
    print("patched ActiveExecution._mark_complete")

# Apply the patches when Python initializes
patch_get_command_env()
patch__execute_step_command_body()
patch_ActiveExecution_mark_complete()