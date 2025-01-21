import os
import pathlib
import pickle
import traceback
from collections.abc import Generator
from typing import Any, Dict, Tuple, Union

import dagster
from dagster import (DagsterEventType, Field, InputContext, MetadataValue,
                     OutputContext, StringSource, UPathIOManager, io_manager)
from dagster._utils import PICKLE_PROTOCOL
from upath import UPath

from .gcs import gcs_rm, gcs_rsync


class YieldingIOManager(UPathIOManager):
    """
    Abstract IOManager class that loads partitions via generator
    """

    def _get_path(self, context: Union[InputContext, OutputContext]) -> UPath:
        """
        Look for a custom destination path in the metadata, otherwise use the default
        """
        # Get the actual output object from the output context
        event_data = None
        if isinstance(context, OutputContext):
            events = context.step_context.instance.all_logs(
                run_id=context.run_id, of_type=DagsterEventType.STEP_OUTPUT
            )
            for e in events:
                event_specific_data = e.dagster_event.event_specific_data
                if (
                    event_specific_data.step_output_handle.step_key == context.step_key
                    and event_specific_data.step_output_handle.output_name
                    == context.name
                ):
                    event_data = event_specific_data
                    break

        if event_data is not None and "path" in event_data.metadata:
            return UPath(event_data.metadata["path"].value)
        return super()._get_path(context)

    def _load_multiple_inputs(
        self, context: InputContext
    ) -> Generator[tuple[str, Any], None, None]:  # type: ignore[override]
        """
        Overwrite default behavior to return a generator of partitions
        """
        allow_missing_partitions = (
            context.metadata.get("allow_missing_partitions", False)
            if context.metadata is not None
            else False
        )

        paths = self._get_paths_for_partitions(context)

        context.log.debug(f"Loading {len(paths)} partitions...")

        sorted_partition_keys = sorted(paths.keys(), reverse=True)
        for partition_key in sorted_partition_keys:
            path = paths[partition_key]
            context.log.debug(
                f"Loading partition from {path} using {self.__class__.__name__}"
            )
            try:
                obj = self.load_from_path(context, path)
                yield partition_key, obj  # type: ignore[override]
            except Exception as e:
                if not allow_missing_partitions:
                    raise e
                context.log.debug(
                    f"Couldn't load partition {path} and skipped it "
                    "because the input metadata includes allow_missing_partitions=True"
                )
                context.log.debug(traceback.format_exc())

    def load_input(
        self, context: InputContext
    ) -> Union[Any, Generator[Tuple[str, Any], None, None]]:
        """
        Overriding original version to handle different typing (generator return option)
        """
        # Op outputs?
        if not context.has_asset_key:
            path = self._get_path(context)
            return self._load_single_input(path, context)

        # Non-partitioned asset
        if not context.has_asset_partitions:
            path = self._get_path(context)
            return self._load_single_input(path, context)

        asset_partition_keys = context.asset_partition_keys

        # Partitioned, but none exist.
        if len(asset_partition_keys) == 0:
            return None

        # Paritioned, but there's only one.
        if len(asset_partition_keys) == 1:
            paths = self._get_paths_for_partitions(context)
            path = paths[asset_partition_keys[0]]
            return self._load_single_input(path, context)

        # Partitioned, and there's more than one.
        return self._load_multiple_inputs(context)


class YieldingPickleIOManager(YieldingIOManager):
    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        pickled_obj = pickle.dumps(obj, PICKLE_PROTOCOL)
        context.log.info(f"Writing to {path}...")
        with path.open("wb") as f:
            f.write(pickled_obj)

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        context.log.info(f"Loading from {path}...")
        with path.open("rb") as f:
            obj = pickle.load(f)
        return obj


@io_manager(
    config_schema={
        "gcs_bucket": Field(StringSource),
        "gcs_prefix": Field(StringSource, is_required=False, default_value="dagster"),
    }
)
def yielding_gcs_io_manager(init_context):
    base_path_str = f"gs://{init_context.resource_config.get('gcs_bucket')}/{init_context.resource_config.get('gcs_prefix')}"  # NOQA: E501
    base_path = UPath(base_path_str)
    manager = YieldingPickleIOManager(base_path=base_path)
    return manager


def _dirsize(path) -> int:
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, name)) for name in files)
    return total


class GCSFileIOManager(YieldingIOManager):
    """
    Use IO Manager mechanics to reference files on disk rather than in memory
    """

    def __init__(self, local_base_path: pathlib.Path, remote_base_path: UPath):
        self.local_base_path = local_base_path
        self.remote_base_path = remote_base_path
        super().__init__(base_path=remote_base_path)

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath):
        """
        Version of dump_to_path that shells out to gsutil to copy files and directories
        """
        # Remove existing files if they exist
        if path.exists():
            gcs_rm(str(path))

        gcs_rsync(str(obj), str(path))

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        """
        Version of load_from_path that shells out to gsutil to copy files and directories
        """
        local_path = self.local_base_path / path.relative_to(
            self.remote_base_path
        ).path.lstrip("/")
        context.log.info(f"Loading from {path} to {local_path}...")
        gcs_rsync(str(path), str(local_path))
        return str(local_path)

    def get_metadata(
        self,
        context: OutputContext,  # pylint: disable=unused-argument
        obj: Any,  # pylint: disable=unused-argument
    ) -> Dict[str, MetadataValue]:
        """Child classes should override this method to add custom metadata to the outputs."""
        local_path = pathlib.Path(obj)

        if local_path.is_dir():
            filetype = "directory"
        elif local_path.is_file():
            filetype = "file"
        else:
            # Could be a FIFO or a socket or something else weird.
            filetype = "exotic"

        if filetype == "directory":
            size_bytes = _dirsize(local_path)
        else:
            # Could be wrong for exotic filetypes. oh well.
            statinfo = local_path.stat()
            size_bytes = statinfo.st_size

        return {
            "src_path": obj,
            "type": MetadataValue.text(filetype),
            "file_size_bytes": MetadataValue.int(size_bytes),
        }


@io_manager(
    config_schema={
        "gcs_bucket": Field(StringSource),
        "gcs_prefix": Field(StringSource, is_required=False, default_value="dagster"),
        "local_dir": Field(StringSource, is_required=False, default_value="/tmp/run"),
    }
)
def file_gcs_io_manager(init_context):
    remote_base_path_str = f"gs://{init_context.resource_config.get('gcs_bucket')}/{init_context.resource_config.get('gcs_prefix')}"  # NOQA: E501
    remote_base_path = UPath(remote_base_path_str)

    local_base_path = pathlib.Path(init_context.resource_config["local_dir"])
    return GCSFileIOManager(
        local_base_path=local_base_path, remote_base_path=remote_base_path
    )


class SimpleIOManager(UPathIOManager):
    def __init__(self, base_path: UPath, extension: str = ""):
        self.extension = extension
        super().__init__(base_path=base_path)

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath):
        context.log.info(f"Writing from {context.name} to {path}...")
        with path.open("w") as f:
            f.write(obj)

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        context.log.info(f"Loading from {path}...")
        with path.open("r") as f:
            return f.read()


@io_manager(
    config_schema={
        "gcs_bucket": Field(StringSource),
        "gcs_prefix": Field(StringSource, is_required=False, default_value="dagster"),
        "extension": Field(StringSource, is_required=False, default_value=""),
    }
)
def simple_gcs_io_manager(init_context):
    base_path_str = f"gs://{init_context.resource_config.get('gcs_bucket')}/{init_context.resource_config.get('gcs_prefix')}"  # NOQA: E501
    base_path = UPath(base_path_str)
    return SimpleIOManager(
        base_path=base_path, extension=init_context.resource_config.get("extension")
    )


class ListContentsIOManager(YieldingIOManager):
    """
    Lists the contents of the path, but does not load the files
    """

    def __init__(self, remote_base_path: UPath):
        self.remote_base_path = remote_base_path
        super().__init__(base_path=remote_base_path)

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath):
        raise NotImplementedError("ListContentsIOManager is read-only")

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        contents = [path] + list(path.glob("**"))
        contents.sort()
        return contents


@io_manager(
    config_schema={
        "gcs_bucket": Field(StringSource),
        "gcs_prefix": Field(StringSource, is_required=False, default_value="dagster"),
    }
)
def list_contents_io_manager(init_context):
    remote_base_path_str = f"gs://{init_context.resource_config.get('gcs_bucket')}/{init_context.resource_config.get('gcs_prefix')}"  # NOQA: E501
    remote_base_path = UPath(remote_base_path_str)

    return ListContentsIOManager(remote_base_path=remote_base_path)


T = TypeVar("T")


class MetaValueDynamicOut(dagster.DynamicOutput[Generic[T]]):
    """
    Makes the mapping key the data that is passed to downstream job inputs.
    """

    def __init__(
        self,
        value: T,
        *args,
        **kwargs,
    ):
        metadata = kwargs.get("metadata", {})
        metadata["value"] = value
        return super().__init__(
            value,
            *args,
            metadata=metadata,
            **kwargs,
        )


class MetaDataIOManager(dagster.IOManager):
    """
    Passes results directly into Output metadata in the dagster db and loads it from there.

    This was created in order to avoid the network I/O of storing small values to GCS.
    It also avoids scanning the database events table for the value, which would be very
    repetitive for each input load.

    Must be used with MetaValueDynamicOut and it limited to values that can be
    serialized as dagster metadata values.
    """

    def handle_output(self, context: dagster.OutputContext, obj: Any) -> None:
        """
        Object is storedin metadata by MetaValueDynamicOut
        """
        pass

    def load_input(self, context: dagster.InputContext) -> Any:
        """
        Loads object from the output event metadata
        """
        # If the input came from an asset, grab the
        # asset materialization record
        if context.has_asset_key:
            raise Exception("DBIOManager does not currently support assets.")

        events = context.instance.all_logs(
            run_id=context.upstream_output.run_id,
            of_type=dagster.DagsterEventType.STEP_OUTPUT,
        )
        context.log.debug(f"Found {len(events)} events for {context.upstream_output.run_id}")

        output_event = None
        for event in events:
            if (
                event.dagster_event.event_specific_data.step_output_handle.step_key
                == context.upstream_output.step_key
                and event.dagster_event.event_specific_data.step_output_handle.output_name
                == context.upstream_output.name
                and event.dagster_event.event_specific_data.mapping_key == context.upstream_output.mapping_key
            ):
                output_event = event
                break

        value = output_event.dagster_event.event_specific_data.metadata["value"].value

        return value
