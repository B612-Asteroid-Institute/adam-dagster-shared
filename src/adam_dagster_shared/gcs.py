import logging
import pathlib
import subprocess
from typing import Literal, Optional, Tuple

logger = logging.getLogger(__name__)


def parse_gcs_path(gcs_path: str) -> Tuple[str, str]:
    """
    Parse a gcs path that starts with gs:// into bucket name and path
    """
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    parts = gcs_path.split("/", 3)
    if len(parts) < 4:
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    bucket_name, path = parts[2], parts[3]
    return bucket_name, path


def gcs_file_type(path: str) -> Literal["file", "directory", None]:
    """
    Checks if the path is a file, directory, or does not exist
    """
    if not path.startswith("gs://"):
        if not pathlib.Path(path).exists():
            return None
        if pathlib.Path(path).is_file():
            return "file"
        return "directory"

    try:
        result = subprocess.run(
            ["gsutil", "ls", path], check=True, capture_output=True, text=True
        )
        results = result.stdout.split("\n")
        # If the path is a file, it should be the only result
        # If the path is a directory, it will not be in the results
        if path in results:
            return "file"
        return "directory"
    except subprocess.CalledProcessError as e:
        # If the error message contains "One or more URLs matched no objects."
        # then we know the remote directory or file does not exist
        if "One or more URLs matched no objects." in e.stderr:
            return None
        raise


def gcs_rsync(
    src: str,
    dst: str,
    delete_unmatched: Optional[bool] = False,
    exclude: Optional[str] = None,
    make_public: Optional[bool] = False,
) -> subprocess.CompletedProcess | None:
    """
    Syncs a GCS path to another location, using `cp` for files and `rsync` for directories.

    Args:
        src: Source path
        dst: Destination path
        delete_unmatched: If True, deletes files in dst that don't exist in src
        exclude: Pattern to exclude from sync
        make_public: If True, makes the destination files publicly accessible
    """
    file_type = gcs_file_type(src)
    if not dst.startswith("gs://"):
        # Make the local directory first.
        # If dst should be a file, make the parent directory
        if file_type in ["file", None]:
            pathlib.Path(dst).parent.mkdir(parents=True, exist_ok=True)
        else:
            pathlib.Path(dst).mkdir(parents=True, exist_ok=True)

    if file_type is None:
        logger.warning(f"Source path {src} does not exist")
        return

    logger.info(f"Syncing {src} to {dst}")

    if file_type == "file":
        command = ["gsutil", "-m", "cp"]
    else:
        command = ["gsutil", "-m", "rsync", "-r"]
        if exclude is not None:
            command += ["-x", exclude]

    if delete_unmatched:
        command += ["-d"]

    # Add public-read ACL during transfer if requested and destination is GCS
    if make_public and dst.startswith("gs://"):
        command += ["-a", "public-read"]

    command += [src, dst]

    try:
        output = subprocess.run(
            command, check=True, capture_output=True, text=True, start_new_session=True
        )
        logger.info(f"Command stdout: {output.stdout}")
        logger.info(f"Command stderr: {output.stderr}")
    except subprocess.CalledProcessError as e:
        logger.info(f"Error stdout: {e.stdout}")
        logger.info(f"Error stderr: {e.stderr}")
        raise e
    return output


def gcs_rm(path: str) -> subprocess.CompletedProcess:
    """
    Removes a gcs path
    """
    try:
        output = subprocess.run(
            ["gsutil", "-m", "rm", "-r", path],
            check=True,
            capture_output=True,
            text=True,
            start_new_session=True,
        )
    except subprocess.CalledProcessError as e:
        logger.info(e.stdout)
        logger.info(e.stderr)
        raise e
    return output


def gcs_exists(path: str) -> bool:
    """
    Checks if a gcs path exists
    """
    try:
        subprocess.run(
            ["gsutil", "ls", path],
            check=True,
            capture_output=True,
            text=True,
            start_new_session=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False
