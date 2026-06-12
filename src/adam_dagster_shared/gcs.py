import base64
import hashlib
import json
import logging
import pathlib
import shutil
import subprocess
import threading
import urllib.error
import urllib.parse
import urllib.request
from typing import Literal, Optional, Tuple

logger = logging.getLogger(__name__)

# Per the 2026-06-04 WAN bulk-transfer benchmark, ``gcloud storage cp``
# is ~60% faster than ``gsutil -m cp`` for the same payload. We prefer
# ``gcloud storage`` when it's available on PATH and fall back to
# ``gsutil`` otherwise. ``gcloud storage rsync`` also defaults to
# checksum-based delta comparison vs ``gsutil rsync`` (mtime+size),
# which is a strict correctness win for content-driven uploads.
_TOOL_CACHE: dict[str, bool] = {}
_TOKEN_LOCK = threading.Lock()
_CACHED_CREDENTIALS: list = []  # one-slot mailbox for the Credentials


def _prefer_gcloud_storage() -> bool:
    """Cache-checked: True iff ``gcloud storage`` is on PATH and the
    component works (--help exits 0).
    """
    if "gcloud_storage" in _TOOL_CACHE:
        return _TOOL_CACHE["gcloud_storage"]
    if shutil.which("gcloud") is None:
        _TOOL_CACHE["gcloud_storage"] = False
        return False
    try:
        subprocess.run(
            ["gcloud", "storage", "--help"],
            check=True,
            capture_output=True,
            text=True,
            start_new_session=True,
        )
        _TOOL_CACHE["gcloud_storage"] = True
    except (subprocess.CalledProcessError, FileNotFoundError):
        _TOOL_CACHE["gcloud_storage"] = False
    return _TOOL_CACHE["gcloud_storage"]


def _local_md5_base64(path: str) -> str:
    """Streamed MD5 of a local file, base64-encoded to match the
    ``md5Hash`` / ``x-goog-hash`` GCS object metadata format.
    """
    digest = hashlib.md5(usedforsecurity=False)
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return base64.b64encode(digest.digest()).decode("ascii")


def _adc_bearer_token() -> str | None:
    """Return a fresh ADC bearer token, or None if google.auth can't
    resolve credentials. Cached + thread-safely refreshed.
    """
    try:
        import google.auth  # type: ignore[import-not-found]
        import google.auth.transport.requests  # type: ignore[import-not-found]
    except ImportError:
        return None
    with _TOKEN_LOCK:
        if not _CACHED_CREDENTIALS:
            try:
                creds, _project = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
            except Exception:
                logger.exception("Could not resolve Google ADC credentials")
                return None
            _CACHED_CREDENTIALS.append(creds)
        creds = _CACHED_CREDENTIALS[0]
        if not creds.valid:
            try:
                creds.refresh(google.auth.transport.requests.Request())
            except Exception:
                logger.exception("Failed to refresh ADC credentials")
                return None
    return getattr(creds, "token", None)


def _remote_md5_base64(gcs_uri: str) -> str | None:
    """Fetch the ``md5Hash`` of a GCS object via the JSON API. Returns
    None when the object is missing, auth is unavailable, or any other
    error. Use as an upload-skip hint, never as a strict assertion.
    """
    if not gcs_uri.startswith("gs://"):
        return None
    no_scheme = gcs_uri[len("gs://"):]
    bucket, _, obj = no_scheme.partition("/")
    if not bucket or not obj:
        return None
    token = _adc_bearer_token()
    if token is None:
        return None
    api_url = (
        "https://storage.googleapis.com/storage/v1/b/"
        + urllib.parse.quote(bucket, safe="")
        + "/o/"
        + urllib.parse.quote(obj, safe="")
        + "?fields=md5Hash"
    )
    request = urllib.request.Request(
        api_url, headers={"Authorization": f"Bearer {token}"}
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            return None
        logger.warning("Remote md5 fetch HTTPError %s for %s", e.code, gcs_uri)
        return None
    except Exception:
        logger.exception("Remote md5 fetch failed for %s", gcs_uri)
        return None
    md5 = payload.get("md5Hash")
    return str(md5) if md5 else None


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
    force_upload: bool = False,
) -> subprocess.CompletedProcess | None:
    """
    Syncs a GCS path to another location, using `cp` for files and `rsync` for directories.

    Args:
        src: Source path
        dst: Destination path
        delete_unmatched: If True, deletes files in dst that don't exist in src
        exclude: Pattern to exclude from sync
        make_public: If True, makes the destination files publicly accessible
        force_upload: When True, skip the local-vs-remote MD5 short-circuit
            for local-file -> gs:// uploads. Defaults to False so identical
            content saves the subprocess + network round-trip.

    Implementation notes
    --------------------
    Tool selection prefers ``gcloud storage cp`` / ``gcloud storage rsync``
    over ``gsutil -m cp`` / ``gsutil -m rsync -r`` when ``gcloud storage``
    is available on PATH. Per the 2026-06-04 WAN benchmark this is ~60%
    faster for bulk transfers, and ``gcloud storage rsync`` defaults to
    checksum-based delta comparison vs ``gsutil rsync``'s mtime+size --
    a strict correctness win for content-driven flows.

    For single-file local -> gs:// uploads, the function computes the
    local MD5 and compares it to the remote object's ``md5Hash`` (one
    stdlib HTTPS metadata GET with an ADC bearer token; no
    google.cloud.storage). If they match, the subprocess is skipped
    entirely. This is deliberately NOT delegated to the CLI tools:
    ``cp`` has no content-skip at all (``--no-clobber`` keys on
    existence, not content), ``rsync`` cannot target a single file
    without listing the parent prefix, and either way a ``gcloud`` CLI
    boot costs ~5-6 s (measured 2026-06-12) versus ~50 ms for the
    metadata GET -- which is the whole point for hot publisher loops
    that re-upload mostly-unchanged files.
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

    src_is_gs = src.startswith("gs://")
    dst_is_gs = dst.startswith("gs://")

    # Local-to-local copies: ``gcloud storage cp`` explicitly refuses
    # them ("Local copies not supported"), and ``gsutil cp`` works but
    # forks a subprocess for what is an in-process operation. Use
    # ``shutil`` directly for the common local-stack pattern where
    # ``ADAM_GCS_PREFIX`` is a filesystem path. ``delete_unmatched`` and
    # ``make_public`` don't apply to local-to-local, so they're
    # silently dropped.
    if not src_is_gs and not dst_is_gs:
        if file_type == "file":
            shutil.copyfile(src, dst)
        else:
            # Directory: mirror semantically (overlay missing files;
            # leaves existing dst entries in place, like ``gsutil rsync``
            # without ``-d``).
            for src_root, _dirs, files in __import__("os").walk(src):
                rel = pathlib.Path(src_root).relative_to(src)
                dst_root = pathlib.Path(dst) / rel
                dst_root.mkdir(parents=True, exist_ok=True)
                for name in files:
                    shutil.copy2(
                        pathlib.Path(src_root) / name, dst_root / name
                    )
        return None

    # Local-file -> gs:// upload short-circuit: skip when the remote
    # object's MD5 matches the local file. Best-effort -- on any error
    # (auth, network, missing remote) fall through to the subprocess.
    if (
        not force_upload
        and file_type == "file"
        and not src.startswith("gs://")
        and dst.startswith("gs://")
    ):
        try:
            local_md5 = _local_md5_base64(src)
            remote_md5 = _remote_md5_base64(dst)
            if remote_md5 is not None and local_md5 == remote_md5:
                logger.info(
                    "gcs_rsync: skipping upload (md5 match) %s -> %s",
                    src,
                    dst,
                )
                return None
        except Exception:
            # Any failure in the short-circuit path is non-fatal --
            # we still want the upload to proceed.
            logger.debug(
                "gcs_rsync: md5 short-circuit failed for %s -> %s, "
                "falling through to upload",
                src,
                dst,
                exc_info=True,
            )

    logger.info(f"Syncing {src} to {dst}")

    use_gcloud = _prefer_gcloud_storage()
    if file_type == "file":
        if use_gcloud:
            command = ["gcloud", "storage", "cp"]
        else:
            command = ["gsutil", "-m", "cp"]
    else:
        if use_gcloud:
            command = ["gcloud", "storage", "rsync", "--recursive"]
            if exclude is not None:
                command += ["--exclude", exclude]
        else:
            command = ["gsutil", "-m", "rsync", "-r"]
            if exclude is not None:
                command += ["-x", exclude]

    if delete_unmatched:
        # ``gcloud storage rsync`` calls it ``--delete-unmatched-destination-objects``;
        # ``gsutil rsync`` calls it ``-d``.
        if use_gcloud and file_type != "file":
            command += ["--delete-unmatched-destination-objects"]
        elif not use_gcloud:
            command += ["-d"]

    # Add public-read ACL during transfer if requested and destination is GCS
    if make_public and dst.startswith("gs://"):
        if use_gcloud:
            command += ["--predefined-acl", "publicRead"]
        else:
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
