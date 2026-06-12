"""Tests for ``adam_dagster_shared.gcs.gcs_rsync`` behaviour changes:

- Skip the subprocess upload when the local file's MD5 matches the
  remote object's ``md5Hash``.
- Prefer ``gcloud storage cp`` over ``gsutil -m cp`` when available.
- Fall through to upload when MD5s differ.
"""

from __future__ import annotations

import base64
import hashlib
import subprocess
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from adam_dagster_shared import gcs as gcs_mod


def _md5_b64(data: bytes) -> str:
    return base64.b64encode(hashlib.md5(data).digest()).decode("ascii")


@pytest.fixture
def tmp_local_file(tmp_path: Path) -> Path:
    p = tmp_path / "payload.bin"
    p.write_bytes(b"hello-precovery\n")
    return p


def test_local_md5_base64_matches_hashlib(tmp_local_file: Path) -> None:
    expected = _md5_b64(tmp_local_file.read_bytes())
    assert gcs_mod._local_md5_base64(str(tmp_local_file)) == expected


def test_gcs_rsync_skips_upload_when_remote_md5_matches(
    tmp_local_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Identical local + remote MD5 -> short-circuit, no subprocess."""
    local_b64 = _md5_b64(tmp_local_file.read_bytes())
    monkeypatch.setattr(gcs_mod, "gcs_file_type", lambda p: "file")
    monkeypatch.setattr(gcs_mod, "_remote_md5_base64", lambda uri: local_b64)
    subprocess_run = mock.Mock()
    monkeypatch.setattr(gcs_mod.subprocess, "run", subprocess_run)

    out = gcs_mod.gcs_rsync(str(tmp_local_file), "gs://bucket/object.bin")

    assert out is None
    subprocess_run.assert_not_called()


def test_gcs_rsync_falls_through_when_remote_md5_differs(
    tmp_local_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gcs_mod, "gcs_file_type", lambda p: "file")
    monkeypatch.setattr(
        gcs_mod, "_remote_md5_base64", lambda uri: "DIFFERENT_HASH="
    )
    monkeypatch.setattr(gcs_mod, "_prefer_gcloud_storage", lambda: True)
    subprocess_run = mock.Mock(
        return_value=subprocess.CompletedProcess([], 0, "", "")
    )
    monkeypatch.setattr(gcs_mod.subprocess, "run", subprocess_run)

    gcs_mod.gcs_rsync(str(tmp_local_file), "gs://bucket/object.bin")

    assert subprocess_run.call_count == 1
    # Verify it used gcloud storage cp (preferred tool).
    invoked_cmd = subprocess_run.call_args.args[0]
    assert invoked_cmd[:3] == ["gcloud", "storage", "cp"]
    assert invoked_cmd[-2:] == [str(tmp_local_file), "gs://bucket/object.bin"]


def test_gcs_rsync_falls_through_when_remote_object_missing(
    tmp_local_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Remote missing -> no md5 to compare against -> proceed with upload."""
    monkeypatch.setattr(gcs_mod, "gcs_file_type", lambda p: "file")
    monkeypatch.setattr(gcs_mod, "_remote_md5_base64", lambda uri: None)
    monkeypatch.setattr(gcs_mod, "_prefer_gcloud_storage", lambda: True)
    subprocess_run = mock.Mock(
        return_value=subprocess.CompletedProcess([], 0, "", "")
    )
    monkeypatch.setattr(gcs_mod.subprocess, "run", subprocess_run)

    gcs_mod.gcs_rsync(str(tmp_local_file), "gs://bucket/object.bin")

    assert subprocess_run.call_count == 1


def test_gcs_rsync_force_upload_bypasses_md5_short_circuit(
    tmp_local_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_b64 = _md5_b64(tmp_local_file.read_bytes())
    monkeypatch.setattr(gcs_mod, "gcs_file_type", lambda p: "file")
    monkeypatch.setattr(gcs_mod, "_remote_md5_base64", lambda uri: local_b64)
    monkeypatch.setattr(gcs_mod, "_prefer_gcloud_storage", lambda: True)
    subprocess_run = mock.Mock(
        return_value=subprocess.CompletedProcess([], 0, "", "")
    )
    monkeypatch.setattr(gcs_mod.subprocess, "run", subprocess_run)

    gcs_mod.gcs_rsync(
        str(tmp_local_file),
        "gs://bucket/object.bin",
        force_upload=True,
    )

    # force_upload=True -> upload always proceeds.
    assert subprocess_run.call_count == 1


def test_gcs_rsync_uses_gsutil_when_gcloud_storage_unavailable(
    tmp_local_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gcs_mod, "gcs_file_type", lambda p: "file")
    monkeypatch.setattr(gcs_mod, "_remote_md5_base64", lambda uri: None)
    monkeypatch.setattr(gcs_mod, "_prefer_gcloud_storage", lambda: False)
    subprocess_run = mock.Mock(
        return_value=subprocess.CompletedProcess([], 0, "", "")
    )
    monkeypatch.setattr(gcs_mod.subprocess, "run", subprocess_run)

    gcs_mod.gcs_rsync(str(tmp_local_file), "gs://bucket/object.bin")

    invoked_cmd = subprocess_run.call_args.args[0]
    assert invoked_cmd[:3] == ["gsutil", "-m", "cp"]


def test_gcs_rsync_dir_uses_gcloud_storage_rsync_when_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Directory uploads use ``gcloud storage rsync --recursive`` when
    gcloud is preferred -- defaults to checksum-based delta comparison
    (gsutil rsync uses mtime+size, less reliable).
    """
    src_dir = tmp_path / "data"
    src_dir.mkdir()
    (src_dir / "x.parquet").write_bytes(b"x")
    monkeypatch.setattr(gcs_mod, "gcs_file_type", lambda p: "directory")
    monkeypatch.setattr(gcs_mod, "_prefer_gcloud_storage", lambda: True)
    subprocess_run = mock.Mock(
        return_value=subprocess.CompletedProcess([], 0, "", "")
    )
    monkeypatch.setattr(gcs_mod.subprocess, "run", subprocess_run)

    gcs_mod.gcs_rsync(str(src_dir), "gs://bucket/data/")

    invoked_cmd = subprocess_run.call_args.args[0]
    assert invoked_cmd[:4] == ["gcloud", "storage", "rsync", "--recursive"]


def test_gcs_rsync_local_to_local_uses_shutil_no_subprocess(
    tmp_local_file: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Local-to-local copies bypass both the bearer-token MD5 check
    and the subprocess entirely -- ``gcloud storage cp`` rejects
    local-to-local ("Local copies not supported"), and shelling out
    to ``gsutil cp`` for an in-process operation is wasteful. We use
    ``shutil`` directly.
    """
    monkeypatch.setattr(gcs_mod, "gcs_file_type", lambda p: "file")
    remote_md5_mock = mock.Mock(
        side_effect=AssertionError("should not be called for local dst")
    )
    monkeypatch.setattr(gcs_mod, "_remote_md5_base64", remote_md5_mock)
    monkeypatch.setattr(gcs_mod, "_prefer_gcloud_storage", lambda: True)
    subprocess_run = mock.Mock(
        side_effect=AssertionError(
            "local-to-local must not shell out to gsutil/gcloud"
        )
    )
    monkeypatch.setattr(gcs_mod.subprocess, "run", subprocess_run)

    dst = tmp_path / "dst.bin"
    gcs_mod.gcs_rsync(str(tmp_local_file), str(dst))

    remote_md5_mock.assert_not_called()
    subprocess_run.assert_not_called()
    assert dst.exists()
    assert dst.read_bytes() == tmp_local_file.read_bytes()


def test_gcs_rsync_local_to_local_directory_mirrors_contents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Local-to-local directory copies mirror file contents via
    ``shutil``; no subprocess. Existing dst entries are left in place
    (no ``delete_unmatched`` semantics for the local-to-local path).
    """
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "a.parquet").write_bytes(b"a")
    (src_dir / "sub").mkdir()
    (src_dir / "sub" / "b.parquet").write_bytes(b"b")
    dst_dir = tmp_path / "dst"
    monkeypatch.setattr(gcs_mod, "gcs_file_type", lambda p: "directory")
    subprocess_run = mock.Mock(
        side_effect=AssertionError(
            "local-to-local must not shell out to gsutil/gcloud"
        )
    )
    monkeypatch.setattr(gcs_mod.subprocess, "run", subprocess_run)

    gcs_mod.gcs_rsync(str(src_dir), str(dst_dir))

    subprocess_run.assert_not_called()
    assert (dst_dir / "a.parquet").read_bytes() == b"a"
    assert (dst_dir / "sub" / "b.parquet").read_bytes() == b"b"
