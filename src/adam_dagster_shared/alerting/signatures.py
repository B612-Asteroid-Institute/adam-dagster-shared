"""Failure-signature normalization and fingerprinting.

A fingerprint identifies a *class* of failure across runs: the same broken
code path hit by different partitions, run ids, or row counts must map to one
fingerprint, or the digest's top-signature ranking degrades into a per-run list.
Normalization therefore strips every token observed to vary between runs of
the same failure in the 2026-07-29..08-28 measurement set (dates, partition
keys, counts, ids, k8s job hashes) before hashing.
"""

from __future__ import annotations

import hashlib
import re

# Order matters: longer/more-specific patterns first so e.g. a UUID is not
# half-consumed by the bare-hex rule.
_NORMALIZERS: list[tuple[re.Pattern[str], str]] = [
    # ISO datetimes and dates (2026-08-14T22:04:11, 2026-08-14 22:04, 2015-02-12)
    (re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?"), "<ts>"),
    (re.compile(r"\d{4}-\d{2}-\d{2}"), "<date>"),
    # UUIDs, then k8s job/pod hashes ("dagster-step-0bc35374201b422f..."),
    # then any bare hex token of 8+ chars.
    (re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I), "<uuid>"),
    (re.compile(r"dagster-(step|run)-[0-9a-f]{6,}(-[0-9a-z]{4,6})?"), r"dagster-\1-<hash>"),
    (re.compile(r"\b[0-9a-f]{8,}\b", re.I), "<hex>"),
    # GCS/file paths vary by partition and run directory.
    (re.compile(r"gs://[^\s'\"]+"), "<gcs-path>"),
    # Bare integers last: row counts, attempt numbers, ports, sizes.
    (re.compile(r"\b\d+\b"), "<n>"),
]

# The first line carries the exception's identity; stack tails and multi-line
# remainders vary with retry wrappers and are excluded from the skeleton.
_MAX_SKELETON_LEN = 240


def normalize_message(message: str) -> str:
    """Reduce an error message to its stable skeleton."""
    first_line = (message or "").strip().splitlines()[0] if (message or "").strip() else ""
    skeleton = first_line
    for pattern, replacement in _NORMALIZERS:
        skeleton = pattern.sub(replacement, skeleton)
    skeleton = re.sub(r"\s+", " ", skeleton).strip()
    return skeleton[:_MAX_SKELETON_LEN]


def fingerprint(job_name: str, step_key: str | None, cls_name: str | None, message: str) -> str:
    """Stable id for a failure class.

    Keyed on job + step + innermost exception class + message skeleton. The
    step key is included because the same exception class on two assets is
    two different problems (e.g. RuntimeError on aims_observation_index_shards
    vs on observation_attribution_edges).
    """
    basis = "|".join(
        [
            job_name or "",
            step_key or "",
            cls_name or "",
            normalize_message(message),
        ]
    )
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]
