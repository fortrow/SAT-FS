from __future__ import annotations

from pathlib import Path


def clean_namespace_path(path: str | Path) -> str:
    normalized = str(path).replace("\\", "/").strip()
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    if len(normalized) > 1 and normalized.endswith("/"):
        normalized = normalized[:-1]
    return normalized


def normalize_namespace_path(path: str | Path, *, label: str = "path") -> str:
    normalized = clean_namespace_path(path)
    if not normalized:
        raise ValueError(f"{label} is required")
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    _require_clean_absolute_segments(normalized, label)
    return normalized


def normalize_namespace_root(path: str | Path, *, label: str = "remote_root") -> str:
    return normalize_namespace_path(path, label=label)


def normalize_namespace_file_path(path: str | Path, *, label: str = "remote file path") -> str:
    raw = str(path).replace("\\", "/").strip()
    if raw.endswith("/"):
        raise ValueError(f"{label} must identify a file")
    normalized = normalize_namespace_path(raw, label=label)
    if normalized == "/":
        raise ValueError(f"{label} must identify a file")
    return normalized


def normalize_relative_namespace_path(path: str | Path, *, label: str = "relative path") -> str:
    normalized = clean_namespace_path(path)
    if not normalized:
        raise ValueError(f"{label} is required")
    if normalized.startswith("/"):
        raise ValueError(f"{label} must be relative")
    _require_clean_segments(normalized.split("/"), label)
    return normalized


def join_namespace_path(prefix: str | Path, relative_path: str | Path, *, label: str = "relative path") -> str:
    normalized_prefix = normalize_namespace_root(prefix, label="path_prefix")
    relative = normalize_relative_namespace_path(relative_path, label=label)
    if normalized_prefix == "/":
        return "/" + relative
    return f"{normalized_prefix}/{relative}"


def require_namespace_path_inside_prefix(prefix: str | Path, path: str | Path, *, label: str = "path") -> None:
    normalized_prefix = normalize_namespace_root(prefix, label="path_prefix")
    normalized_path = normalize_namespace_root(path, label=label)
    if normalized_prefix == "/":
        return
    if normalized_path != normalized_prefix and not normalized_path.startswith(normalized_prefix + "/"):
        raise ValueError(f"{label} must stay inside path_prefix")


def _require_clean_absolute_segments(path: str, label: str) -> None:
    if path == "/":
        return
    _require_clean_segments(path.split("/")[1:], label)


def _require_clean_segments(segments: list[str], label: str) -> None:
    for part in segments:
        if part in ("", ".", ".."):
            raise ValueError(f"{label} must not contain empty, current, or parent segments")
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in part):
            raise ValueError(f"{label} must not contain control characters")
