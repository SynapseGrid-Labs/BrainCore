"""Validation helpers for text that must survive UTF-8 responses and PostgreSQL JSONB."""

import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

_UNSUPPORTED_TEXT = re.compile("[\x00\ud800-\udfff]")


def validate_storage_values(value: Any) -> Any:
    """Reject unsupported text/numbers without altering otherwise valid JSON values."""
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, str):
            if _UNSUPPORTED_TEXT.search(current):
                raise ValueError("Text must not contain NUL or surrogate code points.")
        elif isinstance(current, float) and not math.isfinite(current):
            raise ValueError("JSON numbers must be finite.")
        elif isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())
        elif isinstance(current, list | tuple):
            pending.extend(current)
    return value


def _safe_error_text(value: str) -> str:
    # Escape only unsupported code points, preserving valid Unicode. This is
    # diagnostic text, never a replacement for stored content.
    return _UNSUPPORTED_TEXT.sub(lambda match: f"\\u{ord(match[0]):04x}", value)


def validation_error_details(errors: Sequence[Mapping[str, Any]]) -> list[dict[str, object]]:
    """Expose a safe projection; input/context may contain invalid text, secrets, or NaN."""
    return [
        {
            "type": _safe_error_text(str(error["type"])),
            "loc": [
                _safe_error_text(part) if isinstance(part, str) else part for part in error["loc"]
            ],
            "msg": _safe_error_text(str(error["msg"])),
        }
        for error in errors
    ]
