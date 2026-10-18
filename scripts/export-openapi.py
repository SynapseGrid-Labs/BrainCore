#!/usr/bin/env python3
"""Write the deterministic public OpenAPI contract used by SDK builds."""

import json
from pathlib import Path

from braincore_memory.api import app


def main() -> None:
    target = Path(__file__).resolve().parents[1] / "openapi.json"
    target.write_text(
        json.dumps(app.openapi(), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
