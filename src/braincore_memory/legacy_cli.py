"""Explicit native subtype write CLI; credentials are host-local environment only."""

import argparse
import json
import os
import sys
from typing import Any

import httpx

from .legacy_types import FIELD_TYPES


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["mutate", "batch"])
    parser.add_argument("--type", choices=sorted(FIELD_TYPES))
    parser.add_argument("--id")
    parser.add_argument(
        "--action", default="upsert", choices=["upsert", "suppress", "retire", "restore"]
    )
    parser.add_argument("--idempotency-key", required=True)
    parser.add_argument("--reason", default="legacy client mutation")
    args = parser.parse_args()
    api_key = os.environ.get("BRAINCORE_API_KEY")
    if not api_key:
        parser.error("BRAINCORE_API_KEY is required")
    if args.command == "mutate" and args.type is None:
        parser.error("--type is required for mutate")
    payload: dict[str, Any] = {
        "object_type": args.type,
        "object_id": args.id,
        "action": args.action,
        "reason": args.reason,
        "fields": json.load(sys.stdin),
    }
    if args.command == "batch":
        payload = {"items": payload["fields"]}
    try:
        with httpx.Client(
            base_url=os.environ.get("BRAINCORE_URL", "http://127.0.0.1:8787"), timeout=30
        ) as client:
            response = client.post(
                "/v1/legacy/objects:" + args.command,
                json=payload,
                headers={
                    "Authorization": "Bearer " + api_key,
                    "Idempotency-Key": args.idempotency_key,
                },
            )
            if not response.is_success:
                print(
                    "canonical legacy mutation failed: HTTP " + str(response.status_code),
                    file=sys.stderr,
                )
                raise SystemExit(1)
            print(json.dumps(response.json()))
    except httpx.HTTPError:
        print("canonical legacy mutation transport failed", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
