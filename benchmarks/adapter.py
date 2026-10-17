"""Independent dataset adapters for BrainCore' normalized benchmark format."""

from __future__ import annotations

import argparse
import ast
import json
import re
from pathlib import Path
from typing import Any


def _loaded(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return ast.literal_eval(value)


def adapt_locomo(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for sample in data:
        conversation = sample["conversation"]
        speaker_a = str(conversation.get("speaker_a", "user"))
        sessions = []
        session_numbers = sorted(
            int(match.group(1))
            for key in conversation
            if (match := re.fullmatch(r"session_(\d+)", key))
        )
        for number in session_numbers:
            turns = conversation[f"session_{number}"]
            sessions.append(
                {
                    "id": f"D{number}",
                    "timestamp": conversation.get(f"session_{number}_date_time"),
                    "messages": [
                        {
                            "role": "user"
                            if str(turn.get("speaker")) == speaker_a
                            else "assistant",
                            "content": str(turn.get("text", "")),
                            "source_id": str(turn.get("dia_id", "")),
                        }
                        for turn in turns
                    ],
                }
            )
        questions = []
        for index, question in enumerate(sample.get("qa", [])):
            evidence = re.findall(r"D\d+", " ".join(map(str, question.get("evidence", []))))
            questions.append(
                {
                    "id": f"{sample['sample_id']}-q{index}",
                    "question": question["question"],
                    "answer": question.get("answer"),
                    "category": str(question.get("category", "unknown")),
                    "evidence_session_ids": sorted(set(evidence)),
                }
            )
        cases.append({"id": str(sample["sample_id"]), "sessions": sessions, "questions": questions})
    return cases


def adapt_longmemeval(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cases = []
    for entry in data:
        sessions = [
            {
                "id": str(session_id),
                "timestamp": timestamp,
                "messages": [
                    {
                        "role": str(turn.get("role", "user")),
                        "content": str(turn.get("content", "")),
                    }
                    for turn in turns
                ],
            }
            for session_id, timestamp, turns in zip(
                entry["haystack_session_ids"],
                entry["haystack_dates"],
                entry["haystack_sessions"],
                strict=True,
            )
        ]
        question_id = str(entry["question_id"])
        cases.append(
            {
                "id": question_id,
                "sessions": sessions,
                "questions": [
                    {
                        "id": question_id,
                        "question": entry["question"],
                        "answer": entry.get("answer"),
                        "category": entry.get("question_type", "unknown"),
                        "evidence_session_ids": list(map(str, entry.get("answer_session_ids", []))),
                    }
                ],
            }
        )
    return cases


def adapt_beam(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cases = []
    for row in data:
        chat = _loaded(row["chat"])
        questions_by_category = _loaded(row["probing_questions"])
        sessions = []
        for index, batch in enumerate(chat):
            messages = []
            for turn in batch:
                if isinstance(turn, list):
                    messages.extend(turn)
                elif isinstance(turn, dict):
                    messages.append(turn)
            sessions.append(
                {
                    "id": f"batch-{index + 1}",
                    "timestamp": next(
                        (
                            message.get("time_anchor")
                            for message in messages
                            if message.get("time_anchor")
                        ),
                        None,
                    ),
                    "messages": [
                        {
                            "role": str(message.get("role", "user")),
                            "content": str(message.get("content", "")),
                            "source_id": str(message.get("id", "")),
                        }
                        for message in messages
                    ],
                }
            )
        questions = []
        for category, entries in questions_by_category.items():
            for index, question in enumerate(entries):
                questions.append(
                    {
                        "id": f"{row['conversation_id']}-{category}-{index}",
                        "question": question["question"],
                        "answer": question.get("answer") or question.get("rubric"),
                        "category": category,
                        "evidence_session_ids": [],
                        "official_rubric": question.get("rubric"),
                    }
                )
        cases.append(
            {"id": str(row["conversation_id"]), "sessions": sessions, "questions": questions}
        )
    return cases


def adapt(suite: str, data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        raise ValueError("benchmark input must be a JSON array of objects")
    if suite == "locomo":
        return adapt_locomo(data)
    if suite == "longmemeval":
        return adapt_longmemeval(data)
    if suite == "beam":
        return adapt_beam(data)
    raise ValueError(f"unsupported suite: {suite}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", required=True, choices=["locomo", "longmemeval", "beam"])
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    normalized = adapt(args.suite, json.loads(args.input.read_text(encoding="utf-8")))
    if args.limit is not None:
        normalized = normalized[: args.limit]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(normalized, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
