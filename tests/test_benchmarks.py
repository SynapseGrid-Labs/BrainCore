import json
from pathlib import Path

from benchmarks.adapter import adapt_beam, adapt_locomo, adapt_longmemeval


def test_locomo_adapter_preserves_sessions_and_evidence_ids() -> None:
    normalized = adapt_locomo(
        [
            {
                "sample_id": "sample-1",
                "conversation": {
                    "speaker_a": "Alice",
                    "speaker_b": "Bob",
                    "session_1": [{"speaker": "Alice", "dia_id": "D1:1", "text": "I like tea."}],
                    "session_1_date_time": "2026-01-01",
                },
                "qa": [
                    {
                        "question": "What does Alice like?",
                        "answer": "tea",
                        "evidence": ["D1:1"],
                        "category": 1,
                    }
                ],
            }
        ]
    )
    assert normalized[0]["sessions"][0]["id"] == "D1"
    assert normalized[0]["questions"][0]["evidence_session_ids"] == ["D1"]


def test_longmemeval_adapter_preserves_official_question_and_answer_sessions() -> None:
    normalized = adapt_longmemeval(
        [
            {
                "question_id": "q1",
                "question": "Where?",
                "answer": "Phoenix",
                "question_type": "single-session-user",
                "haystack_session_ids": ["s1"],
                "haystack_dates": ["2026-01-01"],
                "haystack_sessions": [[{"role": "user", "content": "I moved to Phoenix."}]],
                "answer_session_ids": ["s1"],
            }
        ]
    )
    assert normalized[0]["questions"][0]["id"] == "q1"
    assert normalized[0]["questions"][0]["evidence_session_ids"] == ["s1"]


def test_beam_adapter_handles_dataset_viewer_serialized_fields() -> None:
    normalized = adapt_beam(
        [
            {
                "conversation_id": "1",
                "chat": json.dumps(
                    [[{"role": "user", "content": "Fact", "id": 1, "time_anchor": "now"}]]
                ),
                "probing_questions": repr(
                    {"information_extraction": [{"question": "What fact?", "rubric": "Fact"}]}
                ),
            }
        ]
    )
    assert normalized[0]["sessions"][0]["messages"][0]["content"] == "Fact"
    assert normalized[0]["questions"][0]["category"] == "information_extraction"


def test_benchmark_manifests_pin_sources_without_redistributing_data() -> None:
    root = Path(__file__).resolve().parents[1]
    manifests = [
        json.loads(path.read_text()) for path in (root / "benchmarks/manifests").glob("*.json")
    ]
    assert len(manifests) == 3
    assert all(manifest["source_commit"] for manifest in manifests)
    assert all(manifest["redistributed"] is False for manifest in manifests)
