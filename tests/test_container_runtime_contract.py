"""Keep advertised container runtimes aligned with the shipped build stages."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def assert_documented_container_runtimes(dockerfile: str, readme: str) -> None:
    python = re.findall(
        r"^FROM python:([0-9.]+)-alpine@sha256:[a-f0-9]{64} AS full$", dockerfile, re.M
    )
    bun = re.findall(
        r"^FROM oven/bun:([0-9.]+)-alpine@sha256:[a-f0-9]{64} AS (?:bun-runtime|dashboard-build)$",
        dockerfile,
        re.M,
    )
    assert len(python) == 1, "Expected one digest-pinned Python runtime stage"
    assert len(bun) == 2 and len(set(bun)) == 1, "Bun runtime and dashboard builder must agree"
    assert f"The container uses Python {python[0]} and Bun {bun[0]}." in readme, (
        "README container runtime versions must match the Dockerfile"
    )


def test_documented_container_runtimes_match_image() -> None:
    assert_documented_container_runtimes(
        (ROOT / "Dockerfile").read_text(), (ROOT / "README.md").read_text()
    )


@pytest.mark.parametrize("runtime", ["Python", "Bun"])
def test_outdated_container_runtime_documentation_is_rejected(runtime: str) -> None:
    readme = re.sub(rf"({runtime} )[0-9.]+", r"\g<1>0.0.0", (ROOT / "README.md").read_text())
    with pytest.raises(AssertionError, match="README container runtime versions"):
        assert_documented_container_runtimes((ROOT / "Dockerfile").read_text(), readme)


def test_mixed_bun_builder_and_runtime_versions_are_rejected() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    dockerfile = re.sub(r"(FROM oven/bun:)[0-9.]+", r"\g<1>0.0.0", dockerfile, count=1)
    with pytest.raises(AssertionError, match="Bun runtime and dashboard builder"):
        assert_documented_container_runtimes(dockerfile, (ROOT / "README.md").read_text())
