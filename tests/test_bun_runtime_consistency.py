"""Exercise supported Bun versions consistently across build and verification."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ("ci.yml", "release-candidate.yml")


def assert_verification_uses_image_bun(
    dockerfile: str, workflows: list[str], package_script: str, contributing: str
) -> None:
    version = re.search(r"^FROM oven/bun:([0-9.]+)-alpine@", dockerfile, re.M)
    assert version is not None
    expected = version.group(1)
    for workflow in workflows:
        configured = re.findall(r"bun-version: ([0-9.]+)", workflow)
        assert configured and all(value == expected for value in configured), (
            "Hosted verification must use the container Bun runtime"
        )
    audited = re.findall(r"bun x bun@([0-9.]+) audit", package_script)
    assert audited == [expected], "Package audit must use the container Bun runtime"
    documented = re.findall(r"bun x bun@([0-9.]+) audit", contributing)
    assert documented == [expected], "Contributor audit command must use the container Bun runtime"


def configurations() -> tuple[str, list[str], str, str]:
    return (
        (ROOT / "Dockerfile").read_text(),
        [(ROOT / ".github" / "workflows" / name).read_text() for name in WORKFLOWS],
        (ROOT / "scripts" / "test-packages.sh").read_text(),
        (ROOT / "CONTRIBUTING.md").read_text(),
    )


def test_hosted_verification_and_package_audit_use_image_bun() -> None:
    assert_verification_uses_image_bun(*configurations())


@pytest.mark.parametrize("configuration", ["ci", "release-candidate", "package-audit"])
def test_stale_verification_runtime_is_rejected(configuration: str) -> None:
    dockerfile, workflows, package_script, contributing = configurations()
    if configuration == "package-audit":
        package_script = re.sub(r"bun x bun@[0-9.]+ audit", "bun x bun@0.0.0 audit", package_script)
        message = "Package audit"
    else:
        index = 0 if configuration == "ci" else 1
        workflows[index] = re.sub(
            r"bun-version: [0-9.]+", "bun-version: 0.0.0", workflows[index], count=1
        )
        message = "Hosted verification"
    with pytest.raises(AssertionError, match=message):
        assert_verification_uses_image_bun(dockerfile, workflows, package_script, contributing)


@pytest.mark.parametrize("command", ["bun x bun@1.3.8 audit", ""])
def test_stale_or_missing_contributor_audit_command_is_rejected(command: str) -> None:
    dockerfile, workflows, package_script, contributing = configurations()
    contributing = re.sub(r"bun x bun@[0-9.]+ audit", command, contributing)
    with pytest.raises(AssertionError, match="Contributor audit command"):
        assert_verification_uses_image_bun(dockerfile, workflows, package_script, contributing)
