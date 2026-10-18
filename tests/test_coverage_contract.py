import json
import os
import subprocess
import sys
from pathlib import Path


def test_coverage_plugin_reports_executed_and_missing_branches(tmp_path: Path) -> None:
    (tmp_path / "sample.py").write_text(
        "def choose(value):\n    if value:\n        return 1\n    return 0\n"
    )
    (tmp_path / "test_sample.py").write_text(
        "from sample import choose\ndef test_positive():\n    assert choose(True) == 1\n"
    )
    env = {key: value for key, value in os.environ.items() if not key.startswith("COV_CORE_")}
    env.pop("COVERAGE_PROCESS_START", None)
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "--cov=sample",
            "--cov-branch",
            "--cov-report=json:coverage.json",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads((tmp_path / "coverage.json").read_text())
    measured = report["files"]["sample.py"]
    assert measured["executed_lines"] == [1, 2, 3]
    assert measured["missing_lines"] == [4]
    assert measured["summary"]["num_branches"] == 2
    assert measured["summary"]["covered_branches"] == 1
