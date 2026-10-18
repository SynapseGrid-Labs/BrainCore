import subprocess
import sys
from pathlib import Path


def test_mypy_checks_sdk_input_contract(tmp_path: Path) -> None:
    fixture = tmp_path / "consumer.py"
    fixture.write_text(
        "from braincore_memory import BrainCore\n"
        "client = BrainCore(api_key='synthetic-key')\n"
        "client.search('tea', {'user_id': 'alice'}, limit=10)\n"
    )
    command = [sys.executable, "-m", "mypy", "--follow-imports=silent", "--strict", str(fixture)]
    valid = subprocess.run(  # noqa: S603
        command,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert valid.returncode == 0, valid.stdout + valid.stderr
    fixture.write_text(fixture.read_text() + "client.search('tea', limit='ten')\n")
    invalid = subprocess.run(  # noqa: S603
        command,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert invalid.returncode == 1, invalid.stdout + invalid.stderr
    assert "[arg-type]" in invalid.stdout
    assert "limit" in invalid.stdout
