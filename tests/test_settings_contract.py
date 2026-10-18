from pathlib import Path

import pytest
from pydantic import ValidationError

from braincore_memory.config import Settings


def test_settings_environment_overrides_dotenv_without_disclosing_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "BRAINCORE_PORT=9090\n"
        "BRAINCORE_BOOTSTRAP_ADMIN_KEY=synthetic-file-secret\n"
        "BRAINCORE_DATABASE_URL=postgresql://example:synthetic-password@localhost/example\n"
        "BRAINCORE_SEARCH_VECTOR_MODE=planner\n"
    )
    monkeypatch.setenv("BRAINCORE_PORT", "9091")
    monkeypatch.setenv("BRAINCORE_BOOTSTRAP_ADMIN_KEY", "synthetic-env-secret")
    monkeypatch.delenv("BRAINCORE_DATABASE_URL", raising=False)
    monkeypatch.delenv("BRAINCORE_SEARCH_VECTOR_MODE", raising=False)
    settings = Settings(_env_file=dotenv)
    assert settings.port == 9091
    assert settings.search_vector_mode == "iterative_hnsw"
    assert settings.bootstrap_admin_key is not None
    assert settings.bootstrap_admin_key.get_secret_value() == "synthetic-env-secret"
    rendered = repr(settings) + settings.model_dump_json()
    for secret in ("synthetic-file-secret", "synthetic-env-secret", "synthetic-password"):
        assert secret not in rendered
    assert Settings(port=9092, _env_file=dotenv).port == 9092


def test_settings_environment_rejects_invalid_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRAINCORE_PORT", "70000")
    with pytest.raises(ValidationError, match="port"):
        Settings(_env_file=None)
