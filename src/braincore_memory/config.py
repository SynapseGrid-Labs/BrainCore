from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BRAINCORE_", env_file=".env", extra="ignore")

    # Local development may use peer/trust authentication, but the application
    # never ships a reusable database password. Compose always overrides this
    # value with the generated installation secret.
    database_url: SecretStr = SecretStr("postgresql://braincore@127.0.0.1:5432/braincore")
    bootstrap_admin_key: SecretStr | None = None
    embedder_url: str = "http://127.0.0.1:8080"
    embedding_model: str = Field(default="feature-hash-v1", min_length=1, max_length=200)
    embedding_preprocessing_revision: str = Field(default="token-v1", min_length=1, max_length=200)
    embedding_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    embedding_max_concurrency: int = Field(default=8, ge=1, le=64)
    embedding_cache_max_entries: int = Field(default=256, ge=0, le=10_000)
    embedding_cache_ttl_seconds: float = Field(default=60.0, ge=0, le=3600)
    search_vector_mode: Literal["planner", "exact", "iterative_hnsw"] = "iterative_hnsw"
    search_full_text_mode: Literal["canonical", "projection"] = "projection"
    search_hnsw_ef_search: int = Field(default=1000, ge=1, le=1000)
    search_hnsw_max_scan_tuples: int = Field(default=20_000, ge=1, le=10_000_000)
    search_hnsw_scan_mem_multiplier: float = Field(default=1.0, ge=1, le=100)
    search_diagnostics: bool = False
    llm_base_url: str | None = None
    llm_api_key: SecretStr | None = None
    llm_model: str | None = None
    allowed_origins: str = "http://127.0.0.1:8787"
    bind_host: str = "127.0.0.1"
    port: int = Field(default=8787, ge=1, le=65535)
    max_body_bytes: int = Field(default=1_048_576, ge=1024, le=33_554_432)
    session_ttl_seconds: int = Field(default=3600, ge=300, le=86400)
    cookie_secure: bool = False

    @field_validator("search_vector_mode")
    @classmethod
    def normalize_vector_mode(cls, value: str) -> str:
        # Keep older installations compatible without restoring the unindexed query.
        return "iterative_hnsw" if value == "planner" else value

    @property
    def origins(self) -> list[str]:
        origins = [value.strip() for value in self.allowed_origins.split(",") if value.strip()]
        if not origins or "*" in origins:
            raise ValueError("BRAINCORE_ALLOWED_ORIGINS must contain explicit origins")
        if any(not value.startswith(("http://", "https://")) for value in origins):
            raise ValueError("BRAINCORE_ALLOWED_ORIGINS values must use http or https")
        return origins


@lru_cache
def get_settings() -> Settings:
    return Settings()
