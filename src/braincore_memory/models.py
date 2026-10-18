from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .request_validation import validate_storage_values


class StrictModel(BaseModel):
    """Reject misspelled or unsupported fields at every public model boundary."""

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def validate_request_values(cls, value: Any) -> Any:
        return validate_storage_values(value)


class Scope(StrictModel):
    project_id: str | None = Field(default=None, min_length=1, max_length=200)
    user_id: str | None = Field(default=None, min_length=1, max_length=200)
    agent_id: str | None = Field(default=None, min_length=1, max_length=200)
    run_id: str | None = Field(default=None, min_length=1, max_length=200)

    @model_validator(mode="after")
    def require_value(self) -> "Scope":
        if not any((self.project_id, self.user_id, self.agent_id, self.run_id)):
            raise ValueError("at least one scope field is required")
        return self


class EvidenceInput(StrictModel):
    source_type: str = Field(default="statement", min_length=1, max_length=80)
    source_uri: str | None = Field(default=None, max_length=2048)
    quote: str | None = Field(default=None, max_length=100_000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MemoryReference(StrictModel):
    namespace: list[str] = Field(min_length=1, max_length=20)
    key: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_namespace(self) -> "MemoryReference":
        if any(not part or len(part) > 200 for part in self.namespace):
            raise ValueError("reference namespace parts must contain 1 to 200 characters")
        return self


class MemoryResolveRequest(MemoryReference):
    scope: Scope | None = None


class RememberRequest(StrictModel):
    content: str = Field(min_length=1, max_length=100_000)
    scope: Scope
    evidence: EvidenceInput = Field(default_factory=EvidenceInput)
    valid_from: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    reference: MemoryReference | None = None

    @field_validator("content")
    @classmethod
    def require_nonblank_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Memory content must contain a non-whitespace character.")
        return value

    @model_validator(mode="after")
    def validate_evidence_quote(self) -> "RememberRequest":
        if self.evidence.quote is not None and self.evidence.quote not in self.content:
            raise ValueError("evidence quote must be an exact substring of content")
        return self


class IngestRequest(StrictModel):
    content: str = Field(min_length=1, max_length=500_000)
    scope: Scope
    source_type: str = Field(default="conversation", max_length=80)
    source_uri: str | None = Field(default=None, max_length=2048)
    metadata: dict[str, Any] = Field(default_factory=dict)


class BatchRememberRequest(StrictModel):
    items: list[RememberRequest] = Field(min_length=1, max_length=100)


class BatchIngestRequest(StrictModel):
    items: list[IngestRequest] = Field(min_length=1, max_length=100)


class RawBatchRequest(StrictModel):
    items: list[dict[str, Any]] = Field(min_length=1, max_length=100)

    @model_validator(mode="before")
    @classmethod
    def validate_request_values(cls, value: Any) -> Any:
        # Typed item validation owns failures so a malformed item cannot reject
        # valid siblings. The raw body is retained for idempotent replay/conflicts.
        return value


class OperationAccepted(StrictModel):
    operation_id: UUID
    state: str
    reused: bool = False


class RevisionRequest(StrictModel):
    content: str = Field(min_length=1, max_length=100_000)
    reason: str = Field(min_length=1, max_length=1000)

    @field_validator("reason")
    @classmethod
    def require_nonblank_reason(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Audit reason must contain a non-whitespace character.")
        return value

    @field_validator("content")
    @classmethod
    def require_nonblank_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Memory content must contain a non-whitespace character.")
        return value


class LifecycleRequest(StrictModel):
    action: Literal["suppress", "retire", "restore", "promote"]
    reason: str = Field(min_length=1, max_length=1000)

    @field_validator("reason")
    @classmethod
    def require_nonblank_reason(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Audit reason must contain a non-whitespace character.")
        return value


class FeedbackRequest(StrictModel):
    rating: Literal[-1, 0, 1]
    comment: str | None = Field(default=None, max_length=2000)


class SearchRequest(StrictModel):
    query: str | None = Field(default=None, min_length=1, max_length=10_000)
    scope: Scope | None = None
    limit: int = Field(default=10, ge=1, le=100)
    offset: int = Field(default=0, ge=0, le=10_000)
    include_inactive: bool = False
    object_type: Literal["entity", "fact", "memory", "segment", "episode"] | None = None
    legacy_scope_prefix: str | None = Field(default=None, min_length=1, max_length=2048)
    as_of: datetime | None = None
    reference_namespace_prefix: list[str] | None = Field(default=None, min_length=1, max_length=20)
    metadata_filter: dict[str, Any] | None = None


class SearchStreamStatus(StrictModel):
    state: Literal["ok", "failed", "disabled"]
    warning: str | None = None


class SearchDiagnostics(StrictModel):
    timings_ms: dict[str, float]
    embedding_cache_hit: bool


class SearchResponse(StrictModel):
    results: list[dict[str, Any]]
    streams: dict[str, SearchStreamStatus]
    warnings: list[str]
    retrieval_status: Literal["complete", "degraded"]
    diagnostics: SearchDiagnostics | None = None


class EmbeddingRefreshRequest(StrictModel):
    scope: Scope | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    scan_limit: int = Field(default=1000, ge=1, le=10000)
    after_id: UUID | None = None


class EmbeddingRefreshAccepted(OperationAccepted):
    scanned_items: int
    next_cursor: UUID | None = None
    rescan_recommended: bool = False


class ErrorDetail(StrictModel):
    code: str
    message: str
    request_id: str
    retryable: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorEnvelope(StrictModel):
    error: ErrorDetail


class ApiKeyCreateRequest(StrictModel):
    name: str = Field(min_length=1, max_length=100)
    roles: set[Literal["read", "write", "admin"]] = Field(min_length=1)
    scope: Scope | None = None
    expires_at: datetime | None = None

    @model_validator(mode="after")
    def validate_scope(self) -> "ApiKeyCreateRequest":
        if "admin" in self.roles and self.scope is not None:
            raise ValueError("admin keys are tenant-wide and cannot be scope-bound")
        if "admin" not in self.roles and self.scope is None:
            raise ValueError("non-admin keys must be scope-bound")
        return self


class EvidenceGrantRequest(StrictModel):
    grantee_tenant_id: UUID
    scope: Scope
    reason: str = Field(min_length=1, max_length=2000)
    expires_at: datetime | None = None


class EvidenceGrantRevokeRequest(StrictModel):
    reason: str = Field(min_length=1, max_length=2000)
