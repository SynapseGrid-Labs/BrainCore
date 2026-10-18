import hashlib
import math
import re
from typing import Any, Literal

from fastapi import FastAPI
from pydantic import BaseModel, Field, model_validator

DIMENSIONS = 384
MODEL = "feature-hash-v1"
PREPROCESSING_REVISION = "token-v2"
PreprocessingRevision = Literal["token-v1", "token-v2"]
TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
CODEPOINT_PATTERN = re.compile(r"[a-z0-9]+|[^\x00-\x7f]")
ASCII_LOWERCASE = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")
# Fixed separators and explicit code-point ranges keep vectors identical across
# supported Python versions, independent of their Unicode database releases.
WHITESPACE = " \t\n\r\v\f\u0085\u00a0\u1680" + "".join(
    chr(value) for value in (*range(0x2000, 0x200B), 0x2028, 0x2029, 0x202F, 0x205F, 0x3000)
)


class EmbedRequest(BaseModel):
    inputs: list[str] = Field(min_length=1, max_length=100)
    model: Literal["feature-hash-v1"] = "feature-hash-v1"
    preprocessing_revision: PreprocessingRevision = "token-v2"

    @model_validator(mode="after")
    def bound_inputs(self) -> "EmbedRequest":
        if any(not value or len(value) > 100_000 for value in self.inputs):
            raise ValueError("Each embedding input must contain 1 to 100000 characters.")
        if sum(map(len, self.inputs)) > 500_000:
            raise ValueError("Embedding batches must not exceed 500000 characters.")
        return self


def stable_tokens(text: str) -> list[str]:
    # Preserve ASCII word features and add non-ASCII code-point features,
    # including combining marks and symbols, without runtime Unicode tables.
    return [
        token
        for token in CODEPOINT_PATTERN.findall(text.translate(ASCII_LOWERCASE))
        if token not in WHITESPACE
    ]


def feature_hash(
    text: str, preprocessing_revision: PreprocessingRevision = "token-v2"
) -> list[float]:
    vector = [0.0] * DIMENSIONS
    tokens = (
        TOKEN_PATTERN.findall(text.lower())
        if preprocessing_revision == "token-v1"
        else stable_tokens(text)
    )
    features = tokens + [
        f"{tokens[index]}::{tokens[index + 1]}" for index in range(len(tokens) - 1)
    ]
    for feature in features:
        digest = hashlib.blake2b(feature.encode(), digest_size=8).digest()
        bucket = int.from_bytes(digest[:4], "big") % DIMENSIONS
        sign = 1.0 if digest[4] & 1 else -1.0
        vector[bucket] += sign
    norm = math.sqrt(sum(value * value for value in vector))
    if not norm and preprocessing_revision == "token-v2" and text.strip(WHITESPACE):
        # Symbol-only evidence (or exact feature cancellation) still has a stable
        # representation. Preserve token-v1's original zero-vector behavior.
        digest = hashlib.blake2b(
            ("text:" + text.translate(ASCII_LOWERCASE)).encode(), digest_size=8
        ).digest()
        vector[int.from_bytes(digest[:4], "big") % DIMENSIONS] = 1.0
        norm = 1.0
    return [value / norm for value in vector] if norm else vector


app = FastAPI(title="BrainCore Local Embedder", version="2.0.0")


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "dimensions": DIMENSIONS,
        "algorithm": MODEL,
        "model": MODEL,
        "preprocessing_revision": PREPROCESSING_REVISION,
        "supported_preprocessing_revisions": ["token-v1", "token-v2"],
    }


@app.post("/embed")
def embed(body: EmbedRequest) -> dict[str, Any]:
    return {
        "embeddings": [feature_hash(value, body.preprocessing_revision) for value in body.inputs],
        "dimensions": DIMENSIONS,
        "algorithm": MODEL,
        "model": MODEL,
        "preprocessing_revision": body.preprocessing_revision,
    }
