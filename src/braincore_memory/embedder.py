import hashlib
import math
import re
from typing import Any, Literal

from fastapi import FastAPI
from pydantic import BaseModel, Field, model_validator

DIMENSIONS = 384
MODEL = "feature-hash-v1"
PREPROCESSING_REVISION = "token-v1"
TOKEN_PATTERN = re.compile(r"[a-z0-9]+")


class EmbedRequest(BaseModel):
    inputs: list[str] = Field(min_length=1, max_length=100)
    model: Literal["feature-hash-v1"] = "feature-hash-v1"
    preprocessing_revision: Literal["token-v1"] = "token-v1"

    @model_validator(mode="after")
    def bound_inputs(self) -> "EmbedRequest":
        if any(not value or len(value) > 100_000 for value in self.inputs):
            raise ValueError("Each embedding input must contain 1 to 100000 characters.")
        if sum(map(len, self.inputs)) > 500_000:
            raise ValueError("Embedding batches must not exceed 500000 characters.")
        return self


def feature_hash(text: str) -> list[float]:
    vector = [0.0] * DIMENSIONS
    tokens = TOKEN_PATTERN.findall(text.lower())
    features = tokens + [
        f"{tokens[index]}::{tokens[index + 1]}" for index in range(len(tokens) - 1)
    ]
    for feature in features:
        digest = hashlib.blake2b(feature.encode(), digest_size=8).digest()
        bucket = int.from_bytes(digest[:4], "big") % DIMENSIONS
        sign = 1.0 if digest[4] & 1 else -1.0
        vector[bucket] += sign
    norm = math.sqrt(sum(value * value for value in vector))
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
    }


@app.post("/embed")
def embed(body: EmbedRequest) -> dict[str, Any]:
    return {
        "embeddings": [feature_hash(value) for value in body.inputs],
        "dimensions": DIMENSIONS,
        "algorithm": MODEL,
        "model": MODEL,
        "preprocessing_revision": PREPROCESSING_REVISION,
    }
