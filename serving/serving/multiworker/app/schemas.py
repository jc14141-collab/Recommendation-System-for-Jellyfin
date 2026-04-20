from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

EMBEDDING_DIM = 384


class CandidateMovie(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, protected_namespaces=())

    movie_id: str = Field(..., description="Movie identifier")
    movie_embedding: List[float] = Field(..., description="Movie embedding vector")

    @field_validator("movie_embedding")
    @classmethod
    def validate_movie_embedding(cls, v: List[float]) -> List[float]:
        if len(v) != EMBEDDING_DIM:
            raise ValueError(
                f"movie_embedding must have length {EMBEDDING_DIM}, got {len(v)}"
            )
        return v


class ClientContext(BaseModel):
    surface: Optional[str] = Field(default="homepage")
    device_type: Optional[str] = Field(default="web")
    app_version: Optional[str] = Field(default="0.1.0")


class RecommendRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, protected_namespaces=())

    request_id: str
    user_id: str
    timestamp: str
    request_k: int = Field(default=10, ge=1, le=500)
    user_embedding: List[float] = Field(..., description="User embedding vector")
    candidates: List[CandidateMovie] = Field(default_factory=list)
    client_context: Optional[ClientContext] = Field(default_factory=ClientContext)

    @field_validator("user_embedding")
    @classmethod
    def validate_user_embedding(cls, v: List[float]) -> List[float]:
        if len(v) != EMBEDDING_DIM:
            raise ValueError(
                f"user_embedding must have length {EMBEDDING_DIM}, got {len(v)}"
            )
        return v


class RecommendationItem(BaseModel):
    rank: int = Field(..., ge=1)
    movie_id: str
    score: float
    reason: str


class RecommendResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    request_id: str
    user_id: str
    timestamp: str
    model_version: str
    fallback_used: bool
    latency_ms: float = Field(..., ge=0.0)
    recommendations: List[RecommendationItem]