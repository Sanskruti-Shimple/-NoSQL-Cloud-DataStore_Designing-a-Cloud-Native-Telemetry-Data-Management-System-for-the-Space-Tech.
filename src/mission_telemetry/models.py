from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

MetricName = Annotated[
    str, Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z][a-zA-Z0-9_.-]*$")
]


class TelemetryPacket(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    satellite_id: Annotated[str, Field(min_length=1, max_length=64)]
    module_id: Annotated[str, Field(min_length=1, max_length=64)]
    sensor_id: Annotated[str, Field(min_length=1, max_length=64)]
    metric: MetricName
    value: float = Field(allow_inf_nan=False)
    unit: Annotated[str, Field(min_length=1, max_length=16)]
    observed_at: datetime
    sequence: int = Field(ge=0)

    @field_validator("observed_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("observed_at must include a timezone")
        return value.astimezone(UTC)

    def as_document(self) -> dict[str, object]:
        document = self.model_dump(mode="json")
        document["_id"] = str(self.event_id)
        return document


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: Annotated[str, Field(min_length=1, max_length=128)]
    password: Annotated[str, Field(min_length=1, max_length=256)]


class TokenResponse(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: int


class Alert(BaseModel):
    alert_id: UUID = Field(default_factory=uuid4)
    satellite_id: str
    sensor_id: str
    metric: str
    value: float
    unit: str
    severity: Literal["warning", "critical"]
    message: str
    occurred_at: datetime


class TelemetryAccepted(BaseModel):
    event_id: UUID
    status: Literal["accepted"] = "accepted"


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    service: str = "mission-telemetry"
    mode: Literal["demo", "live"] = "live"
