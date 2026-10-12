from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from mission_telemetry import api
from mission_telemetry.models import TelemetryPacket
from mission_telemetry.settings import Settings

MISSION_PASSWORD = "test-mission-password"
SCIENTIST_PASSWORD = "test-scientist-password"


class FakePlatform:
    def __init__(self, settings: Settings) -> None:
        pass

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def rate_limit(self, client_id: str) -> int:
        return 1

    async def latest(self, satellite_id: str, sensor_id: str) -> dict[str, Any]:
        return {
            "satellite_id": satellite_id,
            "sensor_id": sensor_id,
            "value": 23.5,
        }

    async def history(
        self, satellite_id: str, sensor_id: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        return []


class FakePipeline:
    published: list[TelemetryPacket]

    def __init__(self, settings: Settings, platform: FakePlatform) -> None:
        self.published = []

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def publish(self, packet: TelemetryPacket) -> None:
        self.published.append(packet)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    settings = Settings(
        jwt_secret=SecretStr("test-only-secret-material-longer-than-32-characters"),
        mission_username="operator",
        mission_password=SecretStr(MISSION_PASSWORD),
        scientist_username="scientist",
        scientist_password=SecretStr(SCIENTIST_PASSWORD),
        max_history_days=2,
    )
    monkeypatch.setattr(api, "get_settings", lambda: settings)
    monkeypatch.setattr(api, "Platform", FakePlatform)
    monkeypatch.setattr(api, "TelemetryPipeline", FakePipeline)
    return TestClient(api.app)


def token_for(client: TestClient, username: str, password: str) -> str:
    response = client.post(
        "/v1/auth/token",
        json={"username": username, "password": password},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


def test_dashboard_and_static_assets_are_served(client: TestClient) -> None:
    with client:
        dashboard = client.get("/")
        script = client.get("/assets/app.js")
        stylesheet = client.get("/assets/styles.css")
        assert dashboard.status_code == 200
        assert "Satellite telemetry" in dashboard.text
        assert 'id="pipeline-mode"' in dashboard.text
        assert script.status_code == 200
        assert "function refreshSatellite" in script.text
        assert "DEMO · IN-MEMORY" in script.text
        assert "waitForTelemetry" in script.text
        assert stylesheet.status_code == 200
        assert ".workspace" in stylesheet.text


def test_login_rejects_invalid_password(client: TestClient) -> None:
    with client:
        response = client.post(
            "/v1/auth/token",
            json={"username": "operator", "password": "incorrect-password"},
        )
        assert response.status_code == 401


def test_scientist_can_read_but_cannot_ingest(client: TestClient) -> None:
    with client:
        token = token_for(client, "scientist", SCIENTIST_PASSWORD)
        headers = {"Authorization": f"Bearer {token}"}

        reading = client.get(
            "/v1/satellites/SAT-001/sensors/temp-01/latest",
            headers=headers,
        )
        assert reading.status_code == 200
        assert reading.json()["value"] == 23.5

        rejected = client.post(
            "/v1/telemetry",
            headers=headers,
            json={
                "satellite_id": "SAT-001",
                "module_id": "power",
                "sensor_id": "temp-01",
                "metric": "temperature_c",
                "value": 23.5,
                "unit": "C",
                "observed_at": datetime.now(UTC).isoformat(),
                "sequence": 1,
            },
        )
        assert rejected.status_code == 403


def test_mission_control_can_enqueue_validated_packets(client: TestClient) -> None:
    with client:
        token = token_for(client, "operator", MISSION_PASSWORD)
        event_id = "f1100581-4602-4b90-a714-5c46f1a48a7a"
        response = client.post(
            "/v1/telemetry",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "event_id": event_id,
                "satellite_id": "SAT-001",
                "module_id": "power",
                "sensor_id": "temp-01",
                "metric": "temperature_c",
                "value": 23.5,
                "unit": "C",
                "observed_at": datetime.now(UTC).isoformat(),
                "sequence": 1,
            },
        )
        assert response.status_code == 202
        assert response.json() == {"event_id": event_id, "status": "accepted"}
        pipeline: FakePipeline = client.app.state.pipeline
        assert len(pipeline.published) == 1
        assert str(pipeline.published[0].event_id) == event_id


def test_telemetry_validation_rejects_unknown_fields(client: TestClient) -> None:
    with client:
        token = token_for(client, "operator", MISSION_PASSWORD)
        response = client.post(
            "/v1/telemetry",
            headers={"Authorization": f"Bearer {token}"},
            json={"unexpected_field": "not-a-packet"},
        )
        assert response.status_code == 422


def test_history_limit_counts_utc_calendar_buckets(client: TestClient) -> None:
    with client:
        token = token_for(client, "scientist", SCIENTIST_PASSWORD)
        response = client.get(
            "/v1/satellites/SAT-001/sensors/temp-01/history",
            params={
                "start": "2026-01-01T01:00:00+14:00",
                "end": "2026-01-02T23:00:00-12:00",
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 400
