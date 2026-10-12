from datetime import UTC, datetime, timedelta

import pytest

from mission_telemetry.models import Alert, TelemetryPacket
from mission_telemetry.pipeline import TelemetryPipeline
from mission_telemetry.settings import Settings
from mission_telemetry.stores import Platform


@pytest.mark.asyncio
async def test_demo_pipeline_processes_packets_without_kafka() -> None:
    settings = Settings(app_env="demo", demo_mode=True)
    platform = Platform(settings)
    pipeline = TelemetryPipeline(settings, platform)
    packet = TelemetryPacket(
        satellite_id="SAT-001",
        module_id="power",
        sensor_id="temperature-01",
        metric="temperature_c",
        value=96,
        unit="C",
        observed_at=datetime.now(UTC),
        sequence=1,
    )

    await platform.connect()
    await pipeline.start()
    try:
        await pipeline.publish(packet)
        assert pipeline._demo_queue is not None
        await pipeline._demo_queue.join()
        assert (await platform.latest("SAT-001", "temperature-01"))["event_id"] == str(
            packet.event_id
        )
        assert (await platform.alerts(10))[0]["severity"] == "critical"
    finally:
        await pipeline.close()


@pytest.mark.asyncio
async def test_demo_mode_projects_telemetry_without_external_databases() -> None:
    platform = Platform(Settings(app_env="demo", demo_mode=True))
    packet = TelemetryPacket(
        satellite_id="SAT-001",
        module_id="power",
        sensor_id="temperature-01",
        metric="temperature_c",
        value=96,
        unit="C",
        observed_at=datetime.now(UTC),
        sequence=1,
    )

    await platform.connect()
    await platform.persist(packet)
    stored = await platform.latest("SAT-001", "temperature-01")
    health = await platform.health_snapshot("SAT-001")
    history = await platform.history(
        "SAT-001",
        "temperature-01",
        packet.observed_at - timedelta(minutes=1),
        packet.observed_at + timedelta(minutes=1),
    )
    dependencies = await platform.dependencies("SAT-001")
    alert = Alert(
        satellite_id="SAT-001",
        sensor_id="temperature-01",
        metric="temperature_c",
        value=96,
        unit="C",
        severity="critical",
        message="High temperature",
        occurred_at=packet.observed_at,
    )

    assert stored is not None
    assert stored["event_id"] == str(packet.event_id)
    assert len(health) == 1
    assert len(history) == 1
    assert {node["type"] for node in dependencies["nodes"]} == {
        "Satellite",
        "Module",
        "Sensor",
    }
    assert await platform.record_alert(alert) is True
    assert await platform.record_alert(alert) is False
    assert (await platform.alerts(10))[0]["alert_id"] == str(alert.alert_id)
    assert await platform.rate_limit("test-client") == 1
