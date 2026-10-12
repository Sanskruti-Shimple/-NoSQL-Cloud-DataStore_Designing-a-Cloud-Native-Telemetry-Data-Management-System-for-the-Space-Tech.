from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from mission_telemetry.alerts import evaluate_alert
from mission_telemetry.models import TelemetryPacket


def packet(metric: str, value: float) -> TelemetryPacket:
    return TelemetryPacket(
        satellite_id="SAT-001",
        module_id="power",
        sensor_id="sensor-01",
        metric=metric,
        value=value,
        unit="C",
        observed_at=datetime.now(UTC),
        sequence=1,
    )


@pytest.mark.parametrize(
    ("value", "severity"),
    [(79.9, None), (80, "warning"), (94.9, "warning"), (95, "critical")],
)
def test_temperature_alert_thresholds(value: float, severity: str | None) -> None:
    alert = evaluate_alert(packet("temperature_c", value))
    assert (alert.severity if alert else None) == severity


@pytest.mark.parametrize(
    ("value", "severity"),
    [(20.1, None), (20, "warning"), (15.1, "warning"), (15, "critical")],
)
def test_battery_alert_thresholds(value: float, severity: str | None) -> None:
    alert = evaluate_alert(packet("battery_voltage_v", value))
    assert (alert.severity if alert else None) == severity


def test_alert_id_is_stable_for_replayed_event() -> None:
    telemetry = packet("temperature_c", 96)
    assert evaluate_alert(telemetry).alert_id == evaluate_alert(telemetry).alert_id


def test_unknown_metric_does_not_generate_an_alert() -> None:
    assert evaluate_alert(packet("solar_current_a", 42)) is None


def test_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        TelemetryPacket(
            satellite_id="SAT-001",
            module_id="power",
            sensor_id="sensor-01",
            metric="temperature_c",
            value=20,
            unit="C",
            observed_at=datetime(2026, 1, 1),
            sequence=1,
        )


def test_telemetry_event_id_can_be_supplied_for_deduplication() -> None:
    event_id = uuid4()
    telemetry = TelemetryPacket(
        event_id=event_id,
        satellite_id="SAT-001",
        module_id="power",
        sensor_id="sensor-01",
        metric="temperature_c",
        value=20,
        unit="C",
        observed_at=datetime.now(UTC),
        sequence=1,
    )
    assert telemetry.event_id == event_id
