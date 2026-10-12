from datetime import UTC
from uuid import UUID, uuid5

from mission_telemetry.models import Alert, TelemetryPacket

ALERT_NAMESPACE = UUID("1b09e2a2-4c5a-40ce-86c5-a28e63819824")


def evaluate_alert(packet: TelemetryPacket) -> Alert | None:
    severity: str | None = None
    rule: str | None = None

    if packet.metric == "temperature_c":
        if packet.value >= 95:
            severity, rule = "critical", "critical temperature"
        elif packet.value >= 80:
            severity, rule = "warning", "high temperature"
    elif packet.metric == "battery_voltage_v":
        if packet.value <= 15:
            severity, rule = "critical", "critical battery voltage"
        elif packet.value <= 20:
            severity, rule = "warning", "low battery voltage"

    if severity is None or rule is None:
        return None

    alert_id = uuid5(ALERT_NAMESPACE, f"{packet.event_id}:{rule}")
    return Alert(
        alert_id=alert_id,
        satellite_id=packet.satellite_id,
        sensor_id=packet.sensor_id,
        metric=packet.metric,
        value=packet.value,
        unit=packet.unit,
        severity=severity,
        message=f"{rule.capitalize()}: {packet.value} {packet.unit}",
        occurred_at=packet.observed_at.astimezone(UTC),
    )
