import json
import logging
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger("mission_telemetry.audit")


def record_audit_event(event: str, *, level: int = logging.INFO, **attributes: Any) -> None:
    entry = {
        "event": event,
        "occurred_at": datetime.now(UTC).isoformat(),
        **attributes,
    }
    logger.log(level, json.dumps(entry, separators=(",", ":"), sort_keys=True))
