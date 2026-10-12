import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import Response

from mission_telemetry.audit import record_audit_event
from mission_telemetry.models import (
    Alert,
    HealthResponse,
    LoginRequest,
    TelemetryAccepted,
    TelemetryPacket,
    TokenResponse,
)
from mission_telemetry.pipeline import TelemetryPipeline
from mission_telemetry.security import (
    enforce_rate_limit,
    get_platform,
    login,
    password_hash,
    require_roles,
)
from mission_telemetry.settings import Settings, get_settings
from mission_telemetry.stores import Platform, close_platform

operator = Depends(require_roles("mission-control"))
reader = Depends(require_roles("mission-control", "scientist"))
router = APIRouter(prefix="/v1", dependencies=[Depends(enforce_rate_limit)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    platform = Platform(settings)
    pipeline = TelemetryPipeline(settings, platform)
    app.state.settings = settings
    app.state.platform = platform
    app.state.pipeline = pipeline
    app.state.users = {
        settings.mission_username: ("mission-control", "mission-control"),
        settings.scientist_username: ("scientist", "scientist"),
    }
    app.state.password_hashes = {
        "mission-control": password_hash.hash(settings.mission_password.get_secret_value()),
        "scientist": password_hash.hash(settings.scientist_password.get_secret_value()),
    }
    try:
        await platform.connect()
        await pipeline.start()
        yield
    finally:
        await pipeline.close()
        await close_platform(platform)


app = FastAPI(
    title="Mission Telemetry Platform",
    description="Secure real-time satellite telemetry ingestion, analytics, and alerting.",
    version="1.0.0",
    lifespan=lifespan,
)
app.mount(
    "/assets",
    StaticFiles(directory=Path(__file__).parent / "static"),
    name="assets",
)


@app.middleware("http")
async def observe_requests(request: Request, call_next: RequestResponseEndpoint) -> Response:
    started = perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        record_audit_event(
            "http.request_failed",
            level=logging.ERROR,
            method=request.method,
            path=request.url.path,
            duration_ms=round((perf_counter() - started) * 1000, 2),
        )
        raise
    duration_ms = round((perf_counter() - started) * 1000, 2)
    if response.status_code >= 500 or duration_ms >= 1000:
        record_audit_event(
            "http.request_observed",
            level=logging.ERROR if response.status_code >= 500 else logging.WARNING,
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms,
        )
    return response


@app.get("/", include_in_schema=False, tags=["dashboard"])
async def dashboard() -> FileResponse:
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/health", response_model=HealthResponse, tags=["operations"])
async def health(request: Request) -> HealthResponse:
    settings: Settings = request.app.state.settings
    return HealthResponse(mode="demo" if settings.demo_mode else "live")


@router.post("/auth/token", response_model=TokenResponse, tags=["security"])
async def create_token(request: Request, credentials: LoginRequest) -> TokenResponse:
    return await login(request, credentials)


@router.post(
    "/telemetry",
    response_model=TelemetryAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["telemetry"],
)
async def ingest_telemetry(
    packet: TelemetryPacket,
    request: Request,
    _: Annotated[dict[str, str], operator],
) -> TelemetryAccepted:
    pipeline: TelemetryPipeline = request.app.state.pipeline
    await pipeline.publish(packet)
    principal: dict[str, str] = request.state.principal
    record_audit_event(
        "telemetry.packet_enqueued",
        username=principal["username"],
        role=principal["role"],
        event_id=str(packet.event_id),
        satellite_id=packet.satellite_id,
    )
    return TelemetryAccepted(event_id=packet.event_id)


@router.get("/satellites/{satellite_id}/health", tags=["telemetry"])
async def satellite_health(
    satellite_id: str,
    platform: Annotated[Platform, Depends(get_platform)],
    _: Annotated[dict[str, str], reader],
) -> dict[str, object]:
    readings = await platform.health_snapshot(satellite_id)
    return {"satellite_id": satellite_id, "readings": readings}


@router.get("/satellites/{satellite_id}/sensors/{sensor_id}/latest", tags=["telemetry"])
async def latest_telemetry(
    satellite_id: str,
    sensor_id: str,
    platform: Annotated[Platform, Depends(get_platform)],
    _: Annotated[dict[str, str], reader],
) -> dict[str, object]:
    packet = await platform.latest(satellite_id, sensor_id)
    if packet is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Telemetry not found")
    return packet


@router.get("/satellites/{satellite_id}/sensors/{sensor_id}/history", tags=["telemetry"])
async def telemetry_history(
    satellite_id: str,
    sensor_id: str,
    start: datetime,
    end: datetime,
    request: Request,
    platform: Annotated[Platform, Depends(get_platform)],
    _: Annotated[dict[str, str], reader],
) -> dict[str, object]:
    if start.tzinfo is None or end.tzinfo is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="start and end must include timezone offsets",
        )
    start_utc = start.astimezone(UTC)
    end_utc = end.astimezone(UTC)
    if end_utc < start_utc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="end must be greater than or equal to start",
        )
    settings: Settings = request.app.state.settings
    if (end_utc.date() - start_utc.date()).days + 1 > settings.max_history_days:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"History range cannot exceed {settings.max_history_days} calendar days",
        )
    readings = await platform.history(satellite_id, sensor_id, start_utc, end_utc)
    return {
        "satellite_id": satellite_id,
        "sensor_id": sensor_id,
        "start": start,
        "end": end,
        "readings": readings,
    }


@router.get("/satellites/{satellite_id}/dependencies", tags=["systems"])
async def satellite_dependencies(
    satellite_id: str,
    platform: Annotated[Platform, Depends(get_platform)],
    _: Annotated[dict[str, str], reader],
) -> dict[str, object]:
    return await platform.dependencies(satellite_id)


@router.get("/alerts", response_model=list[Alert], tags=["alerts"])
async def list_alerts(
    platform: Annotated[Platform, Depends(get_platform)],
    _: Annotated[dict[str, str], reader],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    satellite_id: str | None = None,
) -> list[Alert]:
    alerts = await platform.alerts(limit)
    if satellite_id is not None:
        alerts = [alert for alert in alerts if alert["satellite_id"] == satellite_id]
    return [Alert.model_validate(alert) for alert in alerts]


app.include_router(router)
