from datetime import UTC, datetime, timedelta
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pwdlib import PasswordHash

from mission_telemetry.audit import record_audit_event
from mission_telemetry.models import LoginRequest, TokenResponse
from mission_telemetry.settings import Settings
from mission_telemetry.stores import Platform

password_hash = PasswordHash.recommended()
bearer = HTTPBearer(auto_error=False)


def create_access_token(settings: Settings, username: str, role: str) -> TokenResponse:
    now = datetime.now(UTC)
    expires = now + timedelta(minutes=settings.token_ttl_minutes)
    token = jwt.encode(
        {
            "sub": username,
            "role": role,
            "iat": now,
            "exp": expires,
        },
        settings.jwt_secret.get_secret_value(),
        algorithm="HS256",
    )
    return TokenResponse(
        access_token=token,
        expires_in=settings.token_ttl_minutes * 60,
    )


async def login(request: Request, credentials: LoginRequest) -> TokenResponse:
    settings: Settings = request.app.state.settings
    users: dict[str, tuple[str, str]] = request.app.state.users
    user = users.get(credentials.username)
    role = user[0] if user is not None else "mission-control"
    password_valid = password_hash.verify(
        credentials.password, request.app.state.password_hashes[role]
    )
    if user is None or not password_valid:
        record_audit_event(
            "authentication.login_failed",
            username=credentials.username,
            client_ip=request.client.host if request.client else None,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    record_audit_event(
        "authentication.login_succeeded",
        username=credentials.username,
        role=role,
        client_ip=request.client.host if request.client else None,
    )
    return create_access_token(settings, credentials.username, role)


async def current_principal(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> dict[str, str]:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    settings: Settings = request.app.state.settings
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.jwt_secret.get_secret_value(),
            algorithms=["HS256"],
            options={"require": ["sub", "role", "exp"]},
        )
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    subject = payload.get("sub")
    role = payload.get("role")
    if (
        not isinstance(subject, str)
        or not isinstance(role, str)
        or role
        not in {
            "mission-control",
            "scientist",
        }
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid access token")
    principal = {"username": subject, "role": role}
    request.state.principal = principal
    return principal


def require_roles(*allowed_roles: str):
    async def dependency(
        principal: Annotated[dict[str, str], Depends(current_principal)],
    ) -> dict[str, str]:
        if principal["role"] not in allowed_roles:
            record_audit_event(
                "authorization.denied",
                username=principal["username"],
                role=principal["role"],
            )
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient role")
        return principal

    return dependency


def get_platform(request: Request) -> Platform:
    platform: Platform = request.app.state.platform
    return platform


async def enforce_rate_limit(request: Request) -> None:
    platform: Platform = request.app.state.platform
    settings: Settings = request.app.state.settings
    client_id = request.client.host if request.client else "unknown"
    count = await platform.rate_limit(client_id)
    if count > settings.rate_limit_per_minute:
        record_audit_event(
            "api.rate_limit_exceeded",
            client_ip=client_id,
            limit=settings.rate_limit_per_minute,
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded",
            headers={"Retry-After": "60"},
        )
