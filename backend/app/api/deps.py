"""FastAPI dependencies for authentication and authorization."""

from __future__ import annotations

from typing import Callable

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.user import User
from app.services import auth_service

# Bearer token extractor (auto_error=True raises 403 if missing)
_bearer = HTTPBearer(auto_error=True)
_bearer_optional = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Extract and verify Bearer token, load user from DB.

    Raises 401 if token is invalid/expired or user not found/inactive.
    """
    token = credentials.credentials
    try:
        payload = auth_service.verify_token(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="登录已过期，请重新登录",
        )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效的认证令牌",
        )

    try:
        user_id = int(payload.get("sub", 0))
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效的认证令牌",
        )
    user = await auth_service.get_user_by_id(db, user_id)
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户不存在或已被禁用",
        )
    return user


async def get_optional_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_optional),
    db: AsyncSession = Depends(get_db),
) -> User | None:
    """Same as get_current_user but returns None if no token provided.

    Useful for public endpoints that optionally show user info.
    """
    if credentials is None:
        return None

    token = credentials.credentials
    try:
        payload = auth_service.verify_token(token)
    except jwt.PyJWTError:
        return None

    try:
        user_id = int(payload.get("sub", 0))
    except (TypeError, ValueError):
        return None
    user = await auth_service.get_user_by_id(db, user_id)
    if not user or not user.is_active:
        return None
    return user


def require_role(*roles: str) -> Callable:
    """Dependency factory that checks user.role is in allowed roles.

    Usage:
        @router.get("/admin-only", dependencies=[Depends(require_role("admin"))])
        async def admin_endpoint(...): ...

    Or inject user:
        current_user: User = Depends(require_role("admin", "analyst"))
    """
    async def _check_role(
        current_user: User = Depends(get_current_user),
    ) -> User:
        if current_user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"权限不足，需要角色: {', '.join(roles)}",
            )
        return current_user

    return _check_role
