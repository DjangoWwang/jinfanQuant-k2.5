"""Authentication API endpoints."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.database import get_db
from app.models.user import User
from app.api.deps import get_current_user, get_optional_user, require_role
from app.services import auth_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


# ------------------------------------------------------------------
# Request / Response schemas
# ------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=50)
    password: str = Field(..., min_length=1)


class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=2, max_length=50)
    password: str = Field(..., min_length=8, max_length=128)
    role: str = Field("viewer", pattern="^(admin|analyst|viewer)$")
    email: str | None = Field(None, max_length=200)
    display_name: str | None = Field(None, max_length=100)
    setup_key: str | None = Field(None, description="首次注册管理员时需要的安装密钥")

    @field_validator("username")
    @classmethod
    def normalize_username(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 2:
            raise ValueError("用户名长度不能少于2个字符")
        return v

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip().lower()
        return v or None

    @field_validator("display_name")
    @classmethod
    def normalize_display_name(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        return v or None


class ChangePasswordRequest(BaseModel):
    old_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8, max_length=128)


class UserInfo(BaseModel):
    id: int
    username: str
    email: str | None = None
    display_name: str | None = None
    role: str
    is_active: bool
    last_login_at: str | None = None
    created_at: str | None = None

    model_config = {"from_attributes": True}


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserInfo


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _user_to_info(user: User) -> UserInfo:
    return UserInfo(
        id=user.id,
        username=user.username,
        email=user.email,
        display_name=user.display_name,
        role=user.role,
        is_active=user.is_active,
        last_login_at=user.last_login_at.isoformat() if user.last_login_at else None,
        created_at=user.created_at.isoformat() if user.created_at else None,
    )


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------

@router.post("/login", response_model=LoginResponse)
async def login(payload: LoginRequest, db: AsyncSession = Depends(get_db)):
    """用户登录，返回 JWT token 和用户信息。"""
    user = await auth_service.authenticate(db, payload.username, payload.password)
    if not user:
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="账号已被禁用")

    # Update last_login_at
    user.last_login_at = datetime.now(timezone.utc)
    await db.commit()

    token = auth_service.create_access_token(user.id, user.role)
    return LoginResponse(
        access_token=token,
        user=_user_to_info(user),
    )


@router.post("/register", response_model=UserInfo, status_code=201)
async def register(
    payload: RegisterRequest,
    request: Request,
    caller: User | None = Depends(get_optional_user),
    db: AsyncSession = Depends(get_db),
):
    """注册新用户。

    - 如果系统中没有任何用户，第一个注册的用户自动成为 admin（无需认证）。
    - 否则需要 admin 权限的 Bearer token 才能创建新用户。
    """
    user_count = await auth_service.get_user_count(db)

    if user_count == 0:
        # First user: auto-admin with advisory lock to prevent concurrent init
        if not settings.ADMIN_SETUP_KEY:
            raise HTTPException(status_code=500, detail="ADMIN_SETUP_KEY 未配置，无法初始化管理员")
        if not payload.setup_key or payload.setup_key != settings.ADMIN_SETUP_KEY:
            raise HTTPException(status_code=403, detail="初始管理员注册需要正确的安装密钥")

        try:
            user = await auth_service.create_initial_admin(
                db,
                username=payload.username,
                password=payload.password,
                email=payload.email,
                display_name=payload.display_name,
            )
            await db.commit()
        except ValueError as e:
            await db.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        except IntegrityError:
            await db.rollback()
            raise HTTPException(status_code=409, detail="用户名或邮箱已被使用")
    else:
        # Require admin authentication (reuse centralized auth dependency)
        if not caller:
            raise HTTPException(status_code=401, detail="需要管理员登录后才能创建新用户")
        if caller.role != "admin":
            raise HTTPException(status_code=403, detail="权限不足，仅管理员可创建用户")
        role = payload.role

        try:
            user = await auth_service.create_user(
                db,
                username=payload.username,
                password=payload.password,
                role=role,
                email=payload.email,
                display_name=payload.display_name,
            )
            await _log_action(
                db, caller, "user.create", request,
                target_type="user", target_id=user.id,
                detail=f"创建用户 {user.username} (角色: {role})",
            )
            await db.commit()
        except ValueError as e:
            await db.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        except IntegrityError:
            await db.rollback()
            raise HTTPException(status_code=409, detail="用户名或邮箱已被使用")

    return _user_to_info(user)


@router.get("/me", response_model=UserInfo)
async def get_me(current_user: User = Depends(get_current_user)):
    """获取当前登录用户信息。"""
    return _user_to_info(current_user)


@router.put("/change-password")
async def change_password(
    payload: ChangePasswordRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """修改当前用户密码。"""
    if not auth_service.verify_password(payload.old_password, current_user.hashed_password):
        raise HTTPException(status_code=400, detail="原密码错误")

    await auth_service.change_password(db, current_user, payload.new_password)
    await db.commit()
    return {"message": "密码修改成功"}


@router.get("/users", response_model=list[UserInfo])
async def list_users(
    current_user: User = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """获取所有用户列表（仅管理员）。"""
    users = await auth_service.list_users(db)
    return [_user_to_info(u) for u in users]


# ------------------------------------------------------------------
# User management (admin only)
# ------------------------------------------------------------------

class UpdateUserRequest(BaseModel):
    role: str | None = Field(None, pattern="^(admin|analyst|viewer)$")
    display_name: str | None = Field(None, max_length=100)
    email: str | None = Field(None, max_length=200)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip().lower()
        return v or None

    @field_validator("display_name")
    @classmethod
    def normalize_display_name(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        return v or None


class ToggleActiveRequest(BaseModel):
    is_active: bool


class ResetPasswordRequest(BaseModel):
    new_password: str = Field(..., min_length=8, max_length=128)


class AuditLogInfo(BaseModel):
    id: int
    user_id: int
    username: str
    action: str
    target_type: str | None = None
    target_id: int | None = None
    detail: str | None = None
    ip_address: str | None = None
    created_at: str | None = None

    model_config = {"from_attributes": True}


class AuditLogListResponse(BaseModel):
    items: list[AuditLogInfo]
    total: int


def _get_client_ip(request: Request) -> str | None:
    """Extract client IP. Only trusts X-Forwarded-For when TRUST_PROXY_HEADERS is enabled."""
    if settings.TRUST_PROXY_HEADERS:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return None


async def _log_action(
    db: AsyncSession,
    user: User,
    action: str,
    request: Request,
    target_type: str | None = None,
    target_id: int | None = None,
    detail: str | None = None,
):
    """Record an audit log entry within the current transaction."""
    await auth_service.create_audit_log(
        db,
        user_id=user.id,
        username=user.username,
        action=action,
        target_type=target_type,
        target_id=target_id,
        detail=detail,
        ip_address=_get_client_ip(request),
    )


@router.patch("/users/{user_id}", response_model=UserInfo)
async def update_user(
    user_id: int,
    payload: UpdateUserRequest,
    request: Request,
    current_user: User = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """更新用户信息（仅管理员）。"""
    # Lock target user row to prevent concurrent modification
    target = await auth_service.get_user_by_id_for_update(db, user_id)
    if not target:
        raise HTTPException(status_code=404, detail="用户不存在")

    # Prevent downgrading the last active admin (existence check with row lock)
    if (payload.role is not None
            and payload.role != "admin"
            and target.role == "admin"):
        has_other = await auth_service.exists_other_active_admin(db, target.id)
        if not has_other:
            raise HTTPException(
                status_code=400,
                detail="不能降级系统最后一个管理员的角色",
            )

    changes = []
    if payload.role is not None and payload.role != target.role:
        changes.append(f"角色: {target.role} → {payload.role}")
    if payload.display_name is not None and payload.display_name != target.display_name:
        changes.append(f"显示名: {target.display_name} → {payload.display_name}")
    if payload.email is not None and payload.email != (target.email or ""):
        changes.append("邮箱已更新")

    try:
        await auth_service.update_user(
            db, target,
            role=payload.role,
            display_name=payload.display_name,
            email=payload.email,
        )
        if changes:
            await _log_action(
                db, current_user, "user.update", request,
                target_type="user", target_id=user_id,
                detail="; ".join(changes),
            )
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="用户名或邮箱已被使用")
    except ValueError as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Failed to update user %d", user_id)
        await db.rollback()
        raise

    return _user_to_info(target)


@router.put("/users/{user_id}/toggle-active", response_model=UserInfo)
async def toggle_user_active(
    user_id: int,
    payload: ToggleActiveRequest,
    request: Request,
    current_user: User = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """启用/禁用用户（仅管理员）。不能禁用自己或最后一个管理员。"""
    if user_id == current_user.id:
        raise HTTPException(status_code=400, detail="不能禁用自己的账号")

    # Lock target user row to prevent concurrent modification
    target = await auth_service.get_user_by_id_for_update(db, user_id)
    if not target:
        raise HTTPException(status_code=404, detail="用户不存在")

    # Prevent disabling the last active admin (existence check with row lock)
    if not payload.is_active and target.role == "admin":
        has_other = await auth_service.exists_other_active_admin(db, target.id)
        if not has_other:
            raise HTTPException(
                status_code=400,
                detail="不能禁用系统最后一个管理员账户",
            )

    try:
        await auth_service.toggle_active(db, target, payload.is_active)
        action = "user.enable" if payload.is_active else "user.disable"
        await _log_action(
            db, current_user, action, request,
            target_type="user", target_id=user_id,
            detail=f"{'启用' if payload.is_active else '禁用'}用户 {target.username}",
        )
        await db.commit()
    except Exception:
        logger.exception("Failed to toggle user %d active state", user_id)
        await db.rollback()
        raise

    return _user_to_info(target)


@router.post("/users/{user_id}/reset-password")
async def reset_user_password(
    user_id: int,
    payload: ResetPasswordRequest,
    request: Request,
    current_user: User = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """重置用户密码（仅管理员）。"""
    target = await auth_service.get_user_by_id_for_update(db, user_id)
    if not target:
        raise HTTPException(status_code=404, detail="用户不存在")

    try:
        await auth_service.reset_password(db, target, payload.new_password)
        await _log_action(
            db, current_user, "user.reset_password", request,
            target_type="user", target_id=user_id,
            detail=f"重置用户 {target.username} 的密码",
        )
        await db.commit()
    except ValueError as e:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Failed to reset password for user %d", user_id)
        await db.rollback()
        raise

    return {"message": "密码重置成功"}


@router.get("/audit-logs", response_model=AuditLogListResponse)
async def get_audit_logs(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=10000),
    action: str | None = None,
    user_id: int | None = None,
    current_user: User = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """获取操作日志（仅管理员）。"""

    logs, total = await auth_service.list_audit_logs(
        db, limit=limit, offset=offset, action=action, user_id=user_id,
    )

    items = []
    for log in logs:
        items.append(AuditLogInfo(
            id=log.id,
            user_id=log.user_id,
            username=log.username,
            action=log.action,
            target_type=log.target_type,
            target_id=log.target_id,
            detail=log.detail,
            ip_address=log.ip_address,
            created_at=log.created_at.isoformat() if log.created_at else None,
        ))

    return AuditLogListResponse(items=items, total=total)
