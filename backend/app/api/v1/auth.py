"""Authentication API endpoints."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.database import get_db
from app.models.user import User
from app.api.deps import get_current_user, require_role
from app.services import auth_service

_bearer_optional = HTTPBearer(auto_error=False)

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
        raise HTTPException(401, "用户名或密码错误")
    if not user.is_active:
        raise HTTPException(403, "账号已被禁用")

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
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_optional),
    db: AsyncSession = Depends(get_db),
):
    """注册新用户。

    - 如果系统中没有任何用户，第一个注册的用户自动成为 admin（无需认证）。
    - 否则需要 admin 权限的 Bearer token 才能创建新用户。
    """
    user_count = await auth_service.get_user_count(db)

    if user_count == 0:
        # First user: auto-admin, requires setup key for security
        if not settings.ADMIN_SETUP_KEY:
            raise HTTPException(500, "ADMIN_SETUP_KEY 未配置，无法初始化管理员")
        if not payload.setup_key or payload.setup_key != settings.ADMIN_SETUP_KEY:
            raise HTTPException(403, "初始管理员注册需要正确的安装密钥")
        role = "admin"
    else:
        # Require admin authentication
        if credentials is None:
            raise HTTPException(401, "需要管理员登录后才能创建新用户")

        import jwt as _jwt
        try:
            token_payload = auth_service.verify_token(credentials.credentials)
        except _jwt.PyJWTError:
            raise HTTPException(401, "无效的认证令牌")

        admin_id = int(token_payload.get("sub", 0))
        admin_user = await auth_service.get_user_by_id(db, admin_id)
        if not admin_user or not admin_user.is_active:
            raise HTTPException(401, "用户不存在或已被禁用")
        if admin_user.role != "admin":
            raise HTTPException(403, "权限不足，仅管理员可创建用户")

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
        await db.commit()
    except ValueError as e:
        await db.rollback()
        raise HTTPException(400, str(e))
    except IntegrityError:
        await db.rollback()
        raise HTTPException(409, "用户名或邮箱已被使用")

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
        raise HTTPException(400, "原密码错误")

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
