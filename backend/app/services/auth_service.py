"""Authentication service: password hashing, JWT token management, user CRUD."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Sequence

import bcrypt
import jwt
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.user import User
from app.models.audit_log import AuditLog

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Password helpers (using bcrypt directly - passlib has compat issues with bcrypt 4.x)
# ------------------------------------------------------------------

def validate_password_strength(plain: str) -> None:
    """Validate password meets minimum security requirements."""
    if len(plain) < 8:
        raise ValueError("密码长度不能少于8个字符")
    if len(plain) > 128:
        raise ValueError("密码长度不能超过128个字符")
    if not any(c.isalpha() for c in plain):
        raise ValueError("密码必须包含至少一个字母")
    if not any(c.isdigit() for c in plain):
        raise ValueError("密码必须包含至少一个数字")


def hash_password(plain: str) -> str:
    validate_password_strength(plain)
    pwd_bytes = plain.encode("utf-8")
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pwd_bytes, salt).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except (ValueError, TypeError) as e:
        logger.warning("Password verification failed: %s", e)
        return False


# ------------------------------------------------------------------
# JWT helpers
# ------------------------------------------------------------------

def create_access_token(user_id: int, role: str) -> str:
    """Create a JWT access token with user_id and role in payload."""
    expire = datetime.now(timezone.utc) + timedelta(hours=settings.JWT_EXPIRE_HOURS)
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "role": role,
        "iat": now,
        "exp": expire,
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def verify_token(token: str) -> dict:
    """Decode and verify a JWT token. Raises jwt.PyJWTError on failure."""
    return jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])


# ------------------------------------------------------------------
# User CRUD
# ------------------------------------------------------------------

async def create_user(
    db: AsyncSession,
    username: str,
    password: str,
    role: str = "viewer",
    email: str | None = None,
    display_name: str | None = None,
) -> User:
    """Create a new user. Raises ValueError if username/email already exists."""
    # Check uniqueness
    existing = await db.execute(
        select(User).where(User.username == username)
    )
    if existing.scalar_one_or_none():
        raise ValueError(f"用户名已存在: {username}")

    if email:
        existing_email = await db.execute(
            select(User).where(User.email == email)
        )
        if existing_email.scalar_one_or_none():
            raise ValueError(f"邮箱已被使用: {email}")

    user = User(
        username=username,
        hashed_password=hash_password(password),
        role=role,
        email=email,
        display_name=display_name or username,
    )
    db.add(user)
    await db.flush()
    await db.refresh(user)
    return user


async def authenticate(db: AsyncSession, username: str, password: str) -> User | None:
    """Verify credentials and return user, or None if invalid."""
    result = await db.execute(
        select(User).where(User.username == username)
    )
    user = result.scalar_one_or_none()
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


async def get_user_by_id(db: AsyncSession, user_id: int) -> User | None:
    return await db.get(User, user_id)


async def list_users(db: AsyncSession) -> Sequence[User]:
    result = await db.execute(
        select(User).order_by(User.created_at.desc())
    )
    return result.scalars().all()


async def change_password(db: AsyncSession, user: User, new_password: str) -> None:
    user.hashed_password = hash_password(new_password)
    await db.flush()


async def get_user_count(db: AsyncSession) -> int:
    result = await db.execute(select(func.count(User.id)))
    return result.scalar() or 0


async def create_initial_admin(
    db: AsyncSession,
    *,
    username: str,
    password: str,
    email: str | None = None,
    display_name: str | None = None,
) -> User:
    """Create the first admin user with advisory lock to prevent concurrent init.

    Raises ValueError if the system is already initialized.
    """
    # Serialize initialization with PostgreSQL advisory lock
    try:
        await db.execute(text("SELECT pg_advisory_xact_lock(10001)"))
    except Exception:
        logger.debug("Advisory lock not available, falling back to count check")

    count = await get_user_count(db)
    if count != 0:
        raise ValueError("系统已初始化，不能再使用首个管理员注册流程")

    return await create_user(
        db,
        username=username,
        password=password,
        role="admin",
        email=email,
        display_name=display_name,
    )


async def count_active_admins(db: AsyncSession) -> int:
    """Count active users with admin role."""
    result = await db.execute(
        select(func.count(User.id)).where(
            User.role == "admin", User.is_active.is_(True)
        )
    )
    return result.scalar() or 0


async def exists_other_active_admin(db: AsyncSession, exclude_user_id: int) -> bool:
    """Check if another active admin exists (besides exclude_user_id), with row lock."""
    result = await db.execute(
        select(User.id)
        .where(
            User.role == "admin",
            User.is_active.is_(True),
            User.id != exclude_user_id,
        )
        .limit(1)
        .with_for_update()
    )
    return result.first() is not None


async def get_user_by_id_for_update(db: AsyncSession, user_id: int) -> User | None:
    """Load user with row-level lock for safe mutation."""
    result = await db.execute(
        select(User).where(User.id == user_id).with_for_update()
    )
    return result.scalar_one_or_none()


async def update_user(
    db: AsyncSession,
    user: User,
    *,
    role: str | None = None,
    display_name: str | None = None,
    email: str | None = None,
) -> User:
    """Update user fields. Only non-None values are applied."""
    if role is not None:
        if role not in ("admin", "analyst", "viewer"):
            raise ValueError(f"无效的角色: {role}")
        user.role = role
    if display_name is not None:
        user.display_name = display_name
    if email is not None:
        if email:
            existing = await db.execute(
                select(User).where(User.email == email, User.id != user.id)
            )
            if existing.scalar_one_or_none():
                raise ValueError("邮箱已被其他用户使用")
        user.email = email or None
    await db.flush()
    return user


async def toggle_active(db: AsyncSession, user: User, is_active: bool) -> User:
    """Enable or disable a user."""
    user.is_active = is_active
    await db.flush()
    return user


async def reset_password(db: AsyncSession, user: User, new_password: str) -> None:
    """Admin resets a user's password."""
    user.hashed_password = hash_password(new_password)
    await db.flush()


# ------------------------------------------------------------------
# Audit log
# ------------------------------------------------------------------

async def create_audit_log(
    db: AsyncSession,
    *,
    user_id: int,
    username: str,
    action: str,
    target_type: str | None = None,
    target_id: int | None = None,
    detail: str | None = None,
    ip_address: str | None = None,
) -> AuditLog:
    log = AuditLog(
        user_id=user_id,
        username=username,
        action=action,
        target_type=target_type,
        target_id=target_id,
        detail=detail[:2000] if detail else detail,
        ip_address=ip_address,
    )
    db.add(log)
    await db.flush()
    return log


async def list_audit_logs(
    db: AsyncSession,
    *,
    limit: int = 100,
    offset: int = 0,
    action: str | None = None,
    user_id: int | None = None,
) -> tuple[Sequence[AuditLog], int]:
    """Return paginated audit logs and total count."""
    query = select(AuditLog)
    count_query = select(func.count(AuditLog.id))

    if action:
        query = query.where(AuditLog.action == action)
        count_query = count_query.where(AuditLog.action == action)
    if user_id is not None:
        query = query.where(AuditLog.user_id == user_id)
        count_query = count_query.where(AuditLog.user_id == user_id)

    total = (await db.execute(count_query)).scalar() or 0
    result = await db.execute(
        query.order_by(AuditLog.created_at.desc(), AuditLog.id.desc()).offset(offset).limit(limit)
    )
    return result.scalars().all(), total
