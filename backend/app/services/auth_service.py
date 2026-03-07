"""Authentication service: password hashing, JWT token management, user CRUD."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Sequence

import bcrypt
import jwt
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.user import User

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


def hash_password(plain: str) -> str:
    validate_password_strength(plain)
    pwd_bytes = plain.encode("utf-8")
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pwd_bytes, salt).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


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
