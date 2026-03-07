"""P0 review fixes: password column width, share class constraints, pg_trgm extension

Revision ID: b2c3d4e5f6g7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-08

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6g7"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Ensure pg_trgm extension exists (was missing from previous migration)
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # Fix hashed_password column: model uses String(200), migration created String(128)
    op.alter_column(
        "users", "hashed_password",
        type_=sa.String(255),
        existing_type=sa.String(128),
        existing_nullable=False,
    )

    # Share class constraints: prevent self-referencing and duplicate share classes
    op.create_check_constraint(
        "ck_funds_no_self_parent",
        "funds",
        "parent_fund_id IS NULL OR parent_fund_id <> id",
    )
    op.create_unique_constraint(
        "uq_funds_parent_share_class",
        "funds",
        ["parent_fund_id", "share_class"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_funds_parent_share_class", "funds", type_="unique")
    op.drop_constraint("ck_funds_no_self_parent", "funds", type_="check")
    op.alter_column(
        "users", "hashed_password",
        type_=sa.String(128),
        existing_type=sa.String(255),
        existing_nullable=False,
    )
