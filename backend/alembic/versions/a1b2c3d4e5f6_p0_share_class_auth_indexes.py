"""P0: fund share class, user auth, DB indexes

Revision ID: a1b2c3d4e5f6
Revises: 5e5a3ebfd16e
Create Date: 2026-03-08

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "5e5a3ebfd16e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- Fund share class association ---
    op.add_column("funds", sa.Column("parent_fund_id", sa.Integer(), nullable=True))
    op.add_column("funds", sa.Column("share_class", sa.String(10), nullable=True))
    op.create_foreign_key(
        "fk_funds_parent_fund_id",
        "funds", "funds",
        ["parent_fund_id"], ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_funds_parent_fund_id", "funds", ["parent_fund_id"])

    # --- Users table for auth ---
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("username", sa.String(50), nullable=False, unique=True),
        sa.Column("email", sa.String(200), nullable=True, unique=True),
        sa.Column("hashed_password", sa.String(128), nullable=False),
        sa.Column("display_name", sa.String(100), nullable=True),
        sa.Column("role", sa.String(20), nullable=False, server_default="viewer"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # --- Performance indexes ---
    # nav_history: composite desc index for fast latest-NAV queries
    op.create_index(
        "ix_nav_history_fund_date_desc",
        "nav_history",
        ["fund_id", sa.text("nav_date DESC")],
    )

    # valuation_snapshots: fast latest snapshot per product
    op.create_index(
        "ix_valuation_snapshots_product_date_desc",
        "valuation_snapshots",
        ["product_id", sa.text("valuation_date DESC")],
    )

    # funds: text search on fund_name for faster LIKE queries
    op.create_index(
        "ix_funds_name_trgm",
        "funds",
        ["fund_name"],
        postgresql_using="gin",
        postgresql_ops={"fund_name": "gin_trgm_ops"},
    )


def downgrade() -> None:
    op.drop_index("ix_funds_name_trgm", table_name="funds")
    op.drop_index("ix_valuation_snapshots_product_date_desc", table_name="valuation_snapshots")
    op.drop_index("ix_nav_history_fund_date_desc", table_name="nav_history")
    op.drop_table("users")
    op.drop_index("ix_funds_parent_fund_id", table_name="funds")
    op.drop_constraint("fk_funds_parent_fund_id", "funds", type_="foreignkey")
    op.drop_column("funds", "share_class")
    op.drop_column("funds", "parent_fund_id")
