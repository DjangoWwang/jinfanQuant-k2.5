"""P2-4: product_navs table for calculated NAV series.

Revision ID: f6g7h8i9j0k1
Revises: e5f6g7h8i9j0
Create Date: 2026-03-08
"""

from alembic import op
import sqlalchemy as sa

revision = "f6g7h8i9j0k1"
down_revision = "e5f6g7h8i9j0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "product_navs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("product_id", sa.Integer, sa.ForeignKey("products.id", ondelete="CASCADE"), nullable=False),
        sa.Column("nav_date", sa.Date, nullable=False),
        sa.Column("unit_nav", sa.Numeric(12, 6), nullable=True),
        sa.Column("cumulative_nav", sa.Numeric(12, 6), nullable=True),
        sa.Column("total_nav", sa.Numeric(16, 2), nullable=True),
        sa.Column("total_shares", sa.Numeric(16, 4), nullable=True),
        sa.Column("fund_assets", sa.Numeric(16, 2), nullable=True),
        sa.Column("non_fund_assets", sa.Numeric(16, 2), nullable=True),
        sa.Column("source", sa.String(20), server_default="calculated"),
        sa.Column("snapshot_id", sa.Integer, sa.ForeignKey("valuation_snapshots.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("product_id", "nav_date", name="uq_product_nav_product_date"),
    )

    op.create_index(
        "ix_product_navs_product_date",
        "product_navs",
        ["product_id", "nav_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_product_navs_product_date", table_name="product_navs")
    op.drop_table("product_navs")
