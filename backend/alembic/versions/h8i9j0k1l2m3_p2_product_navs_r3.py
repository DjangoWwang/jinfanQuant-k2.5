"""P2-4 R3: updated_at + valuation_items index.

Revision ID: h8i9j0k1l2m3
Revises: g7h8i9j0k1l2
Create Date: 2026-03-08
"""

from alembic import op
import sqlalchemy as sa

revision = "h8i9j0k1l2m3"
down_revision = "g7h8i9j0k1l2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add updated_at column for audit tracking on upsert
    op.add_column(
        "product_navs",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.execute("UPDATE product_navs SET updated_at = created_at WHERE updated_at IS NULL")

    # Index on valuation_items.linked_fund_id for batch position lookups
    op.create_index(
        "ix_valuation_items_linked_fund_id",
        "valuation_items",
        ["linked_fund_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_valuation_items_linked_fund_id", table_name="valuation_items")
    op.drop_column("product_navs", "updated_at")
