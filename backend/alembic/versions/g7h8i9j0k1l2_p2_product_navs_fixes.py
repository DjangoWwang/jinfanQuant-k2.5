"""P2-4 fixes: source NOT NULL + CHECK constraint.

Revision ID: g7h8i9j0k1l2
Revises: f6g7h8i9j0k1
Create Date: 2026-03-08
"""

from alembic import op
import sqlalchemy as sa

revision = "g7h8i9j0k1l2"
down_revision = "f6g7h8i9j0k1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Backfill any NULL source values
    op.execute("UPDATE product_navs SET source = 'calculated' WHERE source IS NULL")

    # Make source NOT NULL
    op.alter_column(
        "product_navs", "source",
        existing_type=sa.String(20),
        nullable=False,
        server_default="calculated",
    )

    # Add CHECK constraint for valid source values
    op.create_check_constraint(
        "ck_product_navs_source",
        "product_navs",
        "source IN ('valuation', 'calculated')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_product_navs_source", "product_navs", type_="check")
    op.alter_column(
        "product_navs", "source",
        existing_type=sa.String(20),
        nullable=True,
        server_default="calculated",
    )
