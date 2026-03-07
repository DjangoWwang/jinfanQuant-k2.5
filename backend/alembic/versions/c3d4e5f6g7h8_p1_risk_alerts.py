"""P1: risk monitoring rules and alert events tables

Revision ID: c3d4e5f6g7h8
Revises: b2c3d4e5f6g7
Create Date: 2026-03-08

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "c3d4e5f6g7h8"
down_revision: Union[str, None] = "b2c3d4e5f6g7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "risk_rules",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("rule_type", sa.String(30), nullable=False),
        sa.Column("target_type", sa.String(20), nullable=False),
        sa.Column("target_id", sa.Integer(), nullable=True),
        sa.Column("threshold", sa.Numeric(12, 6), nullable=False),
        sa.Column("comparison", sa.String(5), nullable=False),
        sa.Column("severity", sa.String(10), nullable=False, server_default="warning"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_by", sa.Integer(), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_risk_rules_type_active", "risk_rules", ["rule_type", "is_active"])

    op.create_table(
        "alert_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("rule_id", sa.Integer(), sa.ForeignKey("risk_rules.id", ondelete="CASCADE"), nullable=False),
        sa.Column("target_type", sa.String(20), nullable=False),
        sa.Column("target_id", sa.Integer(), nullable=False),
        sa.Column("target_name", sa.String(200), nullable=True),
        sa.Column("metric_value", sa.Numeric(12, 6), nullable=True),
        sa.Column("threshold_value", sa.Numeric(12, 6), nullable=True),
        sa.Column("severity", sa.String(10), nullable=False, server_default="warning"),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("is_read", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_alert_events_target", "alert_events", ["target_type", "target_id", "created_at"])
    op.create_index("ix_alert_events_read_severity", "alert_events", ["is_read", "severity"])
    # Partial unique index for deduplication: one unresolved alert per rule+target
    op.create_index(
        "ix_alert_events_dedup",
        "alert_events",
        ["rule_id", "target_id"],
        unique=True,
        postgresql_where=sa.text("resolved_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_alert_events_dedup", table_name="alert_events")
    op.drop_index("ix_alert_events_read_severity", table_name="alert_events")
    op.drop_index("ix_alert_events_target", table_name="alert_events")
    op.drop_table("alert_events")
    op.drop_index("ix_risk_rules_type_active", table_name="risk_rules")
    op.drop_table("risk_rules")
