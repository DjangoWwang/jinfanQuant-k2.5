"""Risk monitoring service: rule management, alert evaluation, dashboard."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Sequence

import pandas as pd
from sqlalchemy import select, func, update, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.alert import RiskRule, AlertEvent
from app.models.fund import Fund, NavHistory
from app.models.product import Product, ValuationSnapshot, ValuationItem

logger = logging.getLogger(__name__)

# Default trailing window for drawdown / volatility checks (trading days)
_DEFAULT_TRAILING_DAYS = 60


class RiskService:
    """Risk rule CRUD, alert evaluation, and dashboard."""

    # ------------------------------------------------------------------
    # Rule CRUD
    # ------------------------------------------------------------------

    async def create_rule(self, db: AsyncSession, payload: dict[str, Any]) -> RiskRule:
        rule = RiskRule(**payload)
        db.add(rule)
        await db.flush()
        await db.refresh(rule)
        return rule

    async def list_rules(
        self,
        db: AsyncSession,
        rule_type: str | None = None,
        is_active: bool | None = None,
    ) -> Sequence[RiskRule]:
        query = select(RiskRule).order_by(RiskRule.created_at.desc())
        if rule_type is not None:
            query = query.where(RiskRule.rule_type == rule_type)
        if is_active is not None:
            query = query.where(RiskRule.is_active == is_active)
        result = await db.execute(query)
        return result.scalars().all()

    async def get_rule(self, db: AsyncSession, rule_id: int) -> RiskRule | None:
        return await db.get(RiskRule, rule_id)

    async def update_rule(
        self, db: AsyncSession, rule_id: int, payload: dict[str, Any]
    ) -> RiskRule | None:
        rule = await self.get_rule(db, rule_id)
        if not rule:
            return None
        for field, value in payload.items():
            setattr(rule, field, value)
        await db.flush()
        await db.refresh(rule)
        return rule

    async def delete_rule(self, db: AsyncSession, rule_id: int) -> bool:
        """Soft delete: set is_active = False."""
        rule = await self.get_rule(db, rule_id)
        if not rule:
            return False
        rule.is_active = False
        await db.flush()
        return True

    # ------------------------------------------------------------------
    # Alert events
    # ------------------------------------------------------------------

    async def get_alerts(
        self,
        db: AsyncSession,
        is_read: bool | None = None,
        severity: str | None = None,
        limit: int = 50,
    ) -> Sequence[AlertEvent]:
        query = select(AlertEvent).order_by(AlertEvent.created_at.desc()).limit(limit)
        if is_read is not None:
            query = query.where(AlertEvent.is_read == is_read)
        if severity is not None:
            query = query.where(AlertEvent.severity == severity)
        result = await db.execute(query)
        return result.scalars().all()

    async def mark_read(self, db: AsyncSession, alert_id: int) -> AlertEvent | None:
        event = await db.get(AlertEvent, alert_id)
        if not event:
            return None
        event.is_read = True
        await db.flush()
        await db.refresh(event)
        return event

    async def mark_all_read(self, db: AsyncSession) -> int:
        result = await db.execute(
            update(AlertEvent)
            .where(AlertEvent.is_read == False)  # noqa: E712
            .values(is_read=True)
        )
        await db.flush()
        return result.rowcount  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Core: evaluate all active rules
    # ------------------------------------------------------------------

    async def check_all_rules(self, db: AsyncSession) -> list[dict[str, Any]]:
        """Evaluate every active rule and create AlertEvents for triggered ones.

        Returns a list of dicts describing newly created alerts.
        """
        rules = await self.list_rules(db, is_active=True)
        created: list[dict[str, Any]] = []

        for rule in rules:
            try:
                new_alerts = await self._evaluate_rule(db, rule)
                created.extend(new_alerts)
            except Exception:
                logger.exception("评估规则 %s (id=%s) 时出错", rule.name, rule.id)

        return created

    async def _evaluate_rule(
        self, db: AsyncSession, rule: RiskRule
    ) -> list[dict[str, Any]]:
        """Evaluate a single rule against all relevant targets."""
        handler = {
            "drawdown": self._check_drawdown,
            "volatility": self._check_volatility,
            "nav_anomaly": self._check_nav_anomaly,
            "concentration": self._check_concentration,
        }.get(rule.rule_type)

        if handler is None:
            logger.warning("未知规则类型: %s", rule.rule_type)
            return []

        # Determine targets
        targets = await self._resolve_targets(db, rule)
        created: list[dict[str, Any]] = []

        for target_id, target_name in targets:
            result = await handler(db, rule, target_id, target_name)
            if result is not None:
                created.append(result)

        return created

    async def _resolve_targets(
        self, db: AsyncSession, rule: RiskRule
    ) -> list[tuple[int, str]]:
        """Resolve which targets a rule applies to.

        Returns list of (target_id, target_name).
        """
        if rule.target_id is not None:
            # Specific target
            name = await self._get_target_name(db, rule.target_type, rule.target_id)
            if name is None:
                return []
            return [(rule.target_id, name)]

        # Apply to all of that target_type
        if rule.target_type == "fund":
            result = await db.execute(
                select(Fund.id, Fund.fund_name).where(Fund.status == "active")
            )
            return [(r.id, r.fund_name) for r in result.all()]
        elif rule.target_type == "product":
            result = await db.execute(
                select(Product.id, Product.product_name).where(Product.is_active == True)  # noqa: E712
            )
            return [(r.id, r.product_name) for r in result.all()]
        return []

    async def _get_target_name(
        self, db: AsyncSession, target_type: str, target_id: int
    ) -> str | None:
        if target_type == "fund":
            fund = await db.get(Fund, target_id)
            return fund.fund_name if fund else None
        elif target_type == "product":
            product = await db.get(Product, target_id)
            return product.product_name if product else None
        return None

    # ------------------------------------------------------------------
    # Individual check implementations
    # ------------------------------------------------------------------

    async def _check_drawdown(
        self,
        db: AsyncSession,
        rule: RiskRule,
        target_id: int,
        target_name: str,
    ) -> dict[str, Any] | None:
        nav_series = await self._get_trailing_nav_series(db, target_id, _DEFAULT_TRAILING_DAYS)
        if nav_series.empty or len(nav_series) < 2:
            return None

        from app.engine.metrics import calc_max_drawdown
        mdd, _, _ = calc_max_drawdown(nav_series)
        # mdd is negative (e.g., -0.15 for 15% drawdown)
        metric_value = abs(mdd)

        if not self._compare(metric_value, rule.comparison, float(rule.threshold)):
            return None

        message = (
            f"{target_name} 近{_DEFAULT_TRAILING_DAYS}个交易日最大回撤 "
            f"{metric_value:.2%}，超过阈值 {float(rule.threshold):.2%}"
        )
        return await self._create_alert_if_new(
            db, rule, target_id, target_name, metric_value, message
        )

    async def _check_volatility(
        self,
        db: AsyncSession,
        rule: RiskRule,
        target_id: int,
        target_name: str,
    ) -> dict[str, Any] | None:
        nav_series = await self._get_trailing_nav_series(db, target_id, _DEFAULT_TRAILING_DAYS)
        if nav_series.empty or len(nav_series) < 2:
            return None

        from app.engine.metrics import calc_annualized_volatility
        vol = calc_annualized_volatility(nav_series)

        if not self._compare(vol, rule.comparison, float(rule.threshold)):
            return None

        message = (
            f"{target_name} 近{_DEFAULT_TRAILING_DAYS}个交易日年化波动率 "
            f"{vol:.2%}，超过阈值 {float(rule.threshold):.2%}"
        )
        return await self._create_alert_if_new(
            db, rule, target_id, target_name, vol, message
        )

    async def _check_nav_anomaly(
        self,
        db: AsyncSession,
        rule: RiskRule,
        target_id: int,
        target_name: str,
    ) -> dict[str, Any] | None:
        """Detect daily return exceeding threshold (e.g., ±5% single day move)."""
        # Get the most recent daily_return
        result = await db.execute(
            select(NavHistory.daily_return, NavHistory.nav_date)
            .where(NavHistory.fund_id == target_id)
            .where(NavHistory.daily_return.isnot(None))
            .order_by(NavHistory.nav_date.desc())
            .limit(1)
        )
        row = result.first()
        if row is None or row[0] is None:
            return None

        daily_ret = abs(float(row[0]))
        if not self._compare(daily_ret, rule.comparison, float(rule.threshold)):
            return None

        message = (
            f"{target_name} 在 {row[1]} 单日收益率 {float(row[0]):+.2%}，"
            f"超过阈值 ±{float(rule.threshold):.2%}"
        )
        return await self._create_alert_if_new(
            db, rule, target_id, target_name, daily_ret, message
        )

    async def _check_concentration(
        self,
        db: AsyncSession,
        rule: RiskRule,
        target_id: int,
        target_name: str,
    ) -> dict[str, Any] | None:
        """For products, check if any single holding weight > threshold."""
        if rule.target_type != "product":
            return None

        # Get the latest valuation snapshot for this product
        snap_result = await db.execute(
            select(ValuationSnapshot)
            .where(ValuationSnapshot.product_id == target_id)
            .order_by(ValuationSnapshot.valuation_date.desc())
            .limit(1)
        )
        snapshot = snap_result.scalar_one_or_none()
        if snapshot is None:
            return None

        # Find max single-holding weight (value_pct_nav)
        item_result = await db.execute(
            select(
                ValuationItem.item_name,
                ValuationItem.value_pct_nav,
            )
            .where(ValuationItem.snapshot_id == snapshot.id)
            .where(ValuationItem.value_pct_nav.isnot(None))
            .order_by(ValuationItem.value_pct_nav.desc())
            .limit(1)
        )
        top_item = item_result.first()
        if top_item is None or top_item[1] is None:
            return None

        weight = float(top_item[1])  # value_pct_nav is already a percentage (e.g., 0.35 = 35%)
        if not self._compare(weight, rule.comparison, float(rule.threshold)):
            return None

        message = (
            f"{target_name} 中 {top_item[0]} 持仓占比 {weight:.2%}，"
            f"超过集中度阈值 {float(rule.threshold):.2%}"
        )
        return await self._create_alert_if_new(
            db, rule, target_id, target_name, weight, message
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _get_trailing_nav_series(
        self, db: AsyncSession, fund_id: int, n_days: int
    ) -> pd.Series:
        """Retrieve the last *n_days* NAV records as a pandas Series."""
        result = await db.execute(
            select(NavHistory.nav_date, NavHistory.cumulative_nav, NavHistory.unit_nav)
            .where(NavHistory.fund_id == fund_id)
            .order_by(NavHistory.nav_date.desc())
            .limit(n_days)
        )
        rows = result.all()
        if not rows:
            return pd.Series(dtype=float)

        pairs = []
        for r in rows:
            nav = r[1] if r[1] is not None else r[2]  # prefer cumulative_nav
            if nav is not None:
                pairs.append((r[0], float(nav)))

        if not pairs:
            return pd.Series(dtype=float)

        pairs.sort(key=lambda x: x[0])
        dates, navs = zip(*pairs)
        return pd.Series(navs, index=pd.DatetimeIndex(dates))

    @staticmethod
    def _compare(value: float, comparison: str, threshold: float) -> bool:
        """Compare metric value against threshold using the given operator."""
        if comparison == "gt":
            return value > threshold
        elif comparison == "lt":
            return value < threshold
        elif comparison == "gte":
            return value >= threshold
        elif comparison == "lte":
            return value <= threshold
        return False

    async def _has_recent_alert(
        self, db: AsyncSession, rule_id: int, target_id: int
    ) -> bool:
        """Check if an unresolved alert for the same rule+target exists within 24h."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        result = await db.execute(
            select(func.count(AlertEvent.id)).where(
                and_(
                    AlertEvent.rule_id == rule_id,
                    AlertEvent.target_id == target_id,
                    AlertEvent.resolved_at.is_(None),
                    AlertEvent.created_at >= cutoff,
                )
            )
        )
        count = result.scalar() or 0
        return count > 0

    async def _create_alert_if_new(
        self,
        db: AsyncSession,
        rule: RiskRule,
        target_id: int,
        target_name: str,
        metric_value: float,
        message: str,
    ) -> dict[str, Any] | None:
        """Create an AlertEvent only if no duplicate exists.

        Uses both application-level check and DB unique partial index
        (ix_alert_events_dedup) for concurrent safety.
        """
        if await self._has_recent_alert(db, rule.id, target_id):
            return None

        event = AlertEvent(
            rule_id=rule.id,
            target_type=rule.target_type,
            target_id=target_id,
            target_name=target_name,
            metric_value=Decimal(str(round(metric_value, 6))),
            threshold_value=rule.threshold,
            severity=rule.severity,
            message=message,
        )
        db.add(event)
        try:
            await db.flush()
        except IntegrityError:
            # Concurrent insert hit the unique partial index — safe to skip
            await db.rollback()
            return None
        await db.refresh(event)

        return {
            "alert_id": event.id,
            "rule_name": rule.name,
            "target_name": target_name,
            "severity": rule.severity,
            "message": message,
        }

    # ------------------------------------------------------------------
    # Dashboard
    # ------------------------------------------------------------------

    async def get_risk_dashboard(self, db: AsyncSession) -> dict[str, Any]:
        """Return risk dashboard summary."""
        # Active alerts count by severity
        severity_result = await db.execute(
            select(AlertEvent.severity, func.count(AlertEvent.id))
            .where(AlertEvent.is_read == False)  # noqa: E712
            .group_by(AlertEvent.severity)
        )
        alerts_by_severity = {row[0]: row[1] for row in severity_result.all()}

        # Total unread
        unread_total = sum(alerts_by_severity.values())

        # Active rules count
        rules_count_result = await db.execute(
            select(func.count(RiskRule.id)).where(RiskRule.is_active == True)  # noqa: E712
        )
        active_rules = rules_count_result.scalar() or 0

        # Top risks: latest unread critical alerts
        top_risks_result = await db.execute(
            select(AlertEvent)
            .where(AlertEvent.is_read == False)  # noqa: E712
            .order_by(
                # critical first, then by recency
                AlertEvent.severity.desc(),
                AlertEvent.created_at.desc(),
            )
            .limit(10)
        )
        top_risks = [
            {
                "id": e.id,
                "target_type": e.target_type,
                "target_id": e.target_id,
                "target_name": e.target_name,
                "severity": e.severity,
                "message": e.message,
                "metric_value": float(e.metric_value) if e.metric_value else None,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in top_risks_result.scalars().all()
        ]

        # Recent events (including read)
        recent_result = await db.execute(
            select(AlertEvent)
            .order_by(AlertEvent.created_at.desc())
            .limit(20)
        )
        recent_events = [
            {
                "id": e.id,
                "rule_id": e.rule_id,
                "target_type": e.target_type,
                "target_id": e.target_id,
                "target_name": e.target_name,
                "severity": e.severity,
                "message": e.message,
                "is_read": e.is_read,
                "metric_value": float(e.metric_value) if e.metric_value else None,
                "threshold_value": float(e.threshold_value) if e.threshold_value else None,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in recent_result.scalars().all()
        ]

        return {
            "unread_total": unread_total,
            "alerts_by_severity": alerts_by_severity,
            "active_rules": active_rules,
            "top_risks": top_risks,
            "recent_events": recent_events,
        }


risk_service = RiskService()
