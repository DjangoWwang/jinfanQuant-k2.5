"""策略分类模型 — 支持三级树结构，团队可自定义编辑。"""

from sqlalchemy import (
    Column, Integer, String, Boolean, ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class StrategyCategory(Base):
    """策略分类树节点。

    level=1: 一级策略 (股票类, 债券类, 商品/衍生品, 套利/现金管理, 宏观类, FOF)
    level=2: 二级策略 (股票多头, 市场中性, 量化期货, ...)
    level=3: 三级策略 (主观多头, 300指增, 500指增, ...)
    """
    __tablename__ = "strategy_categories"
    __table_args__ = (
        UniqueConstraint("parent_id", "name", name="uq_strategy_parent_name"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    level = Column(Integer, nullable=False)  # 1, 2, 3
    parent_id = Column(Integer, ForeignKey("strategy_categories.id"), nullable=True)
    sort_order = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)

    children = relationship(
        "StrategyCategory",
        backref="parent",
        remote_side=[id],
        foreign_keys=[parent_id],
    )
