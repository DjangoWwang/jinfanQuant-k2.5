"""FOF99 fund list scraper.

已验证API端点:
- POST /newgoapi/fund/advancedList - 全量基金搜索 (需机构账号, 39万+)
- GET /newgoapi/fund/search/funds - 公司关联基金 (~20只)
- GET /newgoapi/funds/info - 基金详情 (params: id=encode_id)
- GET /newgoapi/list/strategy - 策略分类树
- GET /newgoapi/sm/category - 策略分类+基金数量
"""

from __future__ import annotations

import logging
from typing import Any

from app.crawler.fof99.client import Fof99Client

logger = logging.getLogger(__name__)

# 火富牛策略树 ID (从 /newgoapi/list/strategy?type=1 获取)
STRATEGY_IDS: dict[str, int] = {
    "期货策略": 73,
    "量化期货": 74,
    "主观期货": 82,
    "股票对冲": 86,
    "股票市场中性": 87,
    "股票多头": 101,
    "主观多头": 102,
    "300指增": 114,
    "500指增": 121,
    "套利策略": 153,
    "期权策略": 167,
}


class FundScraper:
    """从火富牛平台爬取基金列表和详情数据。"""

    def __init__(self, client: Fof99Client) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # 全量基金搜索 (需机构账号权限)
    # ------------------------------------------------------------------

    async def advanced_search(
        self,
        keyword: str = "",
        strategy_ids: list[int] | None = None,
        page: int = 1,
        pagesize: int = 300,
        order_by: str = "lastOneMonthReturn",
        order: int = 1,
    ) -> dict[str, Any]:
        """全量基金搜索 (POST /newgoapi/fund/advancedList)。

        Args:
            keyword: 搜索关键词 (基金名/代码/备案号)
            strategy_ids: 策略分类ID列表 (如 [74] 表示量化期货)
            page: 页码
            pagesize: 每页数量 (最大300)
            order_by: 排序字段 (lastOneMonthReturn, inception_date, price_change等)
            order: 1=降序, 2=升序

        Returns:
            {"list": [...], "total": N, "page": ..., "pagesize": ...}
        """
        body: dict[str, Any] = {
            "page": page,
            "pagesize": min(pagesize, 300),
            "order_by": order_by,
            "order": order,
        }
        if keyword:
            body["keyValue"] = keyword
        if strategy_ids:
            body["strategy"] = strategy_ids
        return await self._client.api_post("/newgoapi/fund/advancedList", body)

    async def fetch_all_advanced(
        self,
        strategy_ids: list[int] | None = None,
        max_pages: int = 100,
    ) -> list[dict[str, Any]]:
        """分页获取全量基金列表并标准化。

        Args:
            strategy_ids: 策略ID过滤
            max_pages: 最大翻页数 (安全限制)

        Returns:
            标准化后的基金列表
        """
        all_funds: list[dict[str, Any]] = []
        page = 1
        while page <= max_pages:
            result = await self.advanced_search(
                strategy_ids=strategy_ids, page=page, pagesize=300
            )
            batch = result.get("list", [])
            if not batch:
                break
            all_funds.extend(self._normalize_advanced(f) for f in batch)
            total = result.get("total", 0)
            logger.info("advancedList page=%d, batch=%d, total=%d", page, len(batch), total)
            if len(all_funds) >= total:
                break
            page += 1
        return all_funds

    # ------------------------------------------------------------------
    # 公司关联基金 (所有账号可用)
    # ------------------------------------------------------------------

    async def search_funds(self, keyword: str = "") -> list[dict[str, Any]]:
        """搜索公司关联基金 (GET /newgoapi/fund/search/funds)。

        返回公司绑定的全部基金 (~20只), keyword 在客户端过滤。
        """
        data = await self._client.api_get("/newgoapi/fund/search/funds")
        if not isinstance(data, list):
            return []
        if keyword:
            kw = keyword.lower()
            data = [f for f in data if kw in (f.get("fund_name", "") or "").lower()
                    or kw in (f.get("fund_short_name", "") or "").lower()
                    or kw in (f.get("register_number", "") or "").lower()]
        return data

    async def fetch_company_funds(self) -> list[dict[str, Any]]:
        """获取公司关联的全部基金列表并标准化。"""
        raw_list = await self.search_funds()
        return [self._normalize_company_fund(raw) for raw in raw_list]

    # ------------------------------------------------------------------
    # 基金详情
    # ------------------------------------------------------------------

    async def get_fund_info(self, encode_id: str) -> dict[str, Any]:
        """获取单只基金的详细信息。

        Args:
            encode_id: 基金的 encode_id (16位hex), 非数字ID。
        """
        return await self._client.api_get("/newgoapi/funds/info", {"id": encode_id})

    # ------------------------------------------------------------------
    # 策略分类
    # ------------------------------------------------------------------

    async def get_strategy_tree(self, fund_type: int = 1) -> list[dict[str, Any]]:
        """获取策略分类树。

        Args:
            fund_type: 1=私募策略, 2=公募策略
        """
        return await self._client.api_get("/newgoapi/list/strategy", {"type": fund_type})

    async def get_strategy_categories(self) -> list[dict[str, Any]]:
        """获取策略分类及基金数量统计 (sm/category)。"""
        return await self._client.api_get("/newgoapi/sm/category")

    async def get_company_strategy_list(self) -> list[dict[str, Any]]:
        """获取公司自定义策略分类树。"""
        return await self._client.api_get("/newgoapi/common/company/strategy/list")

    # ------------------------------------------------------------------
    # 标准化
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_advanced(raw: dict[str, Any]) -> dict[str, Any]:
        """标准化 advancedList 返回的基金数据。

        原始字段: _id, id(encode_id), fund_name, fund_short_name,
        strategy_one, strategy_two, register_number, inception_date,
        price_nav, price_cw_nav, price_date, advisor, company_id,
        fund_state, fund_index, fund_index_name, factor{...}
        """
        factor = raw.get("factor") or {}
        return {
            "source_id": str(raw.get("_id", "")),
            "encode_id": raw.get("id", ""),
            "fund_name": raw.get("fund_name") or raw.get("fund_short_name") or "",
            "fund_short_name": raw.get("fund_short_name") or "",
            "filing_number": raw.get("register_number") or "",
            "strategy_type": raw.get("strategy_one") or "",
            "strategy_sub": raw.get("strategy_two") or "",
            "inception_date": raw.get("inception_date") or None,
            "latest_nav": raw.get("price_nav"),
            "latest_nav_date": raw.get("price_date") or None,
            "advisor": raw.get("advisor") or "",
            "fund_state": raw.get("fund_state", 0),
            "benchmark_name": raw.get("fund_index_name") or "",
            "ytd_return": factor.get("ytdReturn"),
            "last_1m_return": factor.get("lastOneMonthReturn"),
            "last_1y_return": factor.get("lastOneYearReturn"),
            "data_source": "fof99",
            "_raw": raw,
        }

    @staticmethod
    def _normalize_company_fund(raw: dict[str, Any]) -> dict[str, Any]:
        """标准化 fund/search/funds 返回的公司关联基金数据。

        原始字段: id, fund_name, fund_short_name, strategy_one,
        register_number, encode_id, price_nav, price_date,
        inception_date, cycle_type, advisor, company_id, active
        """
        cycle = raw.get("cycle_type", 0)
        freq = "daily" if cycle == 1 else "weekly" if cycle == 2 else "unknown"

        return {
            "source_id": str(raw.get("id", "")),
            "encode_id": raw.get("encode_id", ""),
            "fund_name": raw.get("fund_name") or raw.get("fund_short_name") or "",
            "fund_short_name": raw.get("fund_short_name") or "",
            "filing_number": raw.get("register_number") or "",
            "strategy_type": raw.get("strategy_one") or "",
            "inception_date": raw.get("inception_date") or None,
            "latest_nav": raw.get("price_nav"),
            "latest_nav_date": raw.get("price_date") or None,
            "nav_frequency": freq,
            "advisor": raw.get("advisor") or "",
            "active": raw.get("active", 1),
            "data_source": "fof99",
            "_raw": raw,
        }
