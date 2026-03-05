"""FOF99 NAV (net asset value) history scraper.

已验证API端点和参数:
- GET /pyapi/fund/view — prices字段DES加密 (v1)
  params: fid(encode_id), pt(1), cycle(2), sd, ed
  返回: {fund: {prices(加密hex), fund_name, alpha, beta, ...}, index: {...}}
- GET /pyapi/fund/viewv2 — prices字段已解密 (v2) ⭐推荐
  params: fid(encode_id), refer(指数id,可选), sd, ed
  返回: {fund: {prices(列表), ...}, index: {...}}
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from app.crawler.fof99.client import Fof99Client, decrypt_prices

logger = logging.getLogger(__name__)


def detect_frequency(nav_records: list[dict[str, Any]]) -> str:
    """根据净值日期间隔自动识别数据频率。"""
    if len(nav_records) < 3:
        return "irregular"

    deltas: list[int] = []
    for i in range(1, min(len(nav_records), 30)):
        d1 = date.fromisoformat(nav_records[i - 1]["nav_date"])
        d2 = date.fromisoformat(nav_records[i]["nav_date"])
        deltas.append(abs((d2 - d1).days))

    if not deltas:
        return "irregular"

    avg_delta = sum(deltas) / len(deltas)
    if avg_delta <= 2:
        return "daily"
    elif avg_delta <= 8:
        return "weekly"
    elif avg_delta <= 35:
        return "monthly"
    return "irregular"


class NavScraper:
    """从火富牛平台爬取基金历史净值数据。"""

    def __init__(self, client: Fof99Client) -> None:
        self._client = client

    async def fetch_nav_history(
        self,
        fund_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
        use_v2: bool = True,
    ) -> list[dict[str, Any]]:
        """获取基金历史净值序列。

        Args:
            fund_id: 基金的 encode_id (16位hex)。
            start_date: 起始日期 YYYY-MM-DD。
            end_date: 截止日期 YYYY-MM-DD。
            use_v2: True=viewv2(已解密), False=view(需DES解密)

        Returns:
            按日期升序的标准化净值列表。
        """
        if use_v2:
            return await self._fetch_via_viewv2(fund_id, start_date, end_date)
        return await self._fetch_via_view(fund_id, start_date, end_date)

    async def _fetch_via_viewv2(
        self, fund_id: str, start_date: str | None, end_date: str | None
    ) -> list[dict[str, Any]]:
        """通过 viewv2 获取 (prices已解密为列表)。"""
        params: dict[str, Any] = {"fid": fund_id}
        if start_date:
            params["sd"] = start_date
        if end_date:
            params["ed"] = end_date

        data = await self._client.pyapi_get("/pyapi/fund/viewv2", params)
        if not data:
            return []

        fund_data = data.get("fund", {})
        raw_prices = fund_data.get("prices", [])

        if not isinstance(raw_prices, list):
            logger.warning("基金 %s viewv2 prices非列表: %s", fund_id, type(raw_prices).__name__)
            return []
        if not raw_prices:
            logger.warning("基金 %s 无净值数据", fund_id)
            return []

        nav_list = [self._normalize_nav(p) for p in raw_prices]
        nav_list.sort(key=lambda x: x["nav_date"])
        logger.info("获取净值(v2) fund=%s: %d条记录", fund_id, len(nav_list))
        return nav_list

    async def _fetch_via_view(
        self, fund_id: str, start_date: str | None, end_date: str | None
    ) -> list[dict[str, Any]]:
        """通过 view(v1) 获取 (prices需DES解密)。"""
        params: dict[str, Any] = {"fid": fund_id, "pt": 1, "cycle": 2}
        if start_date:
            params["sd"] = start_date
        if end_date:
            params["ed"] = end_date

        data = await self._client.pyapi_get("/pyapi/fund/view", params)
        if not data:
            return []

        fund_data = data.get("fund", {})
        encrypted = fund_data.get("prices", "")
        if not encrypted or not isinstance(encrypted, str):
            logger.warning("基金 %s 无加密净值数据", fund_id)
            return []

        raw_prices = decrypt_prices(encrypted)
        nav_list = [self._normalize_nav(p) for p in raw_prices]
        nav_list.sort(key=lambda x: x["nav_date"])
        logger.info("获取净值(v1) fund=%s: %d条记录", fund_id, len(nav_list))
        return nav_list

    async def fetch_fund_metrics(self, fund_id: str) -> dict[str, Any]:
        """获取基金的完整指标数据。

        viewv2 返回的指标字段名与 view(v1) 略有不同:
        - v1: sharpe, volatility
        - v2: sharpe_ratio, vol
        """
        data = await self._client.pyapi_get("/pyapi/fund/viewv2", {"fid": fund_id})
        if not data:
            return {}

        fund = data.get("fund", {})
        return {
            "fund_name": fund.get("fund_name", ""),
            "annual_return": fund.get("annual_return"),
            "cum_return": fund.get("cum_return"),
            "max_drawdown": fund.get("max_drawdown"),
            "sharpe": fund.get("sharpe_ratio") or fund.get("sharpe"),
            "calmar_ratio": fund.get("calmar_ratio"),
            "sortino_ratio": fund.get("sortino_ratio"),
            "alpha": fund.get("alpha"),
            "beta": fund.get("beta"),
            "volatility": fund.get("vol") or fund.get("volatility"),
            "downside_risk": fund.get("downside_risk"),
            "corr": fund.get("corr"),
            "cVaR": fund.get("cVaR"),
            "information_ratio": fund.get("information_ratio"),
            "tracking_error": fund.get("tracking_error"),
            "start_date": data.get("startDate"),
            "end_date": data.get("endDate"),
        }

    async def fetch_nav_with_frequency(
        self, fund_id: str, start_date: str | None = None
    ) -> tuple[list[dict[str, Any]], str]:
        """获取净值并自动检测频率。"""
        records = await self.fetch_nav_history(fund_id, start_date)
        freq = detect_frequency(records) if records else "irregular"
        return records, freq

    @staticmethod
    def _normalize_nav(raw: dict[str, Any]) -> dict[str, Any]:
        """标准化净值记录。

        火富牛格式: {pd, nav, cnw, cn, drawdown, pc}
        viewv2还可能有: e_pc (超额收益变化)
        """
        return {
            "nav_date": raw.get("pd", ""),
            "unit_nav": float(raw.get("nav", 0)),
            "cumulative_nav": float(raw.get("cnw") or raw.get("cn") or raw.get("nav", 0)),
            "drawdown": float(raw["drawdown"]) if raw.get("drawdown") is not None else None,
            "change_pct": float(raw["pc"]) if isinstance(raw.get("pc"), (int, float)) else None,
        }
