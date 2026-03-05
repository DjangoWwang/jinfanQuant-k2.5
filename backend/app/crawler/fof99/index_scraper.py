"""FOF99 market index data scraper.

已验证API端点:
- GET /newgoapi/index/indexList - 指数元数据列表 (100+个标准指数)
- GET /newgoapi/global/index/configs - 指数配置 (9组227个, 含encode_id)
- GET /pyapi/fund/viewv2?fid=<indexList.id> - 指数历史净值 (已解密)
- GET /newgoapi/fund/index/list - 牛牛自建指数价格 (未加密)
"""

from __future__ import annotations

import logging
from typing import Any

from app.crawler.fof99.client import Fof99Client

logger = logging.getLogger(__name__)


class IndexScraper:
    """从火富牛平台爬取指数/基准数据。"""

    def __init__(self, client: Fof99Client) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # 指数列表
    # ------------------------------------------------------------------

    async def fetch_index_list(self) -> list[dict[str, Any]]:
        """获取标准指数列表 (GET /newgoapi/index/indexList)。

        返回约100个标准指数, 含最新价格和基准日期。
        """
        data = await self._client.api_get("/newgoapi/index/indexList")
        if not isinstance(data, list):
            return []
        return [self._normalize_index_meta(item) for item in data]

    async def fetch_index_configs(self) -> list[dict[str, Any]]:
        """获取分组指数配置 (GET /newgoapi/global/index/configs)。

        返回9组约227个指数 (A股/港股/私募/公募/债券/商品/其他/自建等)。
        每个指数有 id(数字), code, fund_name, id_encode。
        """
        return await self._client.api_get("/newgoapi/global/index/configs")

    # ------------------------------------------------------------------
    # 指数价格历史
    # ------------------------------------------------------------------

    async def fetch_index_history(
        self,
        index_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """获取指数历史净值序列。

        通过 /pyapi/fund/viewv2?fid=<indexList.id> 获取。
        viewv2 返回的 prices 已解密（列表格式）。

        Args:
            index_id: 指数的 id (从 indexList 获取的32位hex)
            start_date: 起始日期 YYYY-MM-DD (可选)
            end_date: 截止日期 YYYY-MM-DD (可选)

        Returns:
            按日期升序的标准化价格列表
        """
        params: dict[str, Any] = {"fid": index_id}
        if start_date:
            params["sd"] = start_date
        if end_date:
            params["ed"] = end_date

        data = await self._client.pyapi_get("/pyapi/fund/viewv2", params)
        if not data:
            return []

        fund = data.get("fund", {})
        raw_prices = fund.get("prices", [])

        if not isinstance(raw_prices, list):
            logger.warning("指数 %s prices非列表类型: %s", index_id, type(raw_prices).__name__)
            return []

        result = [self._normalize_price(p) for p in raw_prices]
        result.sort(key=lambda x: x["date"])
        logger.info("获取指数价格 index=%s (%s): %d条", index_id, fund.get("fund_name", ""), len(result))
        return result

    async def fetch_index_metrics(self, index_id: str) -> dict[str, Any]:
        """获取指数的指标数据 (不加密)。"""
        data = await self._client.pyapi_get("/pyapi/fund/viewv2", {"fid": index_id})
        if not data:
            return {}

        fund = data.get("fund", {})
        return {
            "index_name": fund.get("fund_name", ""),
            "annual_return": fund.get("annual_return"),
            "cum_return": fund.get("cum_return"),
            "max_drawdown": fund.get("max_drawdown"),
            "sharpe_ratio": fund.get("sharpe_ratio"),
            "volatility": fund.get("vol"),
            "start_date": data.get("startDate"),
            "end_date": data.get("endDate"),
        }

    # ------------------------------------------------------------------
    # 牛牛自建指数 (火富牛自有策略指数)
    # ------------------------------------------------------------------

    async def fetch_nn_index_prices(
        self,
        index_ids: str = "NNGPZS,NNQHZS,NNTLZS,NNZQZS",
        start_date: str = "2020-01-01",
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """获取牛牛自建指数价格 (GET /newgoapi/fund/index/list)。

        价格数据未加密，直接返回列表。

        Args:
            index_ids: 逗号分隔的指数代码 (NNGPZS=中性, NNQHZS=期货, NNTLZS=套利, NNZQZS=指增)
            start_date: 起始日期
            end_date: 截止日期 (默认今天)
        """
        if not end_date:
            from datetime import date
            end_date = date.today().isoformat()

        data = await self._client.api_get("/newgoapi/fund/index/list", {
            "id": index_ids,
            "startTime": start_date,
            "endTime": end_date,
        })
        if not isinstance(data, list):
            return []

        result = []
        for item in data:
            prices = item.get("prices", [])
            result.append({
                "index_name": item.get("fund_name", ""),
                "index_id": item.get("id"),
                "prices": [{"date": p["pd"], "close": p.get("cn") or p.get("pn", 0)} for p in prices],
            })
        return result

    # ------------------------------------------------------------------
    # 标准化
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_index_meta(raw: dict[str, Any]) -> dict[str, Any]:
        """标准化 indexList 返回的指数元数据。

        原始字段: id(32位hex), name, short_name, code, price_nav,
        price_date, category, baseline_date
        """
        return {
            "source_id": raw.get("id", ""),
            "index_code": raw.get("code", ""),
            "index_name": raw.get("name") or raw.get("short_name") or "",
            "short_name": raw.get("short_name") or "",
            "category": raw.get("category", ""),
            "latest_price": raw.get("price_nav"),
            "latest_date": raw.get("price_date") or None,
            "baseline_date": raw.get("baseline_date") or None,
        }

    @staticmethod
    def _normalize_price(raw: dict[str, Any]) -> dict[str, Any]:
        """标准化指数价格记录。

        viewv2 格式: {pd, nav, cnw, cn, drawdown, pc}
        """
        return {
            "date": raw.get("pd", ""),
            "close": float(raw.get("nav") or raw.get("cn") or 0),
            "cumulative": float(raw.get("cnw") or raw.get("cn") or 0),
            "drawdown": float(raw["drawdown"]) if raw.get("drawdown") is not None else None,
            "change_pct": float(raw["pc"]) if isinstance(raw.get("pc"), (int, float)) else None,
        }
