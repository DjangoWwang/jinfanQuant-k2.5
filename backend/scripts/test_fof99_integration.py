"""火富牛爬虫端到端集成测试 v2。

测试流程: 登录 → 全量基金搜索 → 公司基金 → 基金详情 → 净值(v2) → 指标 → 指数列表 → 指数历史
用法: cd backend && python scripts/test_fof99_integration.py
"""

import asyncio
import io
import json
import os
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from app.crawler.fof99.client import Fof99Client
from app.crawler.fof99.fund_scraper import FundScraper
from app.crawler.fof99.nav_scraper import NavScraper, detect_frequency
from app.crawler.fof99.index_scraper import IndexScraper

PASS = "[OK]"
FAIL = "[FAIL]"

results: list[tuple[str, str, str]] = []


def report(name: str, ok: bool, detail: str = ""):
    status = PASS if ok else FAIL
    results.append((status, name, detail))
    line = f"  {status} {name}"
    if detail:
        line += f" -- {detail}"
    print(line)


async def main():
    print("=" * 60)
    print("火富牛爬虫集成测试 v2")
    print("=" * 60)

    client = Fof99Client()
    print(f"\n设备ID: {client.device_id}")
    print(f"用户名: {os.getenv('FOF99_USERNAME')}")

    # ------------------------------------------------------------------
    # 1. 登录
    # ------------------------------------------------------------------
    print("\n--- 1. 登录 ---")
    try:
        await client.login()
        report("登录", True)
    except Exception as e:
        report("登录", False, str(e))
        await client.close()
        return

    fund_scraper = FundScraper(client)
    nav_scraper = NavScraper(client)
    index_scraper = IndexScraper(client)

    # ------------------------------------------------------------------
    # 2. 全量基金搜索 (advancedList)
    # ------------------------------------------------------------------
    print("\n--- 2. 全量基金搜索 (advancedList) ---")
    try:
        result = await fund_scraper.advanced_search(pagesize=5)
        total = result.get("total", 0)
        batch = result.get("list", [])
        report("advancedList 返回数据", total > 100, f"total={total}, batch={len(batch)}")

        if batch:
            f0 = batch[0]
            report("字段包含 id(encode_id)", bool(f0.get("id")))
            report("字段包含 fund_name", bool(f0.get("fund_name")))
            report("字段包含 strategy_one", bool(f0.get("strategy_one")))
            print(f"    字段列表: {list(f0.keys())}")
    except Exception as e:
        report("advancedList", False, str(e))

    # 关键词搜索
    try:
        result_kw = await fund_scraper.advanced_search(keyword="明河", pagesize=5)
        total_kw = result_kw.get("total", 0)
        report("关键词搜索 '明河'", total_kw > 0, f"total={total_kw}")
    except Exception as e:
        report("关键词搜索", False, str(e))

    # 策略过滤
    try:
        result_st = await fund_scraper.advanced_search(strategy_ids=[74], pagesize=5)
        total_st = result_st.get("total", 0)
        report("策略过滤 量化期货(74)", total_st > 0, f"total={total_st}")
    except Exception as e:
        report("策略过滤", False, str(e))

    # ------------------------------------------------------------------
    # 3. 公司关联基金
    # ------------------------------------------------------------------
    print("\n--- 3. 公司关联基金 ---")
    raw_funds = []
    try:
        raw_funds = await fund_scraper.search_funds()
        report("search_funds", len(raw_funds) > 0, f"{len(raw_funds)}只")
    except Exception as e:
        report("search_funds", False, str(e))

    # ------------------------------------------------------------------
    # 4. 基金详情
    # ------------------------------------------------------------------
    print("\n--- 4. 基金详情 ---")
    test_encode_id = None
    if raw_funds:
        test_encode_id = raw_funds[0].get("encode_id", "")
    if not test_encode_id:
        # 从advancedList取
        try:
            r = await fund_scraper.advanced_search(pagesize=1)
            if r.get("list"):
                test_encode_id = r["list"][0].get("id", "")
        except Exception:
            pass

    if test_encode_id:
        try:
            info = await fund_scraper.get_fund_info(test_encode_id)
            report("get_fund_info", bool(info), f"encode_id={test_encode_id}")
            if isinstance(info, dict):
                print(f"    字段: {list(info.keys())[:15]}")
        except Exception as e:
            report("get_fund_info", False, str(e))
    else:
        report("get_fund_info", False, "无可用encode_id")

    # ------------------------------------------------------------------
    # 5. 净值获取 (viewv2, 已解密)
    # ------------------------------------------------------------------
    print("\n--- 5. 净值获取 (viewv2) ---")
    # 优先从公司关联基金找有数据的; 否则从advancedList翻页找
    nav_test_id = None
    nav_test_name = ""
    for f in raw_funds:
        if f.get("price_date") and f.get("encode_id"):
            nav_test_id = f["encode_id"]
            nav_test_name = f.get("fund_short_name") or f.get("fund_name", "?")
            break
    if not nav_test_id:
        try:
            for pg in range(1, 5):
                r = await fund_scraper.advanced_search(pagesize=50, page=pg)
                for f in r.get("list", []):
                    if f.get("price_date") and f.get("id"):
                        nav_test_id = f["id"]
                        nav_test_name = f.get("fund_short_name", "?")
                        break
                if nav_test_id:
                    break
        except Exception:
            pass
    if nav_test_id:
        print(f"    测试基金: {nav_test_name} (encode={nav_test_id})")

    if nav_test_id:
        try:
            nav_list = await nav_scraper.fetch_nav_history(nav_test_id)
            report("fetch_nav_history(v2)", len(nav_list) > 0, f"{len(nav_list)}条")

            if nav_list:
                n0 = nav_list[0]
                report("净值字段 nav_date", bool(n0.get("nav_date")))
                report("净值字段 unit_nav", n0.get("unit_nav") is not None and n0["unit_nav"] > 0)
                print(f"    首条: date={n0['nav_date']}, nav={n0['unit_nav']}")
                print(f"    末条: date={nav_list[-1]['nav_date']}, nav={nav_list[-1]['unit_nav']}")

                freq = detect_frequency(nav_list)
                report("频率检测", freq in ("daily", "weekly", "monthly", "irregular"), f"freq={freq}")
        except Exception as e:
            report("fetch_nav_history(v2)", False, str(e))
    else:
        report("fetch_nav_history(v2)", False, "无可用基金")

    # ------------------------------------------------------------------
    # 6. 基金指标
    # ------------------------------------------------------------------
    print("\n--- 6. 基金指标 ---")
    if nav_test_id:
        try:
            metrics = await nav_scraper.fetch_fund_metrics(nav_test_id)
            report("fetch_fund_metrics", bool(metrics))
            if metrics:
                for key in ["sharpe", "max_drawdown", "annual_return", "alpha", "beta"]:
                    val = metrics.get(key)
                    report(f"  指标 {key}", val is not None, str(val))
        except Exception as e:
            report("fetch_fund_metrics", False, str(e))

    # ------------------------------------------------------------------
    # 7. 策略分类
    # ------------------------------------------------------------------
    print("\n--- 7. 策略分类 ---")
    try:
        tree = await fund_scraper.get_strategy_tree(fund_type=1)
        report("get_strategy_tree", bool(tree), f"{len(tree)}个一级策略")
    except Exception as e:
        report("get_strategy_tree", False, str(e))

    try:
        cats = await fund_scraper.get_strategy_categories()
        report("get_strategy_categories", bool(cats), f"{len(cats)}个分类")
        if cats:
            for c in cats[:3]:
                print(f"    {c.get('name')}: {c.get('funds_total')}只")
    except Exception as e:
        report("get_strategy_categories", False, str(e))

    # ------------------------------------------------------------------
    # 8. 指数列表
    # ------------------------------------------------------------------
    print("\n--- 8. 指数列表 ---")
    index_list = []
    try:
        index_list = await index_scraper.fetch_index_list()
        report("fetch_index_list", len(index_list) > 0, f"{len(index_list)}个指数")
    except Exception as e:
        report("fetch_index_list", False, str(e))

    # ------------------------------------------------------------------
    # 9. 指数历史净值
    # ------------------------------------------------------------------
    print("\n--- 9. 指数历史净值 ---")
    if index_list:
        # 找沪深300
        hs300 = next((i for i in index_list if i["index_code"] == "000300"), index_list[0])
        hs300_id = hs300["source_id"]
        print(f"    测试指数: {hs300['index_name']} (id={hs300_id[:16]}...)")

        try:
            prices = await index_scraper.fetch_index_history(hs300_id)
            report("fetch_index_history", len(prices) > 100, f"{len(prices)}条")
            if prices:
                print(f"    首条: {prices[0]['date']}")
                print(f"    末条: {prices[-1]['date']}")
        except Exception as e:
            report("fetch_index_history", False, str(e))

    # ------------------------------------------------------------------
    # 10. 牛牛自建指数
    # ------------------------------------------------------------------
    print("\n--- 10. 牛牛自建指数 ---")
    try:
        nn = await index_scraper.fetch_nn_index_prices(start_date="2026-01-01")
        report("fetch_nn_index_prices", len(nn) > 0, f"{len(nn)}个指数")
        for item in nn:
            print(f"    {item['index_name']}: {len(item['prices'])}条")
    except Exception as e:
        report("fetch_nn_index_prices", False, str(e))

    # ------------------------------------------------------------------
    # 汇总
    # ------------------------------------------------------------------
    await client.close()

    print("\n" + "=" * 60)
    passed = sum(1 for s, _, _ in results if s == PASS)
    failed = sum(1 for s, _, _ in results if s == FAIL)
    print(f"结果: {passed} 通过, {failed} 失败, 共 {len(results)} 项")
    if failed:
        print("\n失败项:")
        for s, name, detail in results:
            if s == FAIL:
                print(f"  {FAIL} {name}: {detail}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
