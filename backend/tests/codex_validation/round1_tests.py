"""Codex交叉验证 — 第1轮综合测试脚本。

包含4类测试:
1. 回测引擎多轮验证
2. 数据完整性校验
3. 指标计算交叉验证
4. 压力测试
"""

import asyncio
import json
import os
import sys
import time
import statistics
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
import requests
import psycopg2

# ============================================================
# 配置
# ============================================================
API_BASE = "http://localhost:8000/api/v1"
DB_CONN = "dbname=fof_platform user=fof_user password=jinfan2026 host=localhost"
RISK_FREE_RATE = 0.02

results_summary = []


def safe_fmt(val, fmt=".4f"):
    """安全格式化，处理None值。"""
    if val is None:
        return "None"
    try:
        return f"{val:{fmt}}"
    except (TypeError, ValueError):
        return str(val)


def log_result(category, test_name, passed, details=""):
    status = "PASS" if passed else "FAIL"
    results_summary.append({
        "category": category,
        "test": test_name,
        "status": status,
        "details": details,
    })
    print(f"  [{status}] {test_name}: {details}")


# ============================================================
# 辅助: 查找有数据的基金ID
# ============================================================
def find_funds_with_data(min_records=50, count=10):
    """从DB找有足够NAV数据的基金ID。"""
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    cur.execute("""
        SELECT f.id, f.fund_name, COUNT(n.id) as cnt
        FROM funds f
        JOIN nav_history n ON n.fund_id = f.id
        WHERE f.fof99_fund_id IS NOT NULL
        GROUP BY f.id, f.fund_name
        HAVING COUNT(n.id) >= %s
        ORDER BY cnt DESC
        LIMIT %s
    """, (min_records, count))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [(r[0], r[1], r[2]) for r in rows]


# ============================================================
# 1. 回测引擎多轮验证
# ============================================================
def test_backtest_engine():
    print("\n" + "=" * 60)
    print("1. 回测引擎多轮验证")
    print("=" * 60)

    funds = find_funds_with_data(min_records=100, count=5)
    if len(funds) < 2:
        log_result("backtest", "数据不足", False, f"仅找到{len(funds)}只有足够数据的基金")
        return

    f1, f2, f3 = funds[0][0], funds[1][0], funds[2][0] if len(funds) > 2 else funds[0][0]

    # 测试1.1: 等权双基金 intersection模式
    print("\n--- 测试1.1: 等权双基金 intersection ---")
    config = {
        "weights": [
            {"fund_id": f1, "weight": 0.5},
            {"fund_id": f2, "weight": 0.5},
        ],
        "start_date": "2024-01-01",
        "end_date": "2025-01-01",
        "rebalance_frequency": "monthly",
        "transaction_cost_bps": 0,
        "history_mode": "intersection",
        "risk_free_rate": RISK_FREE_RATE,
    }
    resp = requests.post(f"{API_BASE}/backtest/run", json=config, timeout=60)
    if resp.status_code == 200:
        data = resp.json()
        m = data["metrics"]

        # 检查指标合理性
        tr = m.get("total_return") or 0
        md = m.get("max_drawdown") or 0
        detail = f"total_ret={safe_fmt(tr)}, max_dd={safe_fmt(md)}"

        checks = []
        checks.append(("total_return范围", -0.80 <= tr <= 2.0))
        checks.append(("max_drawdown范围", -0.95 <= md <= 0))
        checks.append(("nav_series非空", len(data["nav_series"]) > 0))
        if data["nav_series"]:
            first_nav = data["nav_series"][0].get("nav") or 1
            last_nav = data["nav_series"][-1].get("nav") or 1
            implied_return = last_nav / first_nav - 1 if first_nav else 0
            checks.append(("NAV一致性", abs(implied_return - tr) < 0.01))
            dates_sorted = all(
                data["nav_series"][i]["date"] < data["nav_series"][i+1]["date"]
                for i in range(len(data["nav_series"]) - 1)
            )
            checks.append(("日期递增", dates_sorted))

        for name, passed in checks:
            log_result("backtest", f"1.1-{name}", passed, detail)
    else:
        log_result("backtest", "1.1-HTTP请求", False, f"status={resp.status_code}: {resp.text[:200]}")

    # 测试1.2: 基金+指数 dynamic_entry模式
    print("\n--- 测试1.2: 基金+指数 dynamic_entry ---")
    config2 = {
        "weights": [
            {"fund_id": f1, "weight": 0.4},
            {"fund_id": f2, "weight": 0.3},
            {"index_code": "000300", "weight": 0.3},
        ],
        "start_date": "2023-06-01",
        "end_date": "2025-06-01",
        "rebalance_frequency": "quarterly",
        "transaction_cost_bps": 10,
        "history_mode": "dynamic_entry",
        "risk_free_rate": RISK_FREE_RATE,
    }
    resp2 = requests.post(f"{API_BASE}/backtest/run", json=config2, timeout=60)
    if resp2.status_code == 200:
        data2 = resp2.json()
        m2 = data2["metrics"]
        tr2 = m2.get("total_return") or 0
        md2 = m2.get("max_drawdown") or 0
        log_result("backtest", "1.2-HTTP成功", True, f"total_ret={safe_fmt(tr2)}")
        log_result("backtest", "1.2-指标合理", -2.0 <= tr2 <= 5.0,
                  f"max_dd={safe_fmt(md2)}")

        # 交易成本验证: 无成本版本收益应>=有成本版本
        config2_nocost = {**config2, "transaction_cost_bps": 0}
        resp2b = requests.post(f"{API_BASE}/backtest/run", json=config2_nocost, timeout=60)
        if resp2b.status_code == 200:
            m2b = resp2b.json()["metrics"]
            tr2b = m2b.get("total_return") or 0
            log_result("backtest", "1.2-交易成本", tr2b >= tr2 - 0.001,
                      f"无成本={safe_fmt(tr2b)}, 有成本={safe_fmt(tr2)}")
    else:
        log_result("backtest", "1.2-HTTP请求", False, f"status={resp2.status_code}: {resp2.text[:200]}")

    # 测试1.3: 双指数 truncate模式
    print("\n--- 测试1.3: 双指数 truncate ---")
    config3 = {
        "weights": [
            {"index_code": "000300", "weight": 0.5},
            {"index_code": "000905", "weight": 0.5},
        ],
        "start_date": "2024-06-01",
        "end_date": "2025-03-01",
        "rebalance_frequency": "monthly",
        "transaction_cost_bps": 0,
        "history_mode": "truncate",
        "risk_free_rate": RISK_FREE_RATE,
    }
    resp3 = requests.post(f"{API_BASE}/backtest/run", json=config3, timeout=60)
    if resp3.status_code == 200:
        data3 = resp3.json()
        m3 = data3["metrics"]
        tr3 = m3.get("total_return") or 0
        md3 = m3.get("max_drawdown") or 0
        nav_count = len(data3["nav_series"])
        log_result("backtest", "1.3-HTTP成功", True, f"total_ret={safe_fmt(tr3)}")
        log_result("backtest", "1.3-数据量", nav_count >= 20,
                  f"nav_series={nav_count}条")
        log_result("backtest", "1.3-指标合理",
                  -0.50 <= tr3 <= 1.0 and -0.60 <= md3 <= 0,
                  f"total_ret={safe_fmt(tr3)}, max_dd={safe_fmt(md3)}")
    else:
        log_result("backtest", "1.3-HTTP请求", False, f"status={resp3.status_code}: {resp3.text[:200]}")


# ============================================================
# 2. 数据完整性校验
# ============================================================
def test_data_integrity():
    print("\n" + "=" * 60)
    print("2. 数据完整性校验")
    print("=" * 60)

    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    funds = find_funds_with_data(min_records=10, count=5)
    fund_ids = [f[0] for f in funds]

    # 测试2.1: NAV记录数一致性
    print("\n--- 测试2.1: NAV记录数一致性 ---")
    for fid in fund_ids:
        cur.execute("""
            SELECT COUNT(*), MIN(nav_date)::text, MAX(nav_date)::text
            FROM nav_history WHERE fund_id = %s
        """, (fid,))
        db_count, db_first, db_last = cur.fetchone()

        resp = requests.get(f"{API_BASE}/funds/{fid}/nav", timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            api_count = data.get("total_count", len(data.get("records", [])))
            records = data.get("records", [])
            api_first = records[0]["nav_date"] if records else None
            api_last = records[-1]["nav_date"] if records else None

            count_match = api_count == db_count
            first_match = str(api_first) == str(db_first) if api_first else True
            last_match = str(api_last) == str(db_last) if api_last else True

            log_result("data_integrity", f"2.1-基金{fid}记录数",
                      count_match,
                      f"API={api_count}, DB={db_count}")
            if not first_match or not last_match:
                log_result("data_integrity", f"2.1-基金{fid}日期",
                          first_match and last_match,
                          f"API={api_first}~{api_last}, DB={db_first}~{db_last}")
        else:
            log_result("data_integrity", f"2.1-基金{fid}", False, f"status={resp.status_code}")

    # 测试2.2: NAV值精确一致性
    print("\n--- 测试2.2: NAV值精确一致性 ---")
    test_fid = fund_ids[0] if fund_ids else 1
    cur.execute("""
        SELECT nav_date::text, unit_nav, cumulative_nav
        FROM nav_history WHERE fund_id = %s
        ORDER BY nav_date ASC LIMIT 5
    """, (test_fid,))
    db_head = cur.fetchall()

    resp = requests.get(f"{API_BASE}/funds/{test_fid}/nav", timeout=30)
    if resp.status_code == 200:
        records = resp.json().get("records", [])
        all_match = True
        for i, (db_date, db_unit, db_cum) in enumerate(db_head):
            if i >= len(records):
                all_match = False
                break
            api_rec = records[i]
            if str(api_rec["nav_date"]) != db_date:
                all_match = False
                break
            if db_unit is not None and api_rec.get("unit_nav") is not None:
                if abs(float(api_rec["unit_nav"]) - float(db_unit)) > 0.0001:
                    all_match = False
                    break
        log_result("data_integrity", f"2.2-NAV值精确(基金{test_fid})", all_match,
                  f"对比前{len(db_head)}条")
    else:
        log_result("data_integrity", f"2.2-NAV值精确", False, f"status={resp.status_code}")

    # 测试2.3: 指数数据完整性
    print("\n--- 测试2.3: 指数数据完整性 ---")
    index_codes = ["000300", "000905", "000852"]
    for code in index_codes:
        cur.execute("""
            SELECT COUNT(*), MIN(nav_date)::text, MAX(nav_date)::text
            FROM index_nav WHERE index_code = %s
        """, (code,))
        cnt, first, last = cur.fetchone()
        log_result("data_integrity", f"2.3-指数{code}",
                  cnt is not None and cnt > 1000,
                  f"records={cnt}, range={first}~{last}")

    # 验证搜索API能找到指数
    resp = requests.get(f"{API_BASE}/backtest/search-assets?q=&asset_type=index", timeout=15)
    if resp.status_code == 200:
        found = resp.json()
        found_codes = {item.get("index_code") or item.get("code", "") for item in found}
        for code in index_codes:
            log_result("data_integrity", f"2.3-搜索{code}",
                      code in found_codes,
                      f"搜索到{len(found)}只指数")
    else:
        log_result("data_integrity", "2.3-搜索API", False, f"status={resp.status_code}")

    cur.close()
    conn.close()


# ============================================================
# 3. 指标计算交叉验证
# ============================================================
def test_metrics_crosscheck():
    print("\n" + "=" * 60)
    print("3. 指标计算交叉验证")
    print("=" * 60)

    funds = find_funds_with_data(min_records=100, count=3)
    if not funds:
        log_result("metrics", "数据不足", False, "没有找到足够数据的基金")
        return

    for fund_info in funds:
        fid = fund_info[0]
        fname = fund_info[1]
        print(f"\n--- 基金{fid}({fname[:15]}) ---")

        # 获取NAV数据
        resp = requests.get(f"{API_BASE}/funds/{fid}/nav", timeout=30)
        if resp.status_code != 200:
            log_result("metrics", f"3-基金{fid}NAV获取", False, f"status={resp.status_code}")
            continue

        records = resp.json().get("records", [])
        if len(records) < 10:
            log_result("metrics", f"3-基金{fid}数据量", False, f"仅{len(records)}条")
            continue

        # 构建NAV Series (复现fund_service.get_nav_series逻辑)
        same_count = 0
        diff_count = 0
        for r in records:
            u = r.get("unit_nav")
            c = r.get("cumulative_nav")
            if u is not None and c is not None:
                if abs(float(u) - float(c)) < 0.01:
                    same_count += 1
                else:
                    diff_count += 1

        total = same_count + diff_count
        is_interleaved = (total > 20 and same_count > total * 0.15 and diff_count > total * 0.15)

        pairs = []
        for r in records:
            u = r.get("unit_nav")
            c = r.get("cumulative_nav")
            if is_interleaved:
                if u is not None and c is not None and abs(float(u) - float(c)) < 0.01:
                    pairs.append((r["nav_date"], float(c)))
            else:
                nav_val = float(c) if c is not None else (float(u) if u is not None else None)
                if nav_val is not None:
                    pairs.append((r["nav_date"], nav_val))

        if len(pairs) < 5:
            log_result("metrics", f"3-基金{fid}有效数据", False, f"仅{len(pairs)}条有效")
            continue

        dates, navs = zip(*pairs)
        nav_series = pd.Series(navs, index=pd.DatetimeIndex(dates)).sort_index().dropna()

        # 独立计算指标
        total_return = nav_series.iloc[-1] / nav_series.iloc[0] - 1

        n_periods = len(nav_series) - 1
        deltas = pd.Series(nav_series.index).diff().dropna().dt.days
        avg_gap = float(deltas.mean()) if len(deltas) > 0 else 1
        ann_factor = 365.0 / avg_gap if avg_gap > 0 else 365.0

        base = 1 + total_return
        if base > 0:
            ann_return = base ** (ann_factor / n_periods) - 1
            ann_return = max(min(ann_return, 99.99), -0.9999)
        else:
            ann_return = -1.0

        cummax = nav_series.cummax()
        drawdown = (nav_series - cummax) / cummax
        max_dd = float(drawdown.min())

        daily_rets = nav_series.pct_change().dropna()
        if len(daily_rets) > 1:
            ann_vol = float(daily_rets.std(ddof=1) * np.sqrt(ann_factor))
        else:
            ann_vol = 0.0

        sharpe = (ann_return - RISK_FREE_RATE) / ann_vol if ann_vol > 0 else 0.0

        # 获取API指标
        m_resp = requests.get(
            f"{API_BASE}/funds/{fid}/metrics?preset=inception&risk_free_rate={RISK_FREE_RATE}",
            timeout=30,
        )
        if m_resp.status_code != 200:
            log_result("metrics", f"3-基金{fid}API指标", False, f"status={m_resp.status_code}")
            continue

        m = m_resp.json()

        # 对比
        tolerances = {
            "total_return": 0.01,
            "annualized_return": 0.02,
            "max_drawdown": 0.01,
            "annualized_volatility": 0.02,
            "sharpe_ratio": 0.1,
        }

        calc_vals = {
            "total_return": total_return,
            "annualized_return": ann_return,
            "max_drawdown": max_dd,
            "annualized_volatility": ann_vol,
            "sharpe_ratio": sharpe,
        }

        for metric_name, calc_val in calc_vals.items():
            api_val = m.get(metric_name, 0) or 0
            diff = abs(calc_val - api_val)
            tol = tolerances[metric_name]
            passed = diff <= tol
            log_result("metrics", f"3-基金{fid}-{metric_name}", passed,
                      f"calc={calc_val:.6f}, api={api_val:.6f}, diff={diff:.6f}, tol={tol}")


# ============================================================
# 4. 压力测试
# ============================================================
def test_stress():
    print("\n" + "=" * 60)
    print("4. 压力测试")
    print("=" * 60)

    funds = find_funds_with_data(min_records=50, count=10)
    fund_ids = [f[0] for f in funds]

    # 测试4.1: 基金列表+指标并发
    print("\n--- 测试4.1: 基金列表+指标并发 ---")
    import concurrent.futures

    for concurrency in [10, 30]:
        urls = []
        for i in range(60):
            if i % 2 == 0:
                page = (i // 2) % 5 + 1
                urls.append(f"{API_BASE}/funds/?page={page}&page_size=50")
            else:
                fid = fund_ids[i % len(fund_ids)] if fund_ids else 1
                urls.append(f"{API_BASE}/funds/{fid}/metrics?preset=1y")

        results = []
        t0 = time.time()

        def fetch_url(url):
            start = time.perf_counter()
            try:
                r = requests.get(url, timeout=30)
                elapsed = (time.perf_counter() - start) * 1000
                return {"status": r.status_code, "time_ms": elapsed}
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                return {"status": 0, "time_ms": elapsed, "error": str(e)}

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(fetch_url, urls))

        total_time = time.time() - t0
        success = [r for r in results if 200 <= r["status"] < 400]
        times = [r["time_ms"] for r in results if r["status"] != 0]
        errors = [r for r in results if r["status"] == 0 or r["status"] >= 500]

        success_rate = len(success) / len(results)
        avg_ms = statistics.mean(times) if times else 0
        p95_ms = sorted(times)[int(len(times) * 0.95)] if len(times) >= 2 else 0

        log_result("stress", f"4.1-并发{concurrency}-成功率",
                  success_rate >= 0.90,
                  f"{success_rate:.1%} ({len(success)}/{len(results)})")
        log_result("stress", f"4.1-并发{concurrency}-P95",
                  p95_ms < 10000,
                  f"P95={p95_ms:.0f}ms, avg={avg_ms:.0f}ms, total={total_time:.1f}s")
        if errors:
            log_result("stress", f"4.1-并发{concurrency}-错误",
                      len(errors) <= 3,
                      f"errors={len(errors)}: {errors[:2]}")

    # 测试4.2: 回测并发
    print("\n--- 测试4.2: 回测API并发 ---")
    f1 = fund_ids[0] if fund_ids else 1
    f2 = fund_ids[1] if len(fund_ids) > 1 else 1

    backtest_configs = [
        {
            "weights": [{"fund_id": f1, "weight": 1.0}],
            "start_date": "2024-10-01", "end_date": "2025-01-01",
            "rebalance_frequency": "monthly", "transaction_cost_bps": 0,
            "history_mode": "intersection",
        },
        {
            "weights": [
                {"fund_id": f1, "weight": 0.5},
                {"index_code": "000300", "weight": 0.5},
            ],
            "start_date": "2024-06-01", "end_date": "2025-01-01",
            "rebalance_frequency": "monthly", "transaction_cost_bps": 5,
            "history_mode": "intersection",
        },
    ]

    for concurrency in [5, 10]:
        configs = [backtest_configs[i % len(backtest_configs)] for i in range(20)]
        results = []

        def run_backtest(cfg):
            start = time.perf_counter()
            try:
                r = requests.post(f"{API_BASE}/backtest/run", json=cfg, timeout=60)
                elapsed = (time.perf_counter() - start) * 1000
                return {"status": r.status_code, "time_ms": elapsed}
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                return {"status": 0, "time_ms": elapsed, "error": str(e)}

        t0 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(run_backtest, configs))
        total_time = time.time() - t0

        success = [r for r in results if r["status"] == 200]
        times = [r["time_ms"] for r in results if r["status"] != 0]
        success_rate = len(success) / len(results)
        p95_ms = sorted(times)[int(len(times) * 0.95)] if len(times) >= 2 else 0

        log_result("stress", f"4.2-回测并发{concurrency}-成功率",
                  success_rate >= 0.80,
                  f"{success_rate:.1%} ({len(success)}/{len(results)})")
        log_result("stress", f"4.2-回测并发{concurrency}-P95",
                  p95_ms < 30000,
                  f"P95={p95_ms:.0f}ms, total={total_time:.1f}s")


# ============================================================
# 主函数
# ============================================================
def main():
    print("Codex交叉验证 — 第1轮测试")
    print(f"时间: {date.today()}")
    print(f"API: {API_BASE}")
    print()

    # 检查API可达
    try:
        r = requests.get(f"{API_BASE.rsplit('/api', 1)[0]}/health", timeout=5)
        print(f"后端状态: {r.json()}")
    except Exception as e:
        print(f"错误: 后端不可达 - {e}")
        return

    test_backtest_engine()
    test_data_integrity()
    test_metrics_crosscheck()
    test_stress()

    # 汇总
    print("\n" + "=" * 60)
    print("测试汇总")
    print("=" * 60)
    total = len(results_summary)
    passed = sum(1 for r in results_summary if r["status"] == "PASS")
    failed = sum(1 for r in results_summary if r["status"] == "FAIL")
    print(f"总计: {total} 项 | 通过: {passed} | 失败: {failed}")
    print(f"通过率: {passed/total*100:.1f}%" if total > 0 else "无测试")

    if failed > 0:
        print("\n失败项:")
        for r in results_summary:
            if r["status"] == "FAIL":
                print(f"  [{r['category']}] {r['test']}: {r['details']}")

    # 输出JSON结果
    output_path = os.path.join(os.path.dirname(__file__), "round1_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "round": 1,
            "date": str(date.today()),
            "total": total,
            "passed": passed,
            "failed": failed,
            "details": results_summary,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: {output_path}")


if __name__ == "__main__":
    main()
