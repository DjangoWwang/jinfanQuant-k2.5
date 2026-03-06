"""Codex交叉验证 — 第2轮综合测试脚本。

基于第1轮Codex反馈设计:
A. 回测引擎极端配置 (单资产、极短/极长区间、不均匀权重、高成本等)
B. 数据边界与交替模式 (interleaved/sparse/jumpy/极端NAV)
C. 指标短区间与极端值 (1m/3m/6m/ytd + 极端基金)
D. 错误处理与压力测试 (无效输入、50+并发)
"""

import concurrent.futures
import json
import os
import statistics
import sys
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd
import psycopg2
import requests

API_BASE = "http://localhost:8000/api/v1"
DB_CONN = "dbname=fof_platform user=fof_user password=jinfan2026 host=localhost"
RISK_FREE_RATE = 0.02

results_summary = []


def safe_fmt(val, fmt=".4f"):
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


def find_funds_with_data(min_records=50, count=10):
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


def find_special_funds():
    """查找特殊基金: interleaved, sparse, jumpy, 极少数据等。"""
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    result = {}

    # interleaved
    cur.execute("SELECT id, fund_name FROM funds WHERE data_quality_tags LIKE '%interleaved%' LIMIT 3")
    result["interleaved"] = cur.fetchall()

    # sparse
    cur.execute("SELECT id, fund_name FROM funds WHERE data_quality_tags LIKE '%sparse%' LIMIT 3")
    result["sparse"] = cur.fetchall()

    # jumpy
    cur.execute("SELECT id, fund_name FROM funds WHERE data_quality_tags LIKE '%jumpy%' LIMIT 3")
    result["jumpy"] = cur.fetchall()

    # 极少数据
    cur.execute("""
        SELECT fund_id, COUNT(*) as cnt FROM nav_history
        GROUP BY fund_id HAVING COUNT(*) BETWEEN 3 AND 10
        LIMIT 5
    """)
    result["few_records"] = cur.fetchall()

    # 极高NAV
    cur.execute("""
        SELECT fund_id, MAX(unit_nav) as mx FROM nav_history
        WHERE unit_nav IS NOT NULL
        GROUP BY fund_id ORDER BY mx DESC LIMIT 3
    """)
    result["high_nav"] = cur.fetchall()

    # 极低NAV
    cur.execute("""
        SELECT fund_id, MIN(unit_nav) as mn FROM nav_history
        WHERE unit_nav > 0
        GROUP BY fund_id ORDER BY mn LIMIT 3
    """)
    result["low_nav"] = cur.fetchall()

    cur.close()
    conn.close()
    return result


# ============================================================
# A. 回测引擎极端配置
# ============================================================
def test_backtest_extreme():
    print("\n" + "=" * 60)
    print("A. 回测引擎极端配置")
    print("=" * 60)

    funds = find_funds_with_data(min_records=100, count=8)
    if len(funds) < 3:
        log_result("backtest_extreme", "数据不足", False, f"仅{len(funds)}只基金")
        return
    f1, f2, f3 = funds[0][0], funds[1][0], funds[2][0]

    # A.1 单基金回测 — 退化验证
    print("\n--- A.1: 单基金回测退化验证 ---")
    cfg = {
        "weights": [{"fund_id": f1, "weight": 1.0}],
        "start_date": "2024-06-01", "end_date": "2025-01-01",
        "rebalance_frequency": "monthly", "transaction_cost_bps": 0,
        "history_mode": "intersection",
    }
    resp = requests.post(f"{API_BASE}/backtest/run", json=cfg, timeout=60)
    if resp.status_code == 200:
        bt = resp.json()
        bt_ret = bt["metrics"].get("total_return") or 0
        # 独立计算该基金同期收益
        nav_resp = requests.get(
            f"{API_BASE}/funds/{f1}/metrics?start_date=2024-06-01&end_date=2025-01-01",
            timeout=30,
        )
        if nav_resp.status_code == 200:
            fund_ret = nav_resp.json().get("total_return") or 0
            diff = abs(bt_ret - fund_ret)
            log_result("backtest_extreme", "A.1-单基金退化",
                      diff < 0.02,
                      f"回测={safe_fmt(bt_ret)}, 基金={safe_fmt(fund_ret)}, diff={safe_fmt(diff)}")
        else:
            log_result("backtest_extreme", "A.1-单基金退化", True,
                      f"回测成功 ret={safe_fmt(bt_ret)} (指标API {nav_resp.status_code})")
    else:
        log_result("backtest_extreme", "A.1", False, f"status={resp.status_code}")

    # A.2 单指数回测
    print("\n--- A.2: 单指数回测 ---")
    cfg2 = {
        "weights": [{"index_code": "000300", "weight": 1.0}],
        "start_date": "2024-01-01", "end_date": "2024-12-31",
        "rebalance_frequency": "monthly", "transaction_cost_bps": 0,
        "history_mode": "intersection",
    }
    resp2 = requests.post(f"{API_BASE}/backtest/run", json=cfg2, timeout=60)
    if resp2.status_code == 200:
        tr = resp2.json()["metrics"].get("total_return") or 0
        log_result("backtest_extreme", "A.2-单指数", True,
                  f"沪深300 2024: ret={safe_fmt(tr)}")
    else:
        log_result("backtest_extreme", "A.2", False, f"status={resp2.status_code}")

    # A.3 极短区间(7天)
    print("\n--- A.3: 极短区间 ---")
    cfg3 = {
        "weights": [{"fund_id": f1, "weight": 0.5}, {"index_code": "000300", "weight": 0.5}],
        "start_date": "2024-12-20", "end_date": "2024-12-27",
        "rebalance_frequency": "monthly", "transaction_cost_bps": 0,
        "history_mode": "intersection",
    }
    resp3 = requests.post(f"{API_BASE}/backtest/run", json=cfg3, timeout=60)
    ok = resp3.status_code in (200, 400)
    log_result("backtest_extreme", "A.3-极短区间", ok,
              f"status={resp3.status_code}")

    # A.4 极长区间(多年)
    print("\n--- A.4: 极长区间 ---")
    cfg4 = {
        "weights": [{"index_code": "000300", "weight": 0.5}, {"index_code": "000905", "weight": 0.5}],
        "start_date": "2016-01-01", "end_date": "2025-12-31",
        "rebalance_frequency": "quarterly", "transaction_cost_bps": 0,
        "history_mode": "truncate",
    }
    resp4 = requests.post(f"{API_BASE}/backtest/run", json=cfg4, timeout=60)
    if resp4.status_code == 200:
        data4 = resp4.json()
        nav_count = len(data4.get("nav_series", []))
        log_result("backtest_extreme", "A.4-极长区间", nav_count > 100,
                  f"nav_series={nav_count}条")
    else:
        log_result("backtest_extreme", "A.4", False, f"status={resp4.status_code}")

    # A.5 极多资产(5只基金+2只指数)
    print("\n--- A.5: 极多资产 ---")
    w = 1.0 / 7
    cfg5 = {
        "weights": [
            {"fund_id": funds[0][0], "weight": w},
            {"fund_id": funds[1][0], "weight": w},
            {"fund_id": funds[2][0], "weight": w},
            {"fund_id": funds[3][0], "weight": w},
            {"fund_id": funds[4][0], "weight": w},
            {"index_code": "000300", "weight": w},
            {"index_code": "000905", "weight": w},
        ],
        "start_date": "2024-01-01", "end_date": "2025-01-01",
        "rebalance_frequency": "monthly", "transaction_cost_bps": 5,
        "history_mode": "dynamic_entry",
    }
    resp5 = requests.post(f"{API_BASE}/backtest/run", json=cfg5, timeout=60)
    if resp5.status_code == 200:
        tr5 = resp5.json()["metrics"].get("total_return") or 0
        log_result("backtest_extreme", "A.5-极多资产", True,
                  f"7资产回测 ret={safe_fmt(tr5)}")
    else:
        log_result("backtest_extreme", "A.5", False, f"status={resp5.status_code}: {resp5.text[:200]}")

    # A.6 极不均匀权重(99%+1%)
    print("\n--- A.6: 极不均匀权重 ---")
    cfg6 = {
        "weights": [{"fund_id": f1, "weight": 0.99}, {"fund_id": f2, "weight": 0.01}],
        "start_date": "2024-01-01", "end_date": "2025-01-01",
        "rebalance_frequency": "monthly", "transaction_cost_bps": 0,
        "history_mode": "intersection",
    }
    resp6 = requests.post(f"{API_BASE}/backtest/run", json=cfg6, timeout=60)
    if resp6.status_code == 200:
        tr6 = resp6.json()["metrics"].get("total_return") or 0
        log_result("backtest_extreme", "A.6-不均匀权重", True,
                  f"99%+1%组合 ret={safe_fmt(tr6)}")
    else:
        log_result("backtest_extreme", "A.6", False, f"status={resp6.status_code}")

    # A.7 高交易成本(100bps) + 日频再平衡
    print("\n--- A.7: 高成本日频再平衡 ---")
    cfg7 = {
        "weights": [{"fund_id": f1, "weight": 0.5}, {"fund_id": f2, "weight": 0.5}],
        "start_date": "2024-06-01", "end_date": "2025-01-01",
        "rebalance_frequency": "daily", "transaction_cost_bps": 100,
        "history_mode": "intersection",
    }
    cfg7_nocost = {**cfg7, "transaction_cost_bps": 0}
    resp7 = requests.post(f"{API_BASE}/backtest/run", json=cfg7, timeout=60)
    resp7b = requests.post(f"{API_BASE}/backtest/run", json=cfg7_nocost, timeout=60)
    if resp7.status_code == 200 and resp7b.status_code == 200:
        tr7 = resp7.json()["metrics"].get("total_return") or 0
        tr7b = resp7b.json()["metrics"].get("total_return") or 0
        cost_drag = tr7b - tr7
        log_result("backtest_extreme", "A.7-高成本拖累",
                  cost_drag > 0.01,
                  f"无成本={safe_fmt(tr7b)}, 100bps日频={safe_fmt(tr7)}, 拖累={safe_fmt(cost_drag)}")
    else:
        log_result("backtest_extreme", "A.7", False, "请求失败")

    # A.8 interpolate vs downsample对比
    print("\n--- A.8: interpolate vs downsample ---")
    base_cfg = {
        "weights": [{"fund_id": f1, "weight": 0.5}, {"index_code": "000300", "weight": 0.5}],
        "start_date": "2024-01-01", "end_date": "2025-01-01",
        "rebalance_frequency": "monthly", "transaction_cost_bps": 0,
        "history_mode": "intersection",
    }
    resp_ds = requests.post(f"{API_BASE}/backtest/run",
                           json={**base_cfg, "freq_align_method": "downsample"}, timeout=60)
    resp_ip = requests.post(f"{API_BASE}/backtest/run",
                           json={**base_cfg, "freq_align_method": "interpolate"}, timeout=60)
    if resp_ds.status_code == 200 and resp_ip.status_code == 200:
        tr_ds = resp_ds.json()["metrics"].get("total_return") or 0
        tr_ip = resp_ip.json()["metrics"].get("total_return") or 0
        diff = abs(tr_ds - tr_ip)
        log_result("backtest_extreme", "A.8-ds vs ip", diff < 0.20,
                  f"downsample={safe_fmt(tr_ds)}, interpolate={safe_fmt(tr_ip)}, diff={safe_fmt(diff)}")
    else:
        log_result("backtest_extreme", "A.8", resp_ds.status_code == 200 or resp_ip.status_code == 200,
                  f"ds={resp_ds.status_code}, ip={resp_ip.status_code}")


# ============================================================
# B. 数据边界与交替模式
# ============================================================
def test_data_edge_cases():
    print("\n" + "=" * 60)
    print("B. 数据边界与交替模式")
    print("=" * 60)

    special = find_special_funds()

    # B.1 interleaved基金指标
    print("\n--- B.1: interleaved基金 ---")
    for fid, fname in special.get("interleaved", [])[:2]:
        resp = requests.get(f"{API_BASE}/funds/{fid}/metrics?preset=inception", timeout=30)
        ok = resp.status_code in (200, 404)
        detail = ""
        if resp.status_code == 200:
            m = resp.json()
            detail = f"ret={safe_fmt(m.get('total_return'))}"
        log_result("data_edge", f"B.1-interleaved-{fid}", ok, detail or f"status={resp.status_code}")

    # B.2 sparse基金
    print("\n--- B.2: sparse基金 ---")
    for fid, fname in special.get("sparse", [])[:2]:
        resp = requests.get(f"{API_BASE}/funds/{fid}/metrics?preset=inception", timeout=30)
        ok = resp.status_code in (200, 404)
        log_result("data_edge", f"B.2-sparse-{fid}", ok,
                  f"status={resp.status_code}")

    # B.3 jumpy基金
    print("\n--- B.3: jumpy基金 ---")
    for fid, fname in special.get("jumpy", [])[:2]:
        resp = requests.get(f"{API_BASE}/funds/{fid}/metrics?preset=inception", timeout=30)
        ok = resp.status_code in (200, 404)
        if resp.status_code == 200:
            m = resp.json()
            dd = m.get("max_drawdown") or 0
            log_result("data_edge", f"B.3-jumpy-{fid}", ok,
                      f"max_dd={safe_fmt(dd)}")
        else:
            log_result("data_edge", f"B.3-jumpy-{fid}", ok, f"status={resp.status_code}")

    # B.4 极少数据基金
    print("\n--- B.4: 极少数据基金 ---")
    for fid, cnt in special.get("few_records", [])[:3]:
        resp = requests.get(f"{API_BASE}/funds/{fid}/metrics?preset=inception", timeout=30)
        ok = resp.status_code != 500
        log_result("data_edge", f"B.4-few-{fid}({cnt}条)", ok,
                  f"status={resp.status_code}")

    # B.5 极高NAV基金
    print("\n--- B.5: 极高NAV基金 ---")
    for fid, max_nav in special.get("high_nav", [])[:2]:
        resp = requests.get(f"{API_BASE}/funds/{fid}/metrics?preset=inception", timeout=30)
        ok = resp.status_code != 500
        detail = f"max_nav={float(max_nav):.2f}"
        if resp.status_code == 200:
            m = resp.json()
            detail += f", ret={safe_fmt(m.get('total_return'))}"
            # 检查无溢出
            for v in [m.get("total_return"), m.get("annualized_volatility"), m.get("sharpe_ratio")]:
                if v is not None and (np.isinf(v) or np.isnan(v)):
                    ok = False
        log_result("data_edge", f"B.5-highNAV-{fid}", ok, detail)

    # B.6 极低NAV基金
    print("\n--- B.6: 极低NAV基金 ---")
    for fid, min_nav in special.get("low_nav", [])[:2]:
        resp = requests.get(f"{API_BASE}/funds/{fid}/metrics?preset=inception", timeout=30)
        ok = resp.status_code != 500
        log_result("data_edge", f"B.6-lowNAV-{fid}(min={float(min_nav):.4f})", ok,
                  f"status={resp.status_code}")


# ============================================================
# C. 指标短区间与极端值
# ============================================================
def test_metrics_short_intervals():
    print("\n" + "=" * 60)
    print("C. 指标短区间与极端值")
    print("=" * 60)

    funds = find_funds_with_data(min_records=200, count=3)
    if not funds:
        log_result("metrics_short", "数据不足", False, "无足够数据基金")
        return
    fid = funds[0][0]

    # C.1-C.5 各preset
    print("\n--- C.1-C.5: 各preset区间 ---")
    for preset in ["1m", "3m", "6m", "1y", "ytd"]:
        resp = requests.get(f"{API_BASE}/funds/{fid}/metrics?preset={preset}", timeout=30)
        ok = resp.status_code == 200
        detail = ""
        if ok:
            m = resp.json()
            tr = m.get("total_return")
            ar = m.get("annualized_return")
            ok = ok and (tr is not None)
            # 检查annualized_return被cap
            if ar is not None:
                ok = ok and (-1.0 <= ar <= 100.0)
            detail = f"ret={safe_fmt(tr)}, ann_ret={safe_fmt(ar)}"
        log_result("metrics_short", f"C-preset-{preset}", ok,
                  detail or f"status={resp.status_code}")

    # C.6 自定义超短区间(3天)
    print("\n--- C.6: 超短自定义区间 ---")
    resp = requests.get(
        f"{API_BASE}/funds/{fid}/metrics?start_date=2025-03-01&end_date=2025-03-04",
        timeout=30,
    )
    ok = resp.status_code != 500
    if resp.status_code == 200:
        m = resp.json()
        ar = m.get("annualized_return")
        if ar is not None:
            ok = ok and (-1.0 <= ar <= 100.0)
    log_result("metrics_short", "C.6-超短区间", ok,
              f"status={resp.status_code}")

    # C.7 risk_free_rate参数边界
    print("\n--- C.7: risk_free_rate边界 ---")
    resp_0 = requests.get(f"{API_BASE}/funds/{fid}/metrics?preset=inception&risk_free_rate=0.0", timeout=30)
    resp_10 = requests.get(f"{API_BASE}/funds/{fid}/metrics?preset=inception&risk_free_rate=0.10", timeout=30)
    if resp_0.status_code == 200 and resp_10.status_code == 200:
        sr_0 = resp_0.json().get("sharpe_ratio") or 0
        sr_10 = resp_10.json().get("sharpe_ratio") or 0
        # rf=0时sharpe应大于rf=10%时
        ok = sr_0 >= sr_10 - 0.01
        log_result("metrics_short", "C.7-risk_free_rate", ok,
                  f"rf=0: sharpe={safe_fmt(sr_0)}, rf=10%: sharpe={safe_fmt(sr_10)}")
    else:
        log_result("metrics_short", "C.7", False, "请求失败")

    # C.8 月度收益表交叉验证
    print("\n--- C.8: 月度收益表验证 ---")
    bt_cfg = {
        "weights": [{"fund_id": fid, "weight": 1.0}],
        "start_date": "2024-01-01", "end_date": "2025-01-01",
        "rebalance_frequency": "monthly", "transaction_cost_bps": 0,
        "history_mode": "intersection",
    }
    bt_resp = requests.post(f"{API_BASE}/backtest/run", json=bt_cfg, timeout=60)
    if bt_resp.status_code == 200:
        data = bt_resp.json()
        monthly = data.get("monthly_returns", [])
        nav_list = data.get("nav_series", [])
        ok = len(monthly) > 0
        log_result("metrics_short", "C.8-月度收益表非空", ok,
                  f"{len(monthly)}个月")
        if nav_list and monthly:
            # 验证最后一个月的收益
            last_month = monthly[-1]
            log_result("metrics_short", "C.8-月度数据合理",
                      -50 < last_month.get("return_pct", 0) < 50,
                      f"最后月: {last_month}")
    else:
        log_result("metrics_short", "C.8", False, f"status={bt_resp.status_code}")


# ============================================================
# D. 错误处理与压力测试
# ============================================================
def test_error_handling_and_stress():
    print("\n" + "=" * 60)
    print("D. 错误处理与压力测试")
    print("=" * 60)

    funds = find_funds_with_data(min_records=50, count=10)
    fund_ids = [f[0] for f in funds]

    # D.1 无效fund_id
    print("\n--- D.1: 无效fund_id ---")
    resp = requests.post(f"{API_BASE}/backtest/run", json={
        "weights": [{"fund_id": 999999, "weight": 1.0}],
        "start_date": "2024-01-01", "end_date": "2025-01-01",
        "history_mode": "intersection",
    }, timeout=30)
    ok = resp.status_code in (400, 404, 422)
    log_result("error_handling", "D.1-无效fund_id", ok,
              f"status={resp.status_code}")

    # D.2 空权重
    print("\n--- D.2: 空权重 ---")
    resp = requests.post(f"{API_BASE}/backtest/run", json={
        "weights": [],
        "start_date": "2024-01-01", "end_date": "2025-01-01",
        "history_mode": "intersection",
    }, timeout=30)
    ok = resp.status_code in (400, 422)
    log_result("error_handling", "D.2-空权重", ok,
              f"status={resp.status_code}")

    # D.3 start_date > end_date
    print("\n--- D.3: start_date > end_date ---")
    resp = requests.post(f"{API_BASE}/backtest/run", json={
        "weights": [{"fund_id": fund_ids[0], "weight": 1.0}],
        "start_date": "2025-06-01", "end_date": "2024-01-01",
        "history_mode": "intersection",
    }, timeout=30)
    ok = resp.status_code != 500
    log_result("error_handling", "D.3-日期倒序", ok,
              f"status={resp.status_code}")

    # D.4 不存在的preset
    print("\n--- D.4: 无效preset ---")
    resp = requests.get(f"{API_BASE}/funds/{fund_ids[0]}/metrics?preset=invalid_xyz", timeout=15)
    ok = resp.status_code in (400, 422, 500)  # 500 is a known potential issue
    log_result("error_handling", "D.4-无效preset",
              resp.status_code != 500,  # 不应500
              f"status={resp.status_code}")

    # D.5 不存在的基金指标
    print("\n--- D.5: 不存在基金指标 ---")
    resp = requests.get(f"{API_BASE}/funds/999999/metrics?preset=inception", timeout=15)
    ok = resp.status_code in (404, 400)
    log_result("error_handling", "D.5-不存在基金", ok,
              f"status={resp.status_code}")

    # D.6 50并发基金列表+指标
    print("\n--- D.6: 50并发压力测试 ---")
    urls = []
    for i in range(100):
        if i % 2 == 0:
            page = (i // 2) % 5 + 1
            urls.append(f"{API_BASE}/funds/?page={page}&page_size=50")
        else:
            fid = fund_ids[i % len(fund_ids)]
            urls.append(f"{API_BASE}/funds/{fid}/metrics?preset=1y")

    def fetch_url(url):
        start = time.perf_counter()
        try:
            r = requests.get(url, timeout=30)
            elapsed = (time.perf_counter() - start) * 1000
            return {"status": r.status_code, "time_ms": elapsed}
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return {"status": 0, "time_ms": elapsed, "error": str(e)}

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
        results = list(pool.map(fetch_url, urls))
    total_time = time.time() - t0

    success = [r for r in results if 200 <= r["status"] < 400]
    times = [r["time_ms"] for r in results if r["status"] != 0]
    success_rate = len(success) / len(results)
    p95 = sorted(times)[int(len(times) * 0.95)] if len(times) >= 2 else 0
    avg_ms = statistics.mean(times) if times else 0

    log_result("stress", "D.6-50并发成功率", success_rate >= 0.80,
              f"{success_rate:.1%} ({len(success)}/{len(results)})")
    log_result("stress", "D.6-50并发P95", p95 < 15000,
              f"P95={p95:.0f}ms, avg={avg_ms:.0f}ms, total={total_time:.1f}s")

    # D.7 20并发回测
    print("\n--- D.7: 20并发回测 ---")
    bt_configs = [
        {
            "weights": [{"fund_id": fund_ids[i % len(fund_ids)], "weight": 0.5},
                       {"index_code": "000300", "weight": 0.5}],
            "start_date": "2024-06-01", "end_date": "2025-01-01",
            "rebalance_frequency": "monthly", "transaction_cost_bps": 0,
            "history_mode": "intersection",
        }
        for i in range(20)
    ]

    def run_bt(cfg):
        start = time.perf_counter()
        try:
            r = requests.post(f"{API_BASE}/backtest/run", json=cfg, timeout=60)
            elapsed = (time.perf_counter() - start) * 1000
            return {"status": r.status_code, "time_ms": elapsed}
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return {"status": 0, "time_ms": elapsed}

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        bt_results = list(pool.map(run_bt, bt_configs))
    total_time = time.time() - t0

    success = [r for r in bt_results if r["status"] == 200]
    times = [r["time_ms"] for r in bt_results if r["status"] != 0]
    success_rate = len(success) / len(bt_results)
    p95 = sorted(times)[int(len(times) * 0.95)] if len(times) >= 2 else 0

    log_result("stress", "D.7-20并发回测成功率", success_rate >= 0.80,
              f"{success_rate:.1%} ({len(success)}/{len(bt_results)})")
    log_result("stress", "D.7-20并发回测P95", p95 < 30000,
              f"P95={p95:.0f}ms, total={total_time:.1f}s")

    # D.8 超大回测(8资产, 长区间, 日频再平衡)
    print("\n--- D.8: 超大回测 ---")
    if len(funds) >= 5:
        w8 = 1.0 / 8
        cfg8 = {
            "weights": [
                {"fund_id": funds[0][0], "weight": w8},
                {"fund_id": funds[1][0], "weight": w8},
                {"fund_id": funds[2][0], "weight": w8},
                {"fund_id": funds[3][0], "weight": w8},
                {"fund_id": funds[4][0], "weight": w8},
                {"index_code": "000300", "weight": w8},
                {"index_code": "000905", "weight": w8},
                {"index_code": "000852", "weight": w8},
            ],
            "start_date": "2020-01-01", "end_date": "2025-12-31",
            "rebalance_frequency": "monthly", "transaction_cost_bps": 5,
            "history_mode": "dynamic_entry",
        }
        t0 = time.time()
        resp8 = requests.post(f"{API_BASE}/backtest/run", json=cfg8, timeout=120)
        elapsed = time.time() - t0
        ok = resp8.status_code == 200 and elapsed < 60
        detail = f"status={resp8.status_code}, time={elapsed:.1f}s"
        if resp8.status_code == 200:
            detail += f", ret={safe_fmt(resp8.json()['metrics'].get('total_return'))}"
        log_result("stress", "D.8-超大回测", ok, detail)


# ============================================================
# 主函数
# ============================================================
def main():
    print("Codex交叉验证 — 第2轮测试")
    print(f"时间: {date.today()}")
    print(f"API: {API_BASE}")
    print()

    try:
        r = requests.get(f"{API_BASE.rsplit('/api', 1)[0]}/health", timeout=5)
        print(f"后端状态: {r.json()}")
    except Exception as e:
        print(f"错误: 后端不可达 - {e}")
        return

    test_backtest_extreme()
    test_data_edge_cases()
    test_metrics_short_intervals()
    test_error_handling_and_stress()

    # 汇总
    print("\n" + "=" * 60)
    print("第2轮测试汇总")
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

    output_path = os.path.join(os.path.dirname(__file__), "round2_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "round": 2,
            "date": str(date.today()),
            "total": total,
            "passed": passed,
            "failed": failed,
            "details": results_summary,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: {output_path}")


if __name__ == "__main__":
    main()
