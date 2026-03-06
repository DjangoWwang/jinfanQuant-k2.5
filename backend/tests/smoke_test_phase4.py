#!/usr/bin/env python3
"""Phase 4 冒烟测试 — 产品运营+估值表导入。

Usage: python tests/smoke_test_phase4.py
Requires backend running at localhost:8000.
"""

import json
import os
import sys
import time

import requests

BASE = "http://localhost:8000/api/v1"
PASS = 0
FAIL = 0

SAMPLE_DIR = "D:/AI/Claude code/FOF平台开发"
SAMPLE_0213 = f"{SAMPLE_DIR}/估值表日报-GF1077-博孚利鹭岛晋帆私募证券投资基金-4-20260213.xlsx"
SAMPLE_0227 = f"{SAMPLE_DIR}/估值表日报-GF1077-博孚利鹭岛晋帆私募证券投资基金-4-20260227.xlsx"


def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} — {detail}")


def main():
    global PASS, FAIL

    print("=" * 60)
    print("Phase 4 冒烟测试 — 产品运营+估值表导入")
    print("=" * 60)

    # 1. Health check
    print("\n--- Health ---")
    r = requests.get(f"{BASE.replace('/api/v1', '')}/health")
    check("health endpoint", r.status_code == 200)

    # 2. Product CRUD
    print("\n--- Product CRUD ---")
    r = requests.post(f"{BASE}/products/", json={
        "product_name": "冒烟测试产品",
        "product_code": "SMOKE01",
        "product_type": "live",
        "custodian": "测试托管",
        "inception_date": "2024-01-01",
    })
    check("create product", r.status_code == 201, f"status={r.status_code}")
    pid = r.json().get("id")

    r = requests.get(f"{BASE}/products/{pid}")
    check("get product", r.status_code == 200 and r.json()["product_name"] == "冒烟测试产品")

    r = requests.patch(f"{BASE}/products/{pid}", json={"notes": "测试备注"})
    check("update product", r.status_code == 200 and r.json()["notes"] == "测试备注")

    r = requests.get(f"{BASE}/products/")
    check("list products", r.status_code == 200 and r.json()["total"] >= 1)

    r = requests.get(f"{BASE}/products/?product_type=live")
    check("filter by type", r.status_code == 200)

    # 3. Valuation upload
    print("\n--- Valuation Upload ---")
    has_samples = os.path.exists(SAMPLE_0213) and os.path.exists(SAMPLE_0227)

    if has_samples:
        with open(SAMPLE_0213, "rb") as f:
            r = requests.post(f"{BASE}/products/{pid}/valuation", files={"file": f})
        check("upload 0213", r.status_code == 200, f"status={r.status_code}")
        d = r.json()
        check("parse date 0213", d.get("valuation_date") == "2026-02-13")
        check("parse unit_nav 0213", d.get("unit_nav") == 1.0923)
        check("parse total_nav 0213", d.get("total_nav") is not None and d["total_nav"] > 1e6,
              f"total_nav={d.get('total_nav')}")
        check("parse holdings 0213", d.get("holdings_count", 0) > 50)
        check("sub_funds 0213", d.get("sub_funds_count") == 12)

        with open(SAMPLE_0227, "rb") as f:
            r = requests.post(f"{BASE}/products/{pid}/valuation", files={"file": f})
        check("upload 0227", r.status_code == 200)
        d = r.json()
        check("parse date 0227", d.get("valuation_date") == "2026-02-27")
        check("parse unit_nav 0227", d.get("unit_nav") == 1.105)
        check("sub_funds_linked 0227", d.get("sub_funds_linked", 0) >= 1)
    else:
        print("  [SKIP] Sample Excel files not found")

    # 4. NAV series
    print("\n--- NAV Series ---")
    r = requests.get(f"{BASE}/products/{pid}/nav")
    check("nav endpoint", r.status_code == 200)
    if has_samples:
        nav = r.json().get("nav_series", [])
        check("nav points count", len(nav) == 2, f"got {len(nav)}")
        check("nav sorted", nav[0]["date"] < nav[1]["date"] if len(nav) == 2 else False)

    # 5. Valuations list
    print("\n--- Valuations ---")
    r = requests.get(f"{BASE}/products/{pid}/valuations")
    check("valuations list", r.status_code == 200)
    if has_samples:
        check("valuations count", r.json()["total"] == 2)

    # 6. Latest valuation detail
    if has_samples:
        r = requests.get(f"{BASE}/products/{pid}/valuation/latest")
        check("latest valuation", r.status_code == 200)
        d = r.json()
        check("latest has items", len(d.get("items", [])) > 0)
        check("latest has sub_funds", len(d.get("sub_fund_allocations", [])) == 12)

    # 7. Product detail shows latest
    print("\n--- Product Detail (enriched) ---")
    r = requests.get(f"{BASE}/products/{pid}")
    d = r.json()
    if has_samples:
        check("latest_nav populated", d.get("latest_nav") == 1.105)
        check("latest_total_nav populated", d.get("latest_total_nav") is not None)
        check("latest_valuation_date", d.get("latest_valuation_date") == "2026-02-27")
        check("snapshot_count", d.get("snapshot_count") == 2)

    # 8. Mobile dashboard
    print("\n--- Mobile Dashboard ---")
    r = requests.get(f"{BASE}/mobile/dashboard")
    check("mobile dashboard", r.status_code == 200)
    d = r.json()
    check("has live_products", len(d.get("live_products", [])) >= 1)

    # 9. Simulate product
    print("\n--- Simulation Product ---")
    r = requests.post(f"{BASE}/products/", json={
        "product_name": "模拟FOF",
        "product_type": "simulation",
    })
    check("create simulation", r.status_code == 201 and r.json()["product_type"] == "simulation")

    r = requests.get(f"{BASE}/products/?product_type=simulation")
    check("filter simulation", r.json()["total"] >= 1)

    # 10. Delete product
    print("\n--- Cleanup ---")
    r = requests.delete(f"{BASE}/products/{pid}")
    check("delete product", r.status_code == 204)
    r = requests.get(f"{BASE}/products/{pid}")
    check("deleted = 404", r.status_code == 404)

    # Summary
    print("\n" + "=" * 60)
    total = PASS + FAIL
    print(f"Phase 4 冒烟测试结果: {PASS}/{total} 通过")
    if FAIL > 0:
        print(f"  {FAIL} 个测试失败!")
    else:
        print("  全部通过!")
    print("=" * 60)

    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
