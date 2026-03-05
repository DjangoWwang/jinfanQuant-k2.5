"""冒烟测试 — 验证运行中的后端服务关键端点可达。

用法:
    python tests/smoke_test.py              # 默认 http://localhost:8000
    python tests/smoke_test.py http://xx:8000  # 指定地址
"""

import sys
import urllib.request
import json

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"

ENDPOINTS = [
    ("/health", "GET", 200, "健康检查"),
    ("/api/v1/funds/strategy-categories", "GET", 200, "策略分类"),
    ("/api/v1/funds/strategy-tree", "GET", 200, "策略树"),
    ("/api/v1/funds/?page=1&page_size=1", "GET", 200, "基金列表"),
]

passed = 0
failed = 0

for path, method, expected_status, desc in ENDPOINTS:
    url = f"{BASE}{path}"
    try:
        req = urllib.request.Request(url, method=method)
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
            body = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        status = e.code
        body = None
    except Exception as e:
        print(f"  FAIL  {desc} ({path}) -- 连接失败: {e}")
        failed += 1
        continue

    if status == expected_status:
        print(f"  OK    {desc} ({path}) -> {status}")
        passed += 1
    else:
        print(f"  FAIL  {desc} ({path}) -> 期望 {expected_status}, 实际 {status}")
        failed += 1

print(f"\n结果: {passed} 通过, {failed} 失败 / 共 {len(ENDPOINTS)} 项")
sys.exit(1 if failed else 0)
