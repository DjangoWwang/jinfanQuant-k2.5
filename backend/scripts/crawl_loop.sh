#!/bin/bash
# 连续抓取脚本 - 循环运行expand直到超时
cd "D:/AI/Claude code/FOF平台开发/fof-platform/backend"

TIMEOUT_SECONDS=7200  # 2小时
START_TIME=$(date +%s)
ROUND=1

echo "===== 连续抓取开始: $(date), 目标运行 ${TIMEOUT_SECONDS}秒 (2小时) ====="

while true; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - START_TIME))
    REMAINING=$((TIMEOUT_SECONDS - ELAPSED))
    
    if [ $REMAINING -le 300 ]; then
        echo "===== 剩余时间不足5分钟, 停止 ====="
        break
    fi
    
    echo ""
    echo "===== 第${ROUND}轮开始: $(date) | 已运行 ${ELAPSED}秒 | 剩余 ${REMAINING}秒 ====="
    
    # 每轮抓取2000只, 5个worker
    python scripts/crawl_concurrent.py expand --workers 5 --daily-limit 2000 --delay 1.5
    
    echo "===== 第${ROUND}轮完成: $(date) ====="
    
    # 检查数据库状态
    python -c "
import asyncio
from sqlalchemy import text
from app.database import async_session
async def s():
    async with async_session() as db:
        for name, q in [
            ('has_data', \"SELECT count(*) FROM funds WHERE nav_status='has_data'\"),
            ('total', 'SELECT count(*) FROM funds'),
            ('nav_records', 'SELECT count(*) FROM nav_history'),
        ]:
            r = await db.execute(text(q))
            print(f'{name}: {r.scalar()}')
asyncio.run(s())
" 2>/dev/null | grep -v "INFO\|Engine"
    
    ROUND=$((ROUND + 1))
    
    # 轮次间短暂休息
    echo "轮次间休息30秒..."
    sleep 30
done

echo ""
echo "===== 连续抓取结束: $(date) | 共${ROUND}轮 ====="
# 最终状态
python scripts/crawl_concurrent.py status 2>/dev/null
