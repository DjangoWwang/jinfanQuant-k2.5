"""数据质量评分脚本 — 对每只基金的NAV数据进行质量评估和打分。

评分维度 (总分100):
1. 数据量 (25分): 记录条数越多越好
2. 连续性 (25分): 无大段缺失
3. 稳定性 (25分): 无异常跳变
4. 完整性 (25分): unit_nav/cumulative_nav齐全，无NULL

标签:
- interleaved: 存在交替模式(unit_nav与cumulative_nav交替不一致)
- sparse: 数据稀疏(日频基金记录过少)
- jumpy: 存在多次>20%跳变
- short_history: 历史不足1年
- stale: 最新数据超过30天未更新
- high_quality: 综合评分>=80

用法:
    cd backend
    python scripts/check_data_quality.py           # 全部评分
    python scripts/check_data_quality.py --top 20   # 显示TOP 20
    python scripts/check_data_quality.py --fund 335 # 单只基金详情
"""

import argparse
import asyncio
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["DEBUG"] = "false"

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from app.database import async_session


async def score_all_funds() -> list[dict]:
    """对所有有NAV数据的基金计算质量评分。"""
    async with async_session() as db:
        result = await db.execute(text("""
            WITH fund_stats AS (
                SELECT
                    n.fund_id,
                    f.fund_name,
                    f.nav_frequency,
                    f.latest_nav_date,
                    count(*) as record_count,
                    min(n.nav_date) as first_date,
                    max(n.nav_date) as last_date,
                    count(*) FILTER (WHERE n.unit_nav IS NULL) as null_nav_count,
                    count(*) FILTER (WHERE n.cumulative_nav IS NULL) as null_cum_count,
                    count(*) FILTER (
                        WHERE n.unit_nav IS NOT NULL AND n.cumulative_nav IS NOT NULL
                        AND ABS(n.unit_nav - n.cumulative_nav) < 0.01
                    ) as same_count,
                    count(*) FILTER (
                        WHERE n.unit_nav IS NOT NULL AND n.cumulative_nav IS NOT NULL
                        AND ABS(n.unit_nav - n.cumulative_nav) >= 0.01
                    ) as diff_count
                FROM nav_history n
                JOIN funds f ON f.id = n.fund_id
                WHERE f.nav_status = 'has_data'
                GROUP BY n.fund_id, f.fund_name, f.nav_frequency, f.latest_nav_date
            )
            SELECT * FROM fund_stats ORDER BY fund_id
        """))
        fund_stats = result.all()

        # 获取跳变统计
        jump_result = await db.execute(text("""
            WITH nav_changes AS (
                SELECT fund_id, unit_nav,
                    LAG(unit_nav) OVER (PARTITION BY fund_id ORDER BY nav_date) as prev_nav
                FROM nav_history
                WHERE unit_nav IS NOT NULL AND unit_nav > 0
            )
            SELECT fund_id,
                count(*) FILTER (WHERE prev_nav > 0 AND ABS(unit_nav - prev_nav) / prev_nav > 0.2) as jumps_20,
                count(*) FILTER (WHERE prev_nav > 0 AND ABS(unit_nav - prev_nav) / prev_nav > 0.5) as jumps_50
            FROM nav_changes
            WHERE prev_nav IS NOT NULL
            GROUP BY fund_id
        """))
        jump_map = {r[0]: (r[1], r[2]) for r in jump_result.all()}

    today = date.today()
    scores = []

    for row in fund_stats:
        fund_id = row[0]
        fund_name = row[1]
        nav_freq = row[2]
        latest_date = row[3]
        record_count = row[4]
        first_date = row[5]
        last_date = row[6]
        null_nav = row[7]
        null_cum = row[8]
        same_cnt = row[9]
        diff_cnt = row[10]

        tags = []
        total_days = (last_date - first_date).days if first_date and last_date else 0

        # === 1. 数据量评分 (25分) ===
        if nav_freq == "daily":
            expected = total_days * 5 / 7 * 0.8  # 交易日约占70-80%
        else:
            expected = total_days / 7 * 0.9  # 周频
        if expected > 0:
            coverage = record_count / max(expected, 1)
            score_quantity = min(25, int(coverage * 25))
        else:
            score_quantity = 0

        if record_count < 50:
            score_quantity = min(score_quantity, 10)
            tags.append("sparse")

        # === 2. 连续性评分 (25分) ===
        if total_days > 365:
            score_continuity = 25
        elif total_days > 180:
            score_continuity = 20
        elif total_days > 90:
            score_continuity = 15
        elif total_days > 30:
            score_continuity = 10
        else:
            score_continuity = 5
            tags.append("short_history")

        if total_days < 365:
            tags.append("short_history")

        # 最新数据是否过旧
        if latest_date and (today - latest_date).days > 30:
            score_continuity -= 5
            tags.append("stale")

        # === 3. 稳定性评分 (25分) ===
        jumps_20, jumps_50 = jump_map.get(fund_id, (0, 0))
        if jumps_50 == 0 and jumps_20 == 0:
            score_stability = 25
        elif jumps_50 == 0 and jumps_20 <= 3:
            score_stability = 22
        elif jumps_50 <= 2:
            score_stability = 18
        elif jumps_50 <= 10:
            score_stability = 12
            tags.append("jumpy")
        else:
            score_stability = 5
            tags.append("jumpy")

        # 交替模式检测
        total_classified = same_cnt + diff_cnt
        if total_classified > 20:
            same_pct = same_cnt / total_classified
            if 0.15 < same_pct < 0.85:
                tags.append("interleaved")
                score_stability -= 5

        # === 4. 完整性评分 (25分) ===
        if record_count > 0:
            null_pct = (null_nav + null_cum) / (record_count * 2)
            score_completeness = max(0, int((1 - null_pct) * 25))
        else:
            score_completeness = 0

        # === 总分 ===
        total_score = max(0, score_quantity + score_continuity + score_stability + score_completeness)
        total_score = min(100, total_score)

        if total_score >= 80:
            tags.append("high_quality")

        # 去重标签
        tags = list(dict.fromkeys(tags))

        scores.append({
            "fund_id": fund_id,
            "fund_name": fund_name,
            "nav_frequency": nav_freq,
            "record_count": record_count,
            "first_date": str(first_date),
            "last_date": str(last_date),
            "total_days": total_days,
            "jumps_20": jumps_20,
            "jumps_50": jumps_50,
            "score_quantity": score_quantity,
            "score_continuity": score_continuity,
            "score_stability": score_stability,
            "score_completeness": score_completeness,
            "total_score": total_score,
            "tags": tags,
        })

    return scores


async def save_scores(scores: list[dict]) -> None:
    """将评分写入数据库。"""
    async with async_session() as db:
        for s in scores:
            await db.execute(text("""
                UPDATE funds
                SET data_quality_score = :score,
                    data_quality_tags = :tags
                WHERE id = :fid
            """), {
                "score": s["total_score"],
                "tags": ",".join(s["tags"]) if s["tags"] else None,
                "fid": s["fund_id"],
            })
        await db.commit()


async def main():
    parser = argparse.ArgumentParser(description="数据质量评分")
    parser.add_argument("--top", type=int, default=0, help="显示TOP N高分基金")
    parser.add_argument("--bottom", type=int, default=0, help="显示最低N分基金")
    parser.add_argument("--fund", type=int, default=0, help="查看单只基金详情")
    parser.add_argument("--save", action="store_true", help="将评分写入数据库")
    args = parser.parse_args()

    scores = await score_all_funds()

    if args.fund:
        detail = next((s for s in scores if s["fund_id"] == args.fund), None)
        if detail:
            print(f"\n{'='*60}")
            print(f"基金 {detail['fund_id']}: {detail['fund_name']}")
            print(f"{'='*60}")
            print(f"  频率: {detail['nav_frequency']}")
            print(f"  记录数: {detail['record_count']}")
            print(f"  日期范围: {detail['first_date']} ~ {detail['last_date']} ({detail['total_days']}天)")
            print(f"  跳变: >20%={detail['jumps_20']}次, >50%={detail['jumps_50']}次")
            print(f"\n  评分明细:")
            print(f"    数据量:  {detail['score_quantity']:2d}/25")
            print(f"    连续性:  {detail['score_continuity']:2d}/25")
            print(f"    稳定性:  {detail['score_stability']:2d}/25")
            print(f"    完整性:  {detail['score_completeness']:2d}/25")
            print(f"    总分:    {detail['total_score']:2d}/100")
            print(f"  标签: {', '.join(detail['tags']) or '无'}")
        else:
            print(f"基金 {args.fund} 未找到")
        return

    # 排序
    scores.sort(key=lambda x: x["total_score"], reverse=True)

    # 统计
    high_q = sum(1 for s in scores if s["total_score"] >= 80)
    medium_q = sum(1 for s in scores if 60 <= s["total_score"] < 80)
    low_q = sum(1 for s in scores if s["total_score"] < 60)
    interleaved = sum(1 for s in scores if "interleaved" in s["tags"])
    jumpy = sum(1 for s in scores if "jumpy" in s["tags"])

    print(f"\n{'='*70}")
    print(f"数据质量评估报告 — 共 {len(scores)} 只基金")
    print(f"{'='*70}")
    print(f"  高质量(>=80): {high_q} 只")
    print(f"  中等(60-79):  {medium_q} 只")
    print(f"  低质量(<60):  {low_q} 只")
    print(f"\n  交替模式: {interleaved} 只")
    print(f"  频繁跳变: {jumpy} 只")

    # 分数分布
    buckets = {"90-100": 0, "80-89": 0, "70-79": 0, "60-69": 0, "50-59": 0, "<50": 0}
    for s in scores:
        sc = s["total_score"]
        if sc >= 90: buckets["90-100"] += 1
        elif sc >= 80: buckets["80-89"] += 1
        elif sc >= 70: buckets["70-79"] += 1
        elif sc >= 60: buckets["60-69"] += 1
        elif sc >= 50: buckets["50-59"] += 1
        else: buckets["<50"] += 1

    print(f"\n  分数分布:")
    for bucket, count in buckets.items():
        bar = "#" * (count // 2)
        print(f"    {bucket:>7}: {count:4d} {bar}")

    if args.top > 0:
        print(f"\n  TOP {args.top} 高分基金:")
        print(f"  {'ID':>5} {'基金名称':20s} {'频率':5s} {'记录':>5} {'评分':>4} {'标签'}")
        for s in scores[:args.top]:
            print(f"  {s['fund_id']:5d} {s['fund_name'][:20]:20s} {s['nav_frequency'] or '?':5s} {s['record_count']:5d} {s['total_score']:4d} {','.join(s['tags'])}")

    if args.bottom > 0:
        print(f"\n  最低 {args.bottom} 分基金:")
        print(f"  {'ID':>5} {'基金名称':20s} {'频率':5s} {'记录':>5} {'评分':>4} {'标签'}")
        for s in scores[-args.bottom:]:
            print(f"  {s['fund_id']:5d} {s['fund_name'][:20]:20s} {s['nav_frequency'] or '?':5s} {s['record_count']:5d} {s['total_score']:4d} {','.join(s['tags'])}")

    if args.save:
        await save_scores(scores)
        print(f"\n评分已写入数据库 (data_quality_score + data_quality_tags)")


if __name__ == "__main__":
    asyncio.run(main())
