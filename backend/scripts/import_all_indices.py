"""Import all indices: market indices, private fund strategy indices, commodity indices.

Sources:
1. Standard market indices via /newgoapi/index/indexList + /pyapi/fund/viewv2
2. Private fund indices (火富牛/招商) via /newgoapi/fund/index/list (batch, with prices embedded)
3. Additional indices via viewv2 using id_encode from /newgoapi/global/index/configs
"""

import asyncio
import sys
import io
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.crawler.fof99.client import Fof99Client
from app.crawler.fof99.nav_scraper import NavScraper
from app.config import settings


# Key market indices to import from indexList
TARGET_MARKET_CODES = {
    # 宽基指数
    "000001",  # 上证综合指数
    "000016",  # 上证50
    "000300",  # 沪深300
    "000905",  # 中证500
    "000852",  # 中证1000
    "000510",  # 中证综合债
    "000906",  # 中证800
    "000688",  # 科创50
    "000832",  # 中证转债
    "399006",  # 创业板指
    "399303",  # 国证2000
    "000985",  # 中证全指
    # 行业/主题
    "000821",  # 沪深300增强
    "000822",  # 中证500增强
    "000908",  # 沪深300能源
    "000909",  # 沪深300原材料
    "000910",  # 沪深300工业
    "000911",  # 沪深300可选消费
    "000912",  # 沪深300主要消费
    "000913",  # 沪深300医药卫生
    "000914",  # 沪深300金融地产
    "000915",  # 沪深300信息技术
    "000916",  # 沪深300通讯服务
    "000917",  # 沪深300公共事业
    "000918",  # 沪深300成长
}

# Private fund index codes (batch query via /newgoapi/fund/index/list)
PRIVATE_INDEX_IDS = [
    "NNGPZS", "NNQHZS", "NNTLZS", "NNQQZS",    # 火富牛4大策略
    "NNDTZS", "NNHSZS", "NNZZ500", "NNZZ1000",   # 火富牛指增+主观多头
    "NNQHZDP", "NNQHZGP", "NNQHZG", "NNQHHH",    # 火富牛期货细分
    "NNKZZZS",                                      # 火富牛可转债
    "ZSCTA", "ZSFOF", "ZSZHSM", "ZSGPDT", "ZSSCZX",  # 招商私募指数
]

# Additional indices from global/index/configs (use id_encode with viewv2)
# These are market indices not in indexList
EXTRA_INDICES_BY_GROUP = {
    "商品指数": ["T_NH", "NHCI", "NHII", "NHAI"],   # 南华商品/工业品/农产品
    "债券指数": ["CBA00221", "CBA00211", "CBA00301"],  # 中债综合/国债/企业债
}


async def upsert_benchmark(db, code, name, category):
    """Create or get benchmark."""
    existing = await db.execute(
        text("SELECT id FROM benchmarks WHERE index_code = :code"),
        {"code": code}
    )
    if existing.scalar_one_or_none():
        return False
    await db.execute(
        text("""INSERT INTO benchmarks (index_code, index_name, category, is_public)
                VALUES (:code, :name, :cat, true)"""),
        {"code": code, "name": name, "cat": category}
    )
    return True


async def insert_nav_records(db, code, records):
    """Insert NAV records, skip duplicates."""
    existing_result = await db.execute(
        text("SELECT nav_date FROM index_nav WHERE index_code = :code"),
        {"code": code}
    )
    existing_dates = set(row[0] for row in existing_result.fetchall())

    new_count = 0
    for rec in records:
        nav_date_str = rec.get("pd", rec.get("nav_date", ""))
        nav_value = rec.get("cn", rec.get("pn", rec.get("unit_nav")))
        if not nav_date_str or nav_value is None:
            continue
        try:
            nav_dt = datetime.strptime(nav_date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if nav_dt in existing_dates:
            continue
        await db.execute(
            text("INSERT INTO index_nav (index_code, nav_date, nav_value) VALUES (:code, :date, :nav)"),
            {"code": code, "date": nav_dt, "nav": float(nav_value)}
        )
        new_count += 1
    return new_count


async def main():
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    client = Fof99Client()
    await client.login()
    print(f"[OK] FOF99 login (device={client.device_id})")

    nav_scraper = NavScraper(client)

    async with async_session() as db:
        total_imported = 0

        # ============================================================
        # Part 1: Standard market indices from indexList
        # ============================================================
        print("\n" + "="*60)
        print("Part 1: Market Indices (from indexList)")
        print("="*60)

        data = await client.api_get("/newgoapi/index/indexList")
        index_list = data if isinstance(data, list) else data.get("list", [])

        for idx in index_list:
            code = idx.get("code", "")
            if code not in TARGET_MARKET_CODES:
                continue

            name = idx.get("name", "")
            fid = idx.get("id", "")
            category = idx.get("category", "A股指数")

            print(f"\n  {code}: {name}")

            created = await upsert_benchmark(db, code, name, category)
            if created:
                print(f"    Created benchmark")

            try:
                records = await nav_scraper.fetch_nav_history(fid)
            except Exception as e:
                print(f"    [FAIL] {e}")
                continue

            if records:
                # Convert NavScraper normalized format
                raw_records = [{"pd": r["nav_date"], "cn": r["unit_nav"]} for r in records]
                new_count = await insert_nav_records(db, code, raw_records)
                print(f"    [OK] total={len(records)}, new={new_count}, "
                      f"range={records[0]['nav_date']}~{records[-1]['nav_date']}")
                total_imported += 1
            else:
                print(f"    [FAIL] No data")

        # ============================================================
        # Part 2: Private fund strategy indices (batch API)
        # ============================================================
        print("\n" + "="*60)
        print("Part 2: Private Fund Strategy Indices")
        print("="*60)

        ids_str = ",".join(PRIVATE_INDEX_IDS)
        try:
            pdata = await client.api_get("/newgoapi/fund/index/list", params={
                "id": ids_str,
                "startTime": "2015-01-01",
                "endTime": "2026-12-31",
            })
        except Exception as e:
            print(f"[FAIL] Batch private index fetch: {e}")
            pdata = []

        if isinstance(pdata, list):
            for item in pdata:
                idx_id = item.get("id")
                name = item.get("fund_name", "")
                prices = item.get("prices", [])

                # Find code from our list by matching id
                code = None
                for pid in PRIVATE_INDEX_IDS:
                    # Match by checking global configs
                    pass

                # Use fund_short_name or construct code
                code = item.get("fund_short_name", name)
                # Try to find actual code from global configs
                # For now use the name as identifier

                print(f"\n  ID={idx_id}: {name} ({len(prices)} records)")

                # We need a stable code. Let's find it from the configs data.
                # Use numeric id as temp code
                actual_code = f"HFN_{idx_id}"

                # Try to match from our known list
                for pid_code in PRIVATE_INDEX_IDS:
                    # We'll fix this with the configs lookup below
                    pass

                created = await upsert_benchmark(db, actual_code, name, "私募指数")
                if created:
                    print(f"    Created benchmark: {actual_code}")

                if prices:
                    new_count = await insert_nav_records(db, actual_code, prices)
                    print(f"    [OK] new={new_count}, "
                          f"range={prices[0].get('pd', '?')}~{prices[-1].get('pd', '?')}")
                    total_imported += 1

        # Now fix codes using global configs
        print("\n  Fixing index codes from global configs...")
        configs = await client.api_get("/newgoapi/global/index/configs")
        if isinstance(configs, list):
            code_map = {}  # numeric_id -> code
            for group in configs:
                for item in group.get("list", []):
                    item_id = str(item.get("id", ""))
                    item_code = item.get("code", "")
                    if item_id and item_code:
                        code_map[item_id] = item_code

            # Update benchmark codes
            for old_code_key in list(code_map.keys()):
                old_code = f"HFN_{old_code_key}"
                new_code = code_map[old_code_key]
                try:
                    # Check if old code exists
                    exists = await db.execute(
                        text("SELECT id FROM benchmarks WHERE index_code = :code"),
                        {"code": old_code}
                    )
                    if exists.scalar_one_or_none():
                        # Check new code doesn't exist
                        new_exists = await db.execute(
                            text("SELECT id FROM benchmarks WHERE index_code = :code"),
                            {"code": new_code}
                        )
                        if not new_exists.scalar_one_or_none():
                            await db.execute(
                                text("UPDATE benchmarks SET index_code = :new WHERE index_code = :old"),
                                {"new": new_code, "old": old_code}
                            )
                            await db.execute(
                                text("UPDATE index_nav SET index_code = :new WHERE index_code = :old"),
                                {"new": new_code, "old": old_code}
                            )
                            print(f"    Renamed {old_code} -> {new_code}")
                except Exception:
                    pass

        # ============================================================
        # Part 3: Extra indices from global configs (commodity, bond)
        # ============================================================
        print("\n" + "="*60)
        print("Part 3: Commodity & Bond Indices")
        print("="*60)

        if isinstance(configs, list):
            for group in configs:
                gname = group.get("name", "")
                if gname not in EXTRA_INDICES_BY_GROUP:
                    continue
                target_codes = EXTRA_INDICES_BY_GROUP[gname]
                for item in group.get("list", []):
                    item_code = item.get("code", "")
                    if item_code not in target_codes:
                        continue

                    item_id = str(item.get("id", ""))
                    id_encode = item.get("id_encode", item.get("encode_id", ""))
                    item_name = item.get("fund_name", item.get("name", item_code))

                    print(f"\n  {item_code}: {item_name} (id={item_id}, encode={id_encode})")

                    created = await upsert_benchmark(db, item_code, item_name, gname)
                    if created:
                        print(f"    Created benchmark")

                    # Try viewv2 with id_encode
                    fid = id_encode or item_id
                    try:
                        records = await nav_scraper.fetch_nav_history(fid)
                    except Exception as e:
                        print(f"    [FAIL] viewv2: {e}")
                        continue

                    if records:
                        raw_records = [{"pd": r["nav_date"], "cn": r["unit_nav"]} for r in records]
                        new_count = await insert_nav_records(db, item_code, raw_records)
                        print(f"    [OK] total={len(records)}, new={new_count}")
                        total_imported += 1
                    else:
                        print(f"    [FAIL] No data")

        await db.commit()

        # ============================================================
        # Summary
        # ============================================================
        print(f"\n{'='*60}")
        print(f"Total imported: {total_imported}")

        result = await db.execute(text("""
            SELECT b.index_code, b.index_name, b.category,
                   count(n.id) as nav_count,
                   min(n.nav_date)::text as start_d,
                   max(n.nav_date)::text as end_d
            FROM benchmarks b
            LEFT JOIN index_nav n ON n.index_code = b.index_code
            GROUP BY b.index_code, b.index_name, b.category
            ORDER BY b.category, nav_count DESC
        """))
        rows = result.fetchall()
        print(f"\nAll benchmarks ({len(rows)}):")
        current_cat = None
        for r in rows:
            if r[2] != current_cat:
                current_cat = r[2]
                print(f"\n  --- {current_cat} ---")
            print(f"  {r[0]:15s} | {r[1]:35s} | nav={r[3]:6d} | {r[4] or '?'}~{r[5] or '?'}")

    await client.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
