"""Valuation routes — consolidated into products.py.

This module is kept for additional valuation-related endpoints.
All main valuation endpoints are under /products/{product_id}/valuation*.
"""

import io
import re
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel

router = APIRouter(tags=["valuation"])


class ParsedHolding(BaseModel):
    level: int
    item_code: str
    item_name: str
    quantity: Optional[float] = None
    unit_cost: Optional[float] = None
    cost_amount: Optional[float] = None
    cost_pct_nav: Optional[float] = None
    market_price: Optional[float] = None
    market_value: Optional[float] = None
    value_pct_nav: Optional[float] = None
    value_diff: Optional[float] = None


class SubFundInfo(BaseModel):
    filing_number: str
    fund_name: str
    market_value: float
    weight_pct: float
    appreciation: Optional[float] = None


class ParsedValuation(BaseModel):
    product_name: str
    valuation_date: str
    unit_nav: float
    total_nav: float
    total_shares: float
    holdings: List[ParsedHolding]
    sub_funds: List[SubFundInfo]


def parse_excel_valuation(file_content: bytes, filename: str) -> ParsedValuation:
    """
    Parse Excel valuation file (四级估值表).
    Supports common formats like 博富利鹭岛金帆FOF.
    """
    try:
        import pandas as pd
    except ImportError:
        raise HTTPException(status_code=500, detail="pandas not installed")

    try:
        # Read Excel file
        df = pd.read_excel(io.BytesIO(file_content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read Excel: {str(e)}")

    # Try to extract product name from filename or first cell
    product_name = filename.replace(".xlsx", "").replace(".xls", "")

    # Try to find valuation date in filename (common pattern: YYYYMMDD or YYYY-MM-DD)
    date_match = re.search(r'(\d{4}[-_]?\d{2}[-_]?\d{2})', filename)
    if date_match:
        date_str = date_match.group(1).replace("_", "-").replace("-", "-")
        try:
            valuation_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            valuation_date = datetime.now().strftime("%Y-%m-%d")
    else:
        valuation_date = datetime.now().strftime("%Y-%m-%d")

    holdings: List[ParsedHolding] = []
    sub_funds: List[SubFundInfo] = []
    total_nav = 0.0
    unit_nav = 1.0
    total_shares = 0.0

    # Common column name mappings
    column_mappings = {
        '科目代码': ['科目代码', 'item_code', 'code', '科目编号'],
        '科目名称': ['科目名称', 'item_name', 'name', '科目'],
        '数量': ['数量', 'quantity', 'qty', '持仓数量'],
        '单位成本': ['单位成本', 'unit_cost', 'cost_price'],
        '成本金额': ['成本金额', 'cost_amount', '成本'],
        '成本占净值%': ['成本占净值%', '成本占比', 'cost_pct_nav', 'cost_pct'],
        '市价': ['市价', 'market_price', 'price', '收盘价'],
        '市值': ['市值', 'market_value', '市值金额'],
        '市值占净值%': ['市值占净值%', '市值占比', 'value_pct_nav', 'weight', '占比'],
        '估值增值': ['估值增值', 'value_diff', 'valuation_gain', '浮动盈亏'],
    }

    # Find actual column names in the dataframe
    actual_columns = {}
    for standard_name, possible_names in column_mappings.items():
        for col in df.columns:
            col_str = str(col).strip()
            if any(p.lower() in col_str.lower() for p in possible_names):
                actual_columns[standard_name] = col
                break

    # If we found the basic columns, parse the holdings
    if '科目代码' in actual_columns and '科目名称' in actual_columns:
        code_col = actual_columns['科目代码']
        name_col = actual_columns['科目名称']

        for idx, row in df.iterrows():
            try:
                code = str(row[code_col]).strip() if pd.notna(row[code_col]) else ""
                name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""

                if not code or not name:
                    continue

                # Determine level by code pattern (e.g., 1102.01.01.01 = level 4)
                level = code.count('.') + 1 if '.' in code else 1

                # Skip summary rows
                if '合计' in name or '总' in name or code in ['资产类', '负债类']:
                    continue

                # Extract numeric values
                def get_float(col_name):
                    if col_name not in actual_columns:
                        return None
                    val = row[actual_columns[col_name]]
                    if pd.isna(val):
                        return None
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return None

                holding = ParsedHolding(
                    level=min(level, 4),  # Cap at level 4
                    item_code=code,
                    item_name=name,
                    quantity=get_float('数量'),
                    unit_cost=get_float('单位成本'),
                    cost_amount=get_float('成本金额'),
                    cost_pct_nav=get_float('成本占净值%'),
                    market_price=get_float('市价'),
                    market_value=get_float('市值'),
                    value_pct_nav=get_float('市值占净值%'),
                    value_diff=get_float('估值增值'),
                )
                holdings.append(holding)

                # Detect sub-funds by name patterns
                sub_fund_keywords = ['私募', '证券投资基金', '合伙企业', '投资']
                if any(kw in name for kw in sub_fund_keywords) and holding.market_value:
                    # Try to extract filing number (备案编号 pattern: SJxxxx or similar)
                    filing_match = re.search(r'([A-Z]{2}\d{4,})', name)
                    filing_number = filing_match.group(1) if filing_match else ""

                    if filing_number or holding.value_pct_nav:
                        sub_funds.append(SubFundInfo(
                            filing_number=filing_number or "未知",
                            fund_name=name,
                            market_value=holding.market_value or 0,
                            weight_pct=holding.value_pct_nav or 0,
                            appreciation=holding.value_diff,
                        ))

            except Exception:
                continue

    # Try to find NAV info from summary rows
    for idx, row in df.iterrows():
        try:
            row_str = ' '.join([str(v) for v in row.values if pd.notna(v)])

            # Look for NAV patterns
            nav_match = re.search(r'单位净值[:\s]*(\d+\.?\d*)', row_str)
            if nav_match:
                unit_nav = float(nav_match.group(1))

            total_nav_match = re.search(r'资产净值[:\s]*(\d+\.?\d*)', row_str)
            if total_nav_match:
                total_nav = float(total_nav_match.group(1))

            shares_match = re.search(r'实收资本|总份额[:\s]*(\d+\.?\d*)', row_str)
            if shares_match:
                total_shares = float(shares_match.group(1))

        except Exception:
            continue

    # If no NAV info found, estimate from holdings
    if total_nav == 0 and holdings:
        total_nav = sum(h.market_value or 0 for h in holdings if h.level == 1)

    if total_shares == 0 and total_nav > 0 and unit_nav > 0:
        total_shares = total_nav / unit_nav

    return ParsedValuation(
        product_name=product_name,
        valuation_date=valuation_date,
        unit_nav=unit_nav,
        total_nav=total_nav,
        total_shares=total_shares,
        holdings=holdings,
        sub_funds=sub_funds,
    )


@router.post("/valuation/parse", response_model=ParsedValuation)
async def parse_valuation_file(file: UploadFile = File(...)):
    """
    Parse a valuation Excel/PDF file and return structured data.
    Supports 四级估值表 format used by 博富利鹭岛金帆FOF.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file uploaded")

    # Validate file extension
    allowed_extensions = ['.xlsx', '.xls', '.pdf']
    if not any(file.filename.lower().endswith(ext) for ext in allowed_extensions):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type. Allowed: {', '.join(allowed_extensions)}"
        )

    content = await file.read()

    if file.filename.lower().endswith('.pdf'):
        # For PDF, return a placeholder response
        # In production, implement PDF parsing
        raise HTTPException(status_code=501, detail="PDF parsing not yet implemented")

    # Parse Excel file
    return parse_excel_valuation(content, file.filename)


@router.get("/valuation/templates")
async def get_valuation_templates():
    """
    Get list of supported valuation templates.
    """
    return {
        "templates": [
            {
                "name": "博富利鹭岛金帆FOF",
                "description": "四级科目估值表，包含完整的持仓明细",
                "columns": ["科目代码", "科目名称", "数量", "单位成本", "成本金额", "市价", "市值", "市值占比"],
            },
            {
                "name": "Generic FOF",
                "description": "通用FOF估值表格式",
                "columns": ["Code", "Name", "Quantity", "Cost", "Market Value", "Weight"],
            },
        ]
    }
