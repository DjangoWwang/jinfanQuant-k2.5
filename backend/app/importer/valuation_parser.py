"""Valuation table Excel parser with 4-level hierarchy detection.

Custodian banks provide valuation tables as Excel files.  Each row
contains an ``item_code`` whose length encodes the hierarchy level:

    * 4 characters  -> Level 1 (asset category, e.g. "1102" = stocks)
    * 6 characters  -> Level 2 (sub-category)
    * 8 characters  -> Level 3 (individual security)
    * 14+ characters -> Level 4 (detailed lot / tranche)

This module parses such files into a structured dict ready for
database ingestion.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import openpyxl

logger = logging.getLogger(__name__)


def _detect_level(item_code: str) -> int:
    """Return the hierarchy level (1-4) based on item_code length."""
    code_len = len(item_code.strip())
    if code_len <= 4:
        return 1
    elif code_len <= 6:
        return 2
    elif code_len <= 8:
        return 3
    else:
        return 4


class ValuationParser:
    """Parse a custodian valuation table Excel file.

    The parser is designed with a strategy pattern in mind: subclass and
    override ``_locate_columns`` / ``_parse_row`` to support different
    custodian formats (e.g. CICC, China Merchants, CITIC).

    Typical usage::

        parser = ValuationParser()
        result = parser.parse("path/to/valuation.xlsx")
    """

    # Default expected column names (case-insensitive matching).
    EXPECTED_COLUMNS: dict[str, list[str]] = {
        "item_code": ["科目代码", "科目编号", "item_code", "code"],
        "item_name": ["科目名称", "item_name", "name"],
        "quantity": ["数量", "份额", "quantity", "shares"],
        "unit_cost": ["单位成本", "unit_cost"],
        "total_cost": ["成本", "成本合计", "total_cost", "cost"],
        "market_price": ["行情价格", "市价", "market_price", "price"],
        "market_value": ["市值", "市值合计", "market_value"],
        "valuation_appreciation": ["估值增值", "浮动盈亏", "appreciation"],
        "proportion": ["占净值比例", "比例(%)", "proportion", "weight"],
    }

    def parse(self, file_path: str | Path) -> dict[str, Any]:
        """Parse the valuation Excel and return structured data.

        Args:
            file_path: Path to the ``.xlsx`` file.

        Returns:
            A dict with the following shape::

                {
                    "file_name": str,
                    "valuation_date": str | None,
                    "total_nav": float | None,
                    "holdings": [
                        {
                            "item_code": str,
                            "item_name": str,
                            "level": int,          # 1-4
                            "quantity": float | None,
                            "unit_cost": float | None,
                            "total_cost": float | None,
                            "market_price": float | None,
                            "market_value": float | None,
                            "valuation_appreciation": float | None,
                            "proportion": float | None,
                        },
                        ...
                    ]
                }
        """
        file_path = Path(file_path)
        logger.info("Parsing valuation table: %s", file_path.name)

        wb = openpyxl.load_workbook(str(file_path), data_only=True)
        ws = wb.active
        if ws is None:
            raise ValueError(f"Workbook {file_path.name} has no active sheet.")

        col_map = self._locate_columns(ws)
        valuation_date = self._extract_valuation_date(ws)

        holdings: list[dict[str, Any]] = []
        total_nav: float | None = None

        # Data rows start after the header row.
        header_row = col_map.get("_header_row", 1)
        for row in ws.iter_rows(min_row=header_row + 1, values_only=False):
            parsed = self._parse_row(row, col_map)
            if parsed is None:
                continue

            # Detect hierarchy level from item_code.
            parsed["level"] = _detect_level(parsed.get("item_code", ""))
            holdings.append(parsed)

            # Try to capture total NAV from a summary row.
            item_name = str(parsed.get("item_name", ""))
            if "资产净值" in item_name or "基金资产净值" in item_name:
                total_nav = parsed.get("market_value")

        wb.close()

        return {
            "file_name": file_path.name,
            "valuation_date": valuation_date,
            "total_nav": total_nav,
            "holdings": holdings,
        }

    # ------------------------------------------------------------------
    # Override points for custodian-specific formats
    # ------------------------------------------------------------------

    def _locate_columns(self, ws: Any) -> dict[str, int]:
        """Scan the first few rows to build a column-name -> index mapping.

        Returns:
            Dict mapping canonical field names to 0-based column indices.
            Also stores ``_header_row`` (1-based) so data iteration can
            skip the header.
        """
        col_map: dict[str, int] = {}
        for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=5, values_only=False), start=1):
            for cell in row:
                if cell.value is None:
                    continue
                cell_text = str(cell.value).strip()
                for canonical, aliases in self.EXPECTED_COLUMNS.items():
                    if canonical in col_map:
                        continue
                    if cell_text.lower() in [a.lower() for a in aliases]:
                        col_map[canonical] = cell.column - 1  # 0-based
                        col_map["_header_row"] = row_idx
            if len(col_map) > 2:
                break

        if "_header_row" not in col_map:
            col_map["_header_row"] = 1

        return col_map

    def _parse_row(
        self, row: tuple, col_map: dict[str, int]
    ) -> dict[str, Any] | None:
        """Convert a single worksheet row into a holdings dict.

        Returns None if the row should be skipped (e.g. blank or summary).
        """
        def _cell_value(field: str) -> Any:
            idx = col_map.get(field)
            if idx is None or idx >= len(row):
                return None
            return row[idx].value if hasattr(row[idx], "value") else row[idx]

        item_code = _cell_value("item_code")
        if item_code is None or str(item_code).strip() == "":
            return None

        return {
            "item_code": str(item_code).strip(),
            "item_name": str(_cell_value("item_name") or "").strip(),
            "quantity": _safe_float(_cell_value("quantity")),
            "unit_cost": _safe_float(_cell_value("unit_cost")),
            "total_cost": _safe_float(_cell_value("total_cost")),
            "market_price": _safe_float(_cell_value("market_price")),
            "market_value": _safe_float(_cell_value("market_value")),
            "valuation_appreciation": _safe_float(
                _cell_value("valuation_appreciation")
            ),
            "proportion": _safe_float(_cell_value("proportion")),
        }

    def _extract_valuation_date(self, ws: Any) -> str | None:
        """Try to extract the valuation date from the sheet title area.

        Subclasses can override this for custodian-specific placement.
        """
        # TODO: Implement heuristic date extraction from the first few rows.
        #   Common patterns: "估值日期：2025-06-30", cell with a date object, etc.
        return None


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _safe_float(value: Any) -> float | None:
    """Convert a cell value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
