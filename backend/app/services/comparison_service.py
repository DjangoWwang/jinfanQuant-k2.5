"""Business logic for fund comparison."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from app.engine.metrics import calc_all_metrics, normalize_nav, interval_dates
from app.engine.freq_align import (
    align_frequencies,
    align_to_common_dates,
    detect_mixed_frequencies,
)
from app.engine.calendar import get_trading_days
from app.services.fund_service import fund_service

logger = logging.getLogger(__name__)


class ComparisonService:

    async def compare_funds(
        self,
        db: AsyncSession,
        fund_ids: list[int],
        start_date: date | None = None,
        end_date: date | None = None,
        preset: str | None = None,
        align_method: str = "downsample",
        risk_free_rate: float = 0.025,
    ) -> dict[str, Any]:
        """Compare multiple funds with aligned NAV series and metrics."""

        # Resolve dates from preset
        if preset and not start_date:
            start_date, end_date = interval_dates(preset)

        # Fetch NAV series for all funds
        nav_dict: dict[str, pd.Series] = {}
        fund_meta: dict[int, dict] = {}
        missing_funds: list[str] = []

        for fid in fund_ids:
            fund = await fund_service.get_fund(db, fid)
            if not fund:
                missing_funds.append(f"#{fid}(不存在)")
                continue
            series = await fund_service.get_nav_series(db, fid, start_date, end_date)
            if series.empty:
                missing_funds.append(fund.fund_name)
                continue
            key = str(fid)
            nav_dict[key] = series
            fund_meta[fid] = {
                "fund_name": fund.fund_name,
                "nav_frequency": fund.nav_frequency,
            }

        if len(nav_dict) < 2:
            period = f"({start_date} ~ {end_date})" if start_date else ""
            msg = f"所选区间{period}内有数据的基金不足2只。"
            if missing_funds:
                msg += f" 以下基金在此区间无净值数据: {', '.join(missing_funds)}"
            return {
                "error": msg,
                "available_funds": len(nav_dict),
            }

        # Detect frequencies
        actual_start = min(s.index.min() for s in nav_dict.values())
        actual_end = max(s.index.max() for s in nav_dict.values())
        sd = actual_start.date() if isinstance(actual_start, pd.Timestamp) else actual_start
        ed = actual_end.date() if isinstance(actual_end, pd.Timestamp) else actual_end

        trading_days = await get_trading_days(db, sd, ed)
        trading_days_list = list(trading_days)

        freq_map = detect_mixed_frequencies(nav_dict, trading_days_list)
        has_mixed = len(set(freq_map.values())) > 1

        # Align frequencies if mixed
        if has_mixed:
            nav_dict = align_frequencies(nav_dict, trading_days_list, method=align_method)

        # Align to common dates
        aligned = align_to_common_dates(nav_dict)

        # Build results
        series_list = []
        metrics_list = []

        for key, series in aligned.items():
            fid = int(key)
            meta = fund_meta.get(fid, {})
            fund_name = meta.get("fund_name", f"Fund {fid}")

            # Normalized NAV for comparison chart
            norm = normalize_nav(series, base=1.0)
            nav_points = [
                {"date": (d.date() if isinstance(d, pd.Timestamp) else d).isoformat(), "nav": round(float(v), 6)}
                for d, v in norm.items()
            ]

            series_list.append({
                "fund_id": fid,
                "fund_name": fund_name,
                "frequency": freq_map.get(key),
                "nav_series": nav_points,
            })

            # Metrics
            m = calc_all_metrics(series, risk_free_rate)
            metrics_list.append({
                "fund_id": fid,
                "fund_name": fund_name,
                **{k: round(v, 6) if isinstance(v, float) else v for k, v in m.items()},
            })

        # Determine actual date range after alignment
        if aligned:
            first_series = next(iter(aligned.values()))
            actual_start_date = first_series.index[0]
            actual_end_date = first_series.index[-1]
            if isinstance(actual_start_date, pd.Timestamp):
                actual_start_date = actual_start_date.date()
            if isinstance(actual_end_date, pd.Timestamp):
                actual_end_date = actual_end_date.date()
        else:
            actual_start_date = start_date
            actual_end_date = end_date

        freq_warning = None
        if has_mixed:
            freqs_desc = ", ".join(f"{fund_meta.get(int(k), {}).get('fund_name', k)}: {v}" for k, v in freq_map.items())
            if align_method == "downsample":
                freq_warning = f"检测到混合频率({freqs_desc})，已降频至周频对齐"
            else:
                freq_warning = f"检测到混合频率({freqs_desc})，已通过插值升频至日频（含估算数据）"

        return {
            "start_date": actual_start_date,
            "end_date": actual_end_date,
            "alignment_method": align_method,
            "frequency_warning": freq_warning,
            "series": series_list,
            "metrics": metrics_list,
        }


comparison_service = ComparisonService()
