"""Server-side chart rendering using matplotlib.

Produces PNG images for embedding in PDF reports.
Color scheme matches the 晋帆投研 UI: deep blue/indigo primary + gold accents.
"""

from __future__ import annotations

import io
from datetime import date
from typing import Any, Sequence

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------

COLORS = {
    "primary": "#1e3a5f",
    "secondary": "#4f46e5",
    "gold": "#d4a017",
    "positive": "#16a34a",
    "negative": "#dc2626",
    "grid": "#e5e7eb",
    "text": "#374151",
    "bg": "#ffffff",
    "light_bg": "#f8fafc",
}

CATEGORY_PALETTE = [
    "#1e3a5f", "#4f46e5", "#d4a017", "#16a34a",
    "#dc2626", "#7c3aed", "#0891b2", "#ea580c",
    "#6366f1", "#14b8a6", "#f59e0b", "#ef4444",
]

# Try to use a CJK font for Chinese text
_FONT_CONFIGURED = False

def _ensure_font():
    global _FONT_CONFIGURED
    if _FONT_CONFIGURED:
        return
    _FONT_CONFIGURED = True
    import os
    # Try common CJK font paths on Windows
    candidates = [
        "C:/Windows/Fonts/simhei.ttf",
        "C:/Windows/Fonts/msyh.ttc",
        "C:/Windows/Fonts/simsun.ttc",
    ]
    for path in candidates:
        if os.path.exists(path):
            from matplotlib import font_manager
            font_manager.fontManager.addfont(path)
            name = font_manager.FontProperties(fname=path).get_name()
            plt.rcParams["font.family"] = [name, "sans-serif"]
            plt.rcParams["axes.unicode_minus"] = False
            return
    # Fallback: just disable minus sign issue
    plt.rcParams["axes.unicode_minus"] = False


def _fig_to_bytes(fig: plt.Figure) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=150, facecolor="white")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def render_nav_chart(
    product_nav: pd.Series,
    benchmark_nav: pd.Series | None = None,
    product_name: str = "产品净值",
    benchmark_name: str = "基准",
    width: float = 10,
    height: float = 3.5,
) -> bytes:
    """Render a NAV comparison line chart. Returns PNG bytes."""
    _ensure_font()
    fig, ax = plt.subplots(figsize=(width, height))

    ax.plot(product_nav.index, product_nav.values,
            color=COLORS["primary"], linewidth=1.5, label=product_name)

    if benchmark_nav is not None and not benchmark_nav.empty:
        ax.plot(benchmark_nav.index, benchmark_nav.values,
                color=COLORS["gold"], linewidth=1.2, linestyle="--", label=benchmark_name)

    ax.set_facecolor(COLORS["light_bg"])
    ax.grid(True, alpha=0.3, color=COLORS["grid"])
    ax.legend(loc="upper left", fontsize=8, framealpha=0.8)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate(rotation=30)
    ax.set_title("净值走势", fontsize=11, fontweight="bold", color=COLORS["text"])

    return _fig_to_bytes(fig)


def render_drawdown_chart(
    nav_series: pd.Series,
    width: float = 10,
    height: float = 2.5,
) -> bytes:
    """Render drawdown area chart."""
    _ensure_font()
    fig, ax = plt.subplots(figsize=(width, height))

    cummax = nav_series.cummax()
    drawdown = (nav_series - cummax) / cummax

    ax.fill_between(drawdown.index, drawdown.values, 0,
                    color=COLORS["negative"], alpha=0.3)
    ax.plot(drawdown.index, drawdown.values,
            color=COLORS["negative"], linewidth=0.8)
    ax.set_facecolor(COLORS["light_bg"])
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    ax.set_title("回撤", fontsize=10, fontweight="bold", color=COLORS["text"])

    return _fig_to_bytes(fig)


def render_attribution_bar(
    categories: list[dict],
    width: float = 10,
    height: float = 4,
) -> bytes:
    """Render grouped bar chart for Brinson attribution by category.

    categories: list of dicts with keys:
        category_name, allocation_effect, selection_effect, interaction_effect
    """
    _ensure_font()
    fig, ax = plt.subplots(figsize=(width, height))

    names = [c["category_name"] for c in categories]
    alloc = [c["allocation_effect"] * 100 for c in categories]
    selec = [c["selection_effect"] * 100 for c in categories]
    inter = [c["interaction_effect"] * 100 for c in categories]

    x = np.arange(len(names))
    bar_width = 0.25

    ax.bar(x - bar_width, alloc, bar_width, label="配置效应", color=COLORS["primary"])
    ax.bar(x, selec, bar_width, label="选择效应", color=COLORS["secondary"])
    ax.bar(x + bar_width, inter, bar_width, label="交互效应", color=COLORS["gold"])

    ax.set_xticks(x)
    ax.set_xticklabels(names, fontsize=8, rotation=30, ha="right")
    ax.set_ylabel("效应 (%)", fontsize=9)
    ax.legend(fontsize=8, loc="best")
    ax.grid(axis="y", alpha=0.3)
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.set_title("Brinson收益归因", fontsize=11, fontweight="bold", color=COLORS["text"])

    return _fig_to_bytes(fig)


def render_pie_chart(
    data: list[dict],
    title: str = "资产配置",
    width: float = 5,
    height: float = 4,
) -> bytes:
    """Render a donut pie chart for asset allocation.

    data: list of dicts with keys 'name', 'value'
    """
    _ensure_font()
    fig, ax = plt.subplots(figsize=(width, height))

    names = [d["name"] for d in data]
    values = [abs(d["value"]) for d in data]

    if not values or sum(values) == 0:
        ax.text(0.5, 0.5, "无数据", ha="center", va="center", fontsize=12)
        ax.set_title(title, fontsize=11, fontweight="bold")
        return _fig_to_bytes(fig)

    colors = CATEGORY_PALETTE[:len(data)]
    wedges, texts, autotexts = ax.pie(
        values, labels=names, colors=colors, autopct="%1.1f%%",
        startangle=90, pctdistance=0.75, textprops={"fontsize": 7},
    )
    for t in autotexts:
        t.set_fontsize(7)
    centre_circle = plt.Circle((0, 0), 0.5, fc="white")
    ax.add_artist(centre_circle)
    ax.set_title(title, fontsize=11, fontweight="bold", color=COLORS["text"])

    return _fig_to_bytes(fig)


def render_weight_comparison(
    prev_weights: dict[str, float],
    curr_weights: dict[str, float],
    category_names: dict[str, str] | None = None,
    width: float = 9,
    height: float = 3.5,
) -> bytes:
    """Render side-by-side bar chart for weight changes."""
    _ensure_font()
    fig, ax = plt.subplots(figsize=(width, height))

    if category_names is None:
        category_names = {}

    all_cats = sorted(set(prev_weights) | set(curr_weights))
    names = [category_names.get(c, c) for c in all_cats]
    prev_vals = [prev_weights.get(c, 0) * 100 for c in all_cats]
    curr_vals = [curr_weights.get(c, 0) * 100 for c in all_cats]

    x = np.arange(len(all_cats))
    w = 0.35

    ax.bar(x - w / 2, prev_vals, w, label="上期", color=COLORS["secondary"], alpha=0.6)
    ax.bar(x + w / 2, curr_vals, w, label="当期", color=COLORS["primary"])

    ax.set_xticks(x)
    ax.set_xticklabels(names, fontsize=8, rotation=30, ha="right")
    ax.set_ylabel("权重 (%)", fontsize=9)
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    ax.set_title("资产权重变化", fontsize=11, fontweight="bold", color=COLORS["text"])

    return _fig_to_bytes(fig)


def render_monthly_heatmap(
    monthly_returns: list[dict],
    width: float = 8,
    height: float = 3,
) -> bytes:
    """Render calendar-style monthly return grid.

    monthly_returns: list of dicts with keys 'year', 'month', 'return_pct'
    """
    _ensure_font()

    if not monthly_returns:
        fig, ax = plt.subplots(figsize=(width, height))
        ax.text(0.5, 0.5, "无数据", ha="center", va="center")
        return _fig_to_bytes(fig)

    df = pd.DataFrame(monthly_returns)
    pivot = df.pivot_table(index="year", columns="month", values="return_pct", aggfunc="first")
    pivot = pivot.reindex(columns=range(1, 13))

    fig, ax = plt.subplots(figsize=(width, height))

    # Custom colormap: red for negative, green for positive
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("rg", [COLORS["negative"], "#ffffff", COLORS["positive"]])

    vmax = max(abs(pivot.min().min()), abs(pivot.max().max()), 0.01)
    im = ax.imshow(pivot.values, cmap=cmap, aspect="auto", vmin=-vmax, vmax=vmax)

    ax.set_xticks(range(12))
    ax.set_xticklabels([f"{m}月" for m in range(1, 13)], fontsize=7)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([str(y) for y in pivot.index], fontsize=8)

    # Annotate cells
    for i in range(len(pivot.index)):
        for j in range(12):
            val = pivot.iloc[i, j] if j < pivot.shape[1] else np.nan
            if not np.isnan(val):
                ax.text(j, i, f"{val:.1%}", ha="center", va="center", fontsize=6,
                        color="white" if abs(val) > vmax * 0.6 else "black")

    ax.set_title("月度收益率", fontsize=10, fontweight="bold", color=COLORS["text"])
    fig.colorbar(im, ax=ax, shrink=0.7, format=mticker.PercentFormatter(1.0))

    return _fig_to_bytes(fig)
