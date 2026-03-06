"""PDF report builder using ReportLab.

Assembles pre-rendered chart images and computed data into
professional multi-page PDF reports for FOF products.
"""

from __future__ import annotations

import os
from io import BytesIO
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, KeepTogether, HRFlowable,
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ---------------------------------------------------------------------------
# Colors matching 晋帆投研 theme
# ---------------------------------------------------------------------------
C_PRIMARY = colors.HexColor("#1e3a5f")
C_SECONDARY = colors.HexColor("#4f46e5")
C_GOLD = colors.HexColor("#d4a017")
C_LIGHT_BG = colors.HexColor("#f8fafc")
C_GRID = colors.HexColor("#e5e7eb")
C_TEXT = colors.HexColor("#374151")
C_POSITIVE = colors.HexColor("#16a34a")
C_NEGATIVE = colors.HexColor("#dc2626")

# ---------------------------------------------------------------------------
# Font registration
# ---------------------------------------------------------------------------
_FONT_REGISTERED = False
_CN_FONT = "Helvetica"  # fallback


def _ensure_font():
    global _FONT_REGISTERED, _CN_FONT
    if _FONT_REGISTERED:
        return
    _FONT_REGISTERED = True
    candidates = [
        ("SimHei", "C:/Windows/Fonts/simhei.ttf"),
        ("MicrosoftYaHei", "C:/Windows/Fonts/msyh.ttc"),
    ]
    for name, path in candidates:
        if os.path.exists(path):
            try:
                pdfmetrics.registerFont(TTFont(name, path))
                _CN_FONT = name
                return
            except Exception:
                continue


def _get_styles():
    """Build paragraph styles with Chinese font support."""
    _ensure_font()
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        "CNTitle",
        parent=styles["Title"],
        fontName=_CN_FONT,
        fontSize=16,
        textColor=C_PRIMARY,
        spaceAfter=6 * mm,
    ))
    styles.add(ParagraphStyle(
        "CNHeading",
        parent=styles["Heading2"],
        fontName=_CN_FONT,
        fontSize=12,
        textColor=C_PRIMARY,
        spaceAfter=3 * mm,
        spaceBefore=4 * mm,
    ))
    styles.add(ParagraphStyle(
        "CNBody",
        parent=styles["Normal"],
        fontName=_CN_FONT,
        fontSize=9,
        textColor=C_TEXT,
        leading=14,
    ))
    styles.add(ParagraphStyle(
        "CNSmall",
        parent=styles["Normal"],
        fontName=_CN_FONT,
        fontSize=7,
        textColor=colors.grey,
    ))
    return styles


# ---------------------------------------------------------------------------
# Builder class
# ---------------------------------------------------------------------------

class PDFReportBuilder:
    """Build a product performance PDF report.

    Usage:
        builder = PDFReportBuilder(product_name="xxx", period="2026年2月")
        builder.add_header(...)
        builder.add_nav_chart(chart_png_bytes)
        builder.add_metrics_cards(metrics_dict)
        pdf_bytes = builder.build()
    """

    def __init__(
        self,
        product_name: str,
        period: str,
        report_type: str = "monthly",
    ):
        self.product_name = product_name
        self.period = period
        self.report_type = report_type
        self._styles = _get_styles()
        self._story: list = []

    # --- Header ---

    def add_header(
        self,
        product_code: str | None = None,
        benchmark_name: str | None = None,
        custodian: str | None = None,
    ) -> None:
        type_label = "月度报告" if self.report_type == "monthly" else "周度报告"
        self._story.append(Paragraph(
            f"{self.product_name} — {type_label}",
            self._styles["CNTitle"],
        ))

        info_parts = [f"报告期间: {self.period}"]
        if product_code:
            info_parts.append(f"产品代码: {product_code}")
        if benchmark_name:
            info_parts.append(f"基准: {benchmark_name}")
        if custodian:
            info_parts.append(f"托管: {custodian}")
        self._story.append(Paragraph(
            " | ".join(info_parts),
            self._styles["CNSmall"],
        ))
        self._story.append(HRFlowable(
            width="100%", thickness=1, color=C_PRIMARY, spaceAfter=4 * mm,
        ))

    # --- Charts ---

    def add_chart(self, chart_png: bytes, width: float = 170 * mm, height: float = 60 * mm) -> None:
        img = Image(BytesIO(chart_png), width=width, height=height)
        self._story.append(img)
        self._story.append(Spacer(1, 3 * mm))

    def add_nav_chart(self, chart_png: bytes) -> None:
        self._story.append(Paragraph("净值走势", self._styles["CNHeading"]))
        self.add_chart(chart_png, width=170 * mm, height=55 * mm)

    def add_attribution_bar_chart(self, chart_png: bytes) -> None:
        self._story.append(Paragraph("收益归因", self._styles["CNHeading"]))
        self.add_chart(chart_png, width=170 * mm, height=60 * mm)

    def add_monthly_heatmap(self, chart_png: bytes) -> None:
        self._story.append(Paragraph("月度收益率", self._styles["CNHeading"]))
        self.add_chart(chart_png, width=140 * mm, height=50 * mm)

    def add_allocation_pie(self, chart_png: bytes) -> None:
        self._story.append(Paragraph("资产配置", self._styles["CNHeading"]))
        self.add_chart(chart_png, width=90 * mm, height=70 * mm)

    def add_weight_comparison(self, chart_png: bytes) -> None:
        self._story.append(Paragraph("资产权重变化", self._styles["CNHeading"]))
        self.add_chart(chart_png, width=155 * mm, height=55 * mm)

    # --- Metrics ---

    def add_metrics_cards(self, metrics: dict) -> None:
        """Add key metrics as a formatted table row."""
        self._story.append(Paragraph("关键指标", self._styles["CNHeading"]))

        def fmt_pct(v):
            if v is None:
                return "—"
            return f"{v * 100:+.2f}%" if isinstance(v, float) else str(v)

        def fmt_ratio(v):
            if v is None:
                return "—"
            return f"{v:.2f}" if isinstance(v, float) else str(v)

        data = [[
            Paragraph("年化收益", self._styles["CNSmall"]),
            Paragraph("最大回撤", self._styles["CNSmall"]),
            Paragraph("夏普比率", self._styles["CNSmall"]),
            Paragraph("卡玛比率", self._styles["CNSmall"]),
            Paragraph("索提诺比率", self._styles["CNSmall"]),
            Paragraph("年化波动率", self._styles["CNSmall"]),
        ], [
            Paragraph(f"<b>{fmt_pct(metrics.get('annualized_return'))}</b>", self._styles["CNBody"]),
            Paragraph(f"<b>{fmt_pct(metrics.get('max_drawdown'))}</b>", self._styles["CNBody"]),
            Paragraph(f"<b>{fmt_ratio(metrics.get('sharpe_ratio'))}</b>", self._styles["CNBody"]),
            Paragraph(f"<b>{fmt_ratio(metrics.get('calmar_ratio'))}</b>", self._styles["CNBody"]),
            Paragraph(f"<b>{fmt_ratio(metrics.get('sortino_ratio'))}</b>", self._styles["CNBody"]),
            Paragraph(f"<b>{fmt_pct(metrics.get('annualized_volatility'))}</b>", self._styles["CNBody"]),
        ]]

        col_w = 170 * mm / 6
        t = Table(data, colWidths=[col_w] * 6)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), C_LIGHT_BG),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("GRID", (0, 0), (-1, -1), 0.5, C_GRID),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        self._story.append(t)
        self._story.append(Spacer(1, 3 * mm))

    # --- Tables ---

    def add_attribution_table(self, categories: list[dict]) -> None:
        """Add Brinson attribution detail table."""
        self._story.append(Paragraph("归因分解明细", self._styles["CNHeading"]))

        def fp(v):
            return f"{v * 100:.2f}%" if isinstance(v, (int, float)) else "—"

        header = ["分类", "基准权重", "实际权重", "基准收益", "实际收益",
                  "配置效应", "选择效应", "交互效应", "合计"]
        rows = [header]
        for c in categories:
            rows.append([
                c.get("category_name", c.get("category", "")),
                fp(c.get("benchmark_weight", 0)),
                fp(c.get("actual_weight", 0)),
                fp(c.get("benchmark_return", 0)),
                fp(c.get("actual_return", 0)),
                fp(c.get("allocation_effect", 0)),
                fp(c.get("selection_effect", 0)),
                fp(c.get("interaction_effect", 0)),
                fp(c.get("total_effect", 0)),
            ])

        col_w = 170 * mm / 9
        t = Table(rows, colWidths=[col_w * 1.5] + [col_w * (7.5 / 8)] * 8)
        style = [
            ("BACKGROUND", (0, 0), (-1, 0), C_PRIMARY),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, -1), _CN_FONT),
            ("FONTSIZE", (0, 0), (-1, -1), 7),
            ("ALIGN", (1, 0), (-1, -1), "CENTER"),
            ("ALIGN", (0, 0), (0, -1), "LEFT"),
            ("GRID", (0, 0), (-1, -1), 0.5, C_GRID),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, C_LIGHT_BG]),
        ]
        t.setStyle(TableStyle(style))
        self._story.append(t)
        self._story.append(Spacer(1, 3 * mm))

    def add_interval_metrics_table(self, interval_data: dict) -> None:
        """Add long-term interval performance table."""
        self._story.append(Paragraph("多区间收益统计", self._styles["CNHeading"]))

        def fp(v):
            if v is None:
                return "—"
            return f"{v * 100:+.2f}%" if isinstance(v, (int, float)) else str(v)

        def fr(v):
            if v is None:
                return "—"
            return f"{v:.2f}" if isinstance(v, (int, float)) else str(v)

        preset_labels = {
            "1m": "近1月", "3m": "近3月", "6m": "近6月",
            "1y": "近1年", "2y": "近2年", "3y": "近3年",
            "inception": "成立以来",
        }

        header = ["指标"] + [preset_labels.get(k, k) for k in interval_data.keys()]
        rows_data = {
            "累计收益": [],
            "年化收益": [],
            "最大回撤": [],
            "夏普比率": [],
        }
        for preset, metrics in interval_data.items():
            if metrics is None:
                for k in rows_data:
                    rows_data[k].append("—")
            else:
                rows_data["累计收益"].append(fp(metrics.get("total_return")))
                rows_data["年化收益"].append(fp(metrics.get("annualized_return")))
                rows_data["最大回撤"].append(fp(metrics.get("max_drawdown")))
                rows_data["夏普比率"].append(fr(metrics.get("sharpe_ratio")))

        rows = [header]
        for label, vals in rows_data.items():
            rows.append([label] + vals)

        n_cols = len(header)
        col_w = 170 * mm / n_cols
        t = Table(rows, colWidths=[col_w * 1.3] + [col_w * ((n_cols - 1.3) / (n_cols - 1))] * (n_cols - 1))
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), C_PRIMARY),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, -1), _CN_FONT),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("ALIGN", (1, 0), (-1, -1), "CENTER"),
            ("GRID", (0, 0), (-1, -1), 0.5, C_GRID),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, C_LIGHT_BG]),
        ]))
        self._story.append(t)
        self._story.append(Spacer(1, 3 * mm))

    # --- Page break ---

    def add_page_break(self) -> None:
        self._story.append(PageBreak())

    # --- Build ---

    def _footer(self, canvas, doc):
        """Draw footer on every page."""
        canvas.saveState()
        canvas.setFont(_CN_FONT, 7)
        canvas.setFillColor(colors.grey)
        canvas.drawCentredString(
            A4[0] / 2, 12 * mm,
            f"{self.product_name} | {self.period} | 第 {doc.page} 页",
        )
        canvas.restoreState()

    def build(self) -> bytes:
        """Assemble all elements and return PDF bytes."""
        _ensure_font()
        buf = BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=A4,
            leftMargin=15 * mm,
            rightMargin=15 * mm,
            topMargin=15 * mm,
            bottomMargin=20 * mm,
        )
        doc.build(self._story, onFirstPage=self._footer, onLaterPages=self._footer)
        return buf.getvalue()
