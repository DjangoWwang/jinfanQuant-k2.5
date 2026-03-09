"use client";

import { useEffect, useState, useCallback, useRef, useMemo } from "react";
import { useParams, useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import {
  ArrowLeft, Loader2, Upload, FileSpreadsheet, ChevronDown, ChevronRight,
  Calendar, Building2, DollarSign, TrendingUp, BarChart3, Eye, FileText,
  PieChart, Activity, Target,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { fetchApi } from "@/lib/api";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

/* --- Types --- */

interface Product {
  id: number;
  product_name: string;
  product_code: string | null;
  custodian: string | null;
  administrator: string | null;
  product_type: string;
  inception_date: string | null;
  total_shares: number | null;
  management_fee_rate: number;
  performance_fee_rate: number;
  high_watermark: number | null;
  notes: string | null;
  latest_nav: number | null;
  latest_total_nav: number | null;
  latest_valuation_date: string | null;
  snapshot_count: number;
}

interface NavPoint {
  date: string;
  unit_nav: number | null;
  total_nav: number | null;
}

interface HoldingItem {
  item_code: string;
  item_name: string;
  level: number;
  parent_code: string | null;
  quantity: number | null;
  unit_cost: number | null;
  cost_amount: number | null;
  cost_pct_nav: number | null;
  market_price: number | null;
  market_value: number | null;
  value_pct_nav: number | null;
  value_diff: number | null;
  linked_fund_id: number | null;
  linked_fund_name: string | null;
}

interface SubFund {
  filing_number: string;
  fund_name: string;
  market_value: number | null;
  weight_pct: number | null;
  appreciation: number | null;
  linked_fund_id: number | null;
}

interface ValuationSnapshot {
  id: number;
  product_id: number;
  valuation_date: string;
  unit_nav: number | null;
  total_nav: number | null;
  total_shares: number | null;
  source_file: string | null;
  imported_at: string | null;
  items: HoldingItem[];
  sub_fund_allocations: SubFund[];
}

interface UploadResult {
  snapshot_id: number | null;
  product_id: number;
  file_name: string;
  valuation_date: string | null;
  unit_nav: number | null;
  total_nav: number | null;
  holdings_count: number;
  sub_funds_count: number;
  sub_funds_linked: number;
  warnings: string[];
}

interface AttributionCategory {
  category: string;
  category_name: string;
  benchmark_weight: number;
  actual_weight: number;
  benchmark_return: number;
  actual_return: number;
  allocation_effect: number;
  selection_effect: number;
  interaction_effect: number;
  total_effect: number;
}

interface AttributionResult {
  product_id: number;
  period_start: string;
  period_end: string;
  granularity: string;
  periods: {
    period_start: string;
    period_end: string;
    excess_return: number;
    total_allocation: number;
    total_selection: number;
    total_interaction: number;
    categories: AttributionCategory[];
  }[];
  cumulative_excess: number;
  cumulative_allocation: number;
  cumulative_selection: number;
  cumulative_interaction: number;
  aggregated_categories: AttributionCategory[];
}

/* Strategy Attribution types */
interface StrategyFundDetail {
  fund_name: string;
  fund_id: number | null;
  strategy_type: string;
  strategy_sub: string;
  market_value: number;
  weight_pct: number;
}

interface StrategyWeight {
  weight: number;
  weight_pct_nav: number;
  market_value: number;
  fund_count: number;
  funds: StrategyFundDetail[];
}

interface SnapshotStrategyData {
  valuation_date: string;
  unit_nav: number | null;
  total_nav: number | null;
  strategy_weights: Record<string, StrategyWeight>;
}

interface ReturnContrib {
  avg_weight: number;
  strategy_return: number | null;
  contribution: number | null;
}

interface PeriodContribution {
  period_start: string;
  period_end: string;
  total_return: number;
  contributions: Record<string, ReturnContrib>;
}

interface StrategyAttribution {
  snapshots: SnapshotStrategyData[];
  strategy_types: string[];
  return_contribution: PeriodContribution[];
}

/* Factor Exposure types */
interface FactorBeta {
  beta: number | null;
  t_stat: number | null;
  se: number | null;
}

interface StaticRegression {
  alpha: number | null;
  alpha_annualized: number | null;
  alpha_t_stat: number | null;
  r_squared: number | null;
  betas: Record<string, FactorBeta>;
}

interface RollingPoint {
  date: string;
  r_squared: number | null;
  betas: Record<string, FactorBeta>;
}

interface FactorExposure {
  product_id: number;
  data_points: number;
  date_range: { start: string; end: string };
  factors: Record<string, string>;
  static: StaticRegression | null;
  rolling: RollingPoint[];
  rolling_window: number;
  error: string | null;
}

/* --- Page --- */

export default function ProductDetailPage() {
  const params = useParams();
  const router = useRouter();
  const productId = Number(params.productId);

  const [product, setProduct] = useState<Product | null>(null);
  const [navSeries, setNavSeries] = useState<NavPoint[]>([]);
  const [latestSnapshot, setLatestSnapshot] = useState<ValuationSnapshot | null>(null);
  const [allSnapshots, setAllSnapshots] = useState<ValuationSnapshot[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState<UploadResult | null>(null);
  const [expandedL1, setExpandedL1] = useState<Set<string>>(new Set());
  const [expandedL2, setExpandedL2] = useState<Set<string>>(new Set());
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Attribution state
  const [attrStart, setAttrStart] = useState("");
  const [attrEnd, setAttrEnd] = useState("");
  const [attribution, setAttribution] = useState<AttributionResult | null>(null);
  const [attrLoading, setAttrLoading] = useState(false);

  // Strategy attribution state
  const [strategyAttr, setStrategyAttr] = useState<StrategyAttribution | null>(null);
  const [strategyLoading, setStrategyLoading] = useState(false);
  const [strategyGroupBy, setStrategyGroupBy] = useState<"strategy_type" | "strategy_sub">("strategy_type");

  // Factor exposure state
  const [factorExposure, setFactorExposure] = useState<FactorExposure | null>(null);
  const [factorLoading, setFactorLoading] = useState(false);
  // Always-loaded strategy_type weights for factor comparison chart
  const [strategyTypeWeights, setStrategyTypeWeights] = useState<Record<string, number>>({});

  // NAV period filter state
  const [navPeriod, setNavPeriod] = useState("ALL");

  // Report generation state
  const [reportDialogOpen, setReportDialogOpen] = useState(false);
  const [reportType, setReportType] = useState("monthly");
  const [reportStart, setReportStart] = useState("");
  const [reportEnd, setReportEnd] = useState("");
  const [generating, setGenerating] = useState(false);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [prod, nav, valuations] = await Promise.all([
        fetchApi<Product>(`/products/${productId}`),
        fetchApi<{ nav_series: NavPoint[] }>(`/products/${productId}/nav`),
        fetchApi<{ items: ValuationSnapshot[]; total: number }>(`/products/${productId}/valuations?page_size=50`),
      ]);
      setProduct(prod);
      setNavSeries(nav.nav_series);
      setAllSnapshots(valuations.items);

      // Load latest snapshot detail
      if (valuations.items.length > 0) {
        const latest = await fetchApi<ValuationSnapshot>(
          `/products/${productId}/valuation/${valuations.items[0].id}`
        );
        setLatestSnapshot(latest);
      }

      // Load factor exposure (strategy attr loaded separately with groupBy)
      fetchApi<FactorExposure>(`/products/${productId}/factor-exposure?window=60`).then(setFactorExposure).catch(() => {});
    } catch (e) {
      console.error("Load failed:", e);
    } finally {
      setLoading(false);
    }
  }, [productId]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Auto-trigger attribution when data is loaded
  useEffect(() => {
    if (!navSeries.length || attrStart) return;
    const dates = navSeries.map(n => n.date).sort();
    if (dates.length >= 2) {
      setAttrStart(dates[0]);
      setAttrEnd(dates[dates.length - 1]);
    }
  }, [navSeries, attrStart]);

  // Load strategy attribution (reloads when groupBy changes)
  useEffect(() => {
    setStrategyLoading(true);
    fetchApi<StrategyAttribution>(`/products/${productId}/strategy-attribution?group_by=${strategyGroupBy}`)
      .then(setStrategyAttr)
      .catch(() => setStrategyAttr(null))
      .finally(() => setStrategyLoading(false));
  }, [productId, strategyGroupBy]);

  // Always load strategy_type weights (for factor comparison chart, independent of toggle)
  useEffect(() => {
    fetchApi<StrategyAttribution>(`/products/${productId}/strategy-attribution?group_by=strategy_type`)
      .then(data => {
        if (data.snapshots.length > 0) {
          const latest = data.snapshots[data.snapshots.length - 1];
          const weights: Record<string, number> = {};
          for (const [st, w] of Object.entries(latest.strategy_weights)) {
            weights[st] = w.weight;
          }
          setStrategyTypeWeights(weights);
        }
      })
      .catch(() => {});
  }, [productId]);

  // Auto-fill report dates from product data
  useEffect(() => {
    if (reportStart || !navSeries.length) return;
    const dates = navSeries.map(n => n.date).sort();
    if (dates.length >= 2) {
      const inception = product?.inception_date;
      setReportStart(inception || dates[0]);
      setReportEnd(dates[dates.length - 1]);
    }
  }, [navSeries, product, reportStart]);

  // Auto-load attribution when dates are set
  useEffect(() => {
    if (!attrStart || !attrEnd || attribution) return;
    loadAttribution();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [attrStart, attrEnd]);

  async function handleUpload(file: File) {
    setUploading(true);
    setUploadResult(null);
    try {
      const formData = new FormData();
      formData.append("file", file);
      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1"}/products/${productId}/valuation`,
        { method: "POST", body: formData }
      );
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `Upload failed: ${res.status}`);
      }
      const result: UploadResult = await res.json();
      setUploadResult(result);
      await loadData();
    } catch (e: any) {
      setUploadResult({
        snapshot_id: null,
        product_id: productId,
        file_name: file.name,
        valuation_date: null,
        unit_nav: null,
        total_nav: null,
        holdings_count: 0,
        sub_funds_count: 0,
        sub_funds_linked: 0,
        warnings: [`上传失败: ${e.message}`],
      });
    } finally {
      setUploading(false);
    }
  }

  function toggleL1(code: string) {
    setExpandedL1((prev) => {
      const next = new Set(prev);
      next.has(code) ? next.delete(code) : next.add(code);
      return next;
    });
  }

  function toggleL2(code: string) {
    setExpandedL2((prev) => {
      const next = new Set(prev);
      next.has(code) ? next.delete(code) : next.add(code);
      return next;
    });
  }

  async function loadAttribution() {
    if (!attrStart || !attrEnd) return;
    setAttrLoading(true);
    try {
      const result = await fetchApi<AttributionResult>(
        `/reports/${productId}/attribution?period_start=${attrStart}&period_end=${attrEnd}&granularity=monthly`
      );
      setAttribution(result);
    } catch (e: any) {
      console.error("Attribution failed:", e);
      setAttribution(null);
    } finally {
      setAttrLoading(false);
    }
  }

  async function handleGenerateReport() {
    setGenerating(true);
    try {
      const apiBase = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
      const res = await fetch(`${apiBase}/reports/generate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          product_id: productId,
          report_type: reportType,
          period_start: reportStart,
          period_end: reportEnd,
        }),
      });
      if (!res.ok) throw new Error("报告生成失败");
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `报告_${productId}_${reportEnd}.pdf`;
      a.click();
      URL.revokeObjectURL(url);
      setReportDialogOpen(false);
    } catch (e: any) {
      alert(e.message || "报告生成失败");
    } finally {
      setGenerating(false);
    }
  }

  if (loading || !product) {
    return (
      <div className="h-96 flex items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  // Filter NAV by period
  const filteredNav = useMemo(() => {
    if (navPeriod === "ALL") return navSeries;
    const now = new Date();
    let cutoff: Date;
    switch (navPeriod) {
      case "1M": cutoff = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate()); break;
      case "3M": cutoff = new Date(now.getFullYear(), now.getMonth() - 3, now.getDate()); break;
      case "6M": cutoff = new Date(now.getFullYear(), now.getMonth() - 6, now.getDate()); break;
      case "YTD": cutoff = new Date(now.getFullYear(), 0, 1); break;
      case "1Y": cutoff = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate()); break;
      default: return navSeries;
    }
    const cutStr = cutoff.toISOString().slice(0, 10);
    return navSeries.filter(n => n.date >= cutStr);
  }, [navSeries, navPeriod]);

  // Calculate metrics for the filtered period
  const navMetrics = useMemo(() => {
    if (filteredNav.length < 2) return null;
    const startNav = filteredNav[0].unit_nav;
    const endNav = filteredNav[filteredNav.length - 1].unit_nav;
    if (!startNav || !endNav) return null;
    
    const totalReturn = (endNav - startNav) / startNav;
    
    // Calculate max drawdown
    let maxDrawdown = 0;
    let peak = startNav;
    for (const point of filteredNav) {
      if (point.unit_nav) {
        if (point.unit_nav > peak) peak = point.unit_nav;
        const drawdown = (peak - point.unit_nav) / peak;
        if (drawdown > maxDrawdown) maxDrawdown = drawdown;
      }
    }
    
    // Calculate annualized return
    const startDate = new Date(filteredNav[0].date);
    const endDate = new Date(filteredNav[filteredNav.length - 1].date);
    const years = (endDate.getTime() - startDate.getTime()) / (365.25 * 24 * 60 * 60 * 1000);
    const annualizedReturn = years > 0 ? Math.pow(1 + totalReturn, 1 / years) - 1 : 0;
    
    return {
      totalReturn,
      maxDrawdown,
      annualizedReturn,
      startDate: filteredNav[0].date,
      endDate: filteredNav[filteredNav.length - 1].date,
    };
  }, [filteredNav]);

  /* --- NAV Chart --- */
  const navChartOption = {
    tooltip: {
      trigger: "axis",
      formatter: (params: any) => {
        const p = params[0];
        return `${p.axisValue}<br/>单位净值: <b>${p.value?.toFixed(4) ?? "--"}</b>`;
      },
    },
    grid: { left: 50, right: 20, top: 20, bottom: 30 },
    xAxis: {
      type: "category",
      data: filteredNav.map((n) => n.date),
      axisLabel: { fontSize: 10 },
    },
    yAxis: {
      type: "value",
      scale: true,
      axisLabel: { fontSize: 10 },
    },
    series: [
      {
        name: "单位净值",
        type: "line",
        data: filteredNav.map((n) => n.unit_nav),
        smooth: true,
        symbol: "none",
        lineStyle: { width: 2 },
        areaStyle: {
          color: {
            type: "linear",
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: "rgba(59,130,246,0.15)" },
              { offset: 1, color: "rgba(59,130,246,0.01)" },
            ],
          },
        },
      },
    ],
  };

  /* --- Sub-fund Pie Chart --- */
  const subFunds = latestSnapshot?.sub_fund_allocations || [];
  const pieData = subFunds
    .filter((s) => s.weight_pct != null && s.weight_pct > 0)
    .map((s) => ({ name: s.fund_name, value: s.weight_pct }));

  const pieChartOption = {
    tooltip: {
      trigger: "item",
      formatter: (p: any) => `${p.name}<br/>占比: <b>${p.value?.toFixed(2)}%</b>`,
    },
    series: [
      {
        type: "pie",
        radius: ["35%", "65%"],
        label: { fontSize: 10, formatter: "{b}: {d}%" },
        data: pieData,
      },
    ],
  };

  /* --- Holding hierarchy --- */
  const holdings = latestSnapshot?.items || [];
  const l1Items = holdings.filter((h) => h.level === 1);

  function getChildren(parentCode: string, level: number) {
    return holdings.filter(
      (h) => h.level === level && h.parent_code === parentCode
    );
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title={product.product_name}
        breadcrumb={["产品运营"]}
        actions={
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" className="h-8 text-[12px] gap-1" onClick={() => router.push("/product-ops")}>
              <ArrowLeft className="h-3 w-3" />返回
            </Button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".xlsx,.xls"
              className="hidden"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) handleUpload(file);
                e.target.value = "";
              }}
            />
            <Button
              size="sm"
              className="h-8 text-[12px] gap-1.5"
              disabled={uploading}
              onClick={() => fileInputRef.current?.click()}
            >
              {uploading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Upload className="h-3.5 w-3.5" />}
              上传估值表
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="h-8 text-[12px] gap-1.5"
              onClick={() => setReportDialogOpen(true)}
            >
              <FileText className="h-3.5 w-3.5" />
              生成报告
            </Button>
          </div>
        }
      />

      {/* Upload result toast */}
      {uploadResult && (
        <div className={`p-3 rounded border text-[12px] ${
          uploadResult.warnings.some((w) => w.startsWith("上传失败"))
            ? "bg-red-50 border-red-200 text-red-700"
            : "bg-green-50 border-green-200 text-green-700"
        }`}>
          <p className="font-medium">{uploadResult.file_name}</p>
          {uploadResult.valuation_date && <p>估值日期: {uploadResult.valuation_date}</p>}
          {uploadResult.unit_nav != null && <p>单位净值: {uploadResult.unit_nav.toFixed(4)}</p>}
          <p>持仓明细: {uploadResult.holdings_count} 条 | 子基金: {uploadResult.sub_funds_count} 只 | 已关联: {uploadResult.sub_funds_linked} 只</p>
          {uploadResult.warnings.map((w, i) => (
            <p key={i} className="text-amber-600 mt-0.5">{w}</p>
          ))}
          <button className="text-[11px] underline mt-1" onClick={() => setUploadResult(null)}>关闭</button>
        </div>
      )}

      {/* Product info cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <InfoCard icon={DollarSign} label="最新净值" value={product.latest_nav?.toFixed(4) ?? "--"} sub={product.latest_valuation_date ?? ""} />
        <InfoCard icon={BarChart3} label="资产净值" value={fmtMoney(product.latest_total_nav)} sub="总规模" />
        <InfoCard icon={Calendar} label="成立日" value={product.inception_date ?? "--"} sub={product.product_type === "live" ? "实盘" : "模拟"} />
        <InfoCard icon={Building2} label="托管/管理" value={product.custodian ?? "--"} sub={`费率 ${product.management_fee_rate}%/${product.performance_fee_rate}%`} />
      </div>

      {/* Main tabs */}
      <Tabs defaultValue="overview">
        <TabsList className="h-8">
          <TabsTrigger value="overview" className="text-[12px] h-7 px-3">概览</TabsTrigger>
          <TabsTrigger value="strategy" className="text-[12px] h-7 px-3">策略归因</TabsTrigger>
          <TabsTrigger value="factor" className="text-[12px] h-7 px-3">因子暴露</TabsTrigger>
          <TabsTrigger value="attribution" className="text-[12px] h-7 px-3">基准归因</TabsTrigger>
          <TabsTrigger value="holdings" className="text-[12px] h-7 px-3">持仓明细</TabsTrigger>
          <TabsTrigger value="history" className="text-[12px] h-7 px-3">估值历史</TabsTrigger>
        </TabsList>

        {/* Overview tab */}
        <TabsContent value="overview" className="mt-3 space-y-4">
          {/* NAV Chart with Period Selector - Left/Right Layout */}
          <div className="bg-card border border-border rounded">
            <div className="flex items-center justify-between px-4 py-2 border-b border-border">
              <h3 className="text-[13px] font-medium">净值走势</h3>
              <div className="flex gap-0.5">
                {["1M", "3M", "6M", "YTD", "1Y", "ALL"].map((p) => (
                  <Button
                    key={p}
                    variant={p === navPeriod ? "default" : "ghost"}
                    size="sm"
                    className="h-6 px-2 text-[11px]"
                    onClick={() => setNavPeriod(p)}
                  >
                    {p}
                  </Button>
                ))}
              </div>
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-3">
              {/* Chart - Left Side */}
              <div className="lg:col-span-2 p-3 border-b lg:border-b-0 lg:border-r border-border">
                {filteredNav.length > 0 ? (
                  <ReactECharts option={navChartOption} style={{ height: 300 }} />
                ) : (
                  <div className="h-[300px] flex items-center justify-center text-muted-foreground text-[12px]">
                    暂无净值数据，请上传估值表
                  </div>
                )}
              </div>
              {/* Metrics - Right Side */}
              <div className="p-3">
                <h4 className="text-[12px] font-medium text-muted-foreground mb-3">区间统计指标</h4>
                {navMetrics ? (
                  <div className="space-y-3">
                    <div className="bg-muted/40 rounded p-3">
                      <div className="text-[11px] text-muted-foreground">区间收益</div>
                      <div className={`text-xl font-semibold tabular-nums ${navMetrics.totalReturn >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {navMetrics.totalReturn >= 0 ? '+' : ''}{(navMetrics.totalReturn * 100).toFixed(2)}%
                      </div>
                      <div className="text-[10px] text-muted-foreground mt-1">
                        {navMetrics.startDate} ~ {navMetrics.endDate}
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-2">
                      <div className="bg-muted/40 rounded p-2">
                        <div className="text-[11px] text-muted-foreground">年化收益</div>
                        <div className={`text-base font-medium tabular-nums ${navMetrics.annualizedReturn >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {navMetrics.annualizedReturn >= 0 ? '+' : ''}{(navMetrics.annualizedReturn * 100).toFixed(2)}%
                        </div>
                      </div>
                      <div className="bg-muted/40 rounded p-2">
                        <div className="text-[11px] text-muted-foreground">最大回撤</div>
                        <div className="text-base font-medium tabular-nums text-red-600">
                          -{(navMetrics.maxDrawdown * 100).toFixed(2)}%
                        </div>
                      </div>
                    </div>
                    <div className="bg-muted/40 rounded p-2">
                      <div className="text-[11px] text-muted-foreground">当前净值</div>
                      <div className="text-lg font-semibold tabular-nums">
                        {filteredNav[filteredNav.length - 1]?.unit_nav?.toFixed(4) ?? '--'}
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="h-[200px] flex items-center justify-center text-muted-foreground text-[12px]">
                    选择时间区间查看统计指标
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Sub-fund allocation pie */}
          <div className="bg-card border border-border rounded p-3">
            <h3 className="text-[13px] font-medium mb-2">子基金配置</h3>
            {pieData.length > 0 ? (
              <ReactECharts option={pieChartOption} style={{ height: 280 }} />
            ) : (
              <div className="h-[280px] flex items-center justify-center text-muted-foreground text-[12px]">
                暂无持仓数据
              </div>
            )}
          </div>

          {/* Sub-fund table */}
          {subFunds.length > 0 && (
            <div className="bg-card border border-border rounded overflow-hidden">
              <h3 className="text-[13px] font-medium p-3 pb-2">子基金持仓</h3>
              <Table>
                <TableHeader>
                  <TableRow className="bg-muted/40">
                    <TableHead className="h-8 text-[11px] font-normal">基金名称</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">备案号</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">市值</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">占比</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">浮盈</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">关联</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {subFunds.map((sf, i) => (
                    <TableRow key={i} className="text-[12px]">
                      <TableCell className="py-2 font-medium">{sf.fund_name}</TableCell>
                      <TableCell className="py-2 text-center text-muted-foreground text-[11px]">{sf.filing_number}</TableCell>
                      <TableCell className="py-2 text-right tabular-nums">{fmtMoney(sf.market_value)}</TableCell>
                      <TableCell className="py-2 text-right tabular-nums">{fmtPct(sf.weight_pct)}</TableCell>
                      <TableCell className={`py-2 text-right tabular-nums ${
                        (sf.appreciation ?? 0) >= 0 ? "text-green-600" : "text-red-600"
                      }`}>
                        {fmtMoney(sf.appreciation)}
                      </TableCell>
                      <TableCell className="py-2 text-center">
                        {sf.linked_fund_id ? (
                          <button
                            className="text-[11px] text-primary hover:underline"
                            onClick={() => router.push(`/fund-database/${sf.linked_fund_id}`)}
                          >
                            <Eye className="h-3 w-3 inline mr-0.5" />查看
                          </button>
                        ) : (
                          <span className="text-[11px] text-muted-foreground">未关联</span>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </TabsContent>

        {/* Strategy Attribution tab */}
        <TabsContent value="strategy" className="mt-3 space-y-4">
          {/* Group by toggle */}
          <div className="flex items-center gap-2">
            <span className="text-[11px] text-muted-foreground">分组维度:</span>
            <button
              className={`px-2.5 py-1 rounded text-[11px] transition-colors ${strategyGroupBy === "strategy_type" ? "bg-primary text-primary-foreground" : "bg-muted text-muted-foreground hover:bg-muted/80"}`}
              onClick={() => setStrategyGroupBy("strategy_type")}
            >一级标签</button>
            <button
              className={`px-2.5 py-1 rounded text-[11px] transition-colors ${strategyGroupBy === "strategy_sub" ? "bg-primary text-primary-foreground" : "bg-muted text-muted-foreground hover:bg-muted/80"}`}
              onClick={() => setStrategyGroupBy("strategy_sub")}
            >二级标签</button>
            {strategyLoading && <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />}
          </div>

          {strategyAttr && strategyAttr.snapshots.length > 0 ? (
            <>
              {/* Strategy weight stacked bar chart */}
              <div className="bg-card border border-border rounded p-3">
                <h3 className="text-[13px] font-medium mb-2 flex items-center gap-1.5">
                  <PieChart className="h-3.5 w-3.5 text-indigo-500 opacity-70" />
                  历史策略权重变化
                </h3>
                <ReactECharts
                  option={{
                    tooltip: {
                      trigger: "axis",
                      axisPointer: { type: "shadow" },
                      formatter: (params: any) => {
                        if (!Array.isArray(params) || !params.length) return "";
                        let tip = `<b>${params[0].axisValue}</b>`;
                        for (const p of params) {
                          if (p.value > 0) tip += `<br/>${p.seriesName}: ${(p.value * 100).toFixed(1)}%`;
                        }
                        return tip;
                      },
                    },
                    legend: {
                      data: strategyAttr.strategy_types,
                      textStyle: { fontSize: 10 },
                      bottom: 0,
                    },
                    grid: { left: 50, right: 20, top: 15, bottom: 40 },
                    xAxis: {
                      type: "category",
                      data: strategyAttr.snapshots.map(s => s.valuation_date),
                      axisLabel: { fontSize: 10 },
                    },
                    yAxis: {
                      type: "value",
                      max: 1,
                      axisLabel: { fontSize: 10, formatter: (v: number) => `${(v * 100).toFixed(0)}%` },
                    },
                    series: strategyAttr.strategy_types.map(st => ({
                      name: st,
                      type: "bar",
                      stack: "total",
                      emphasis: { focus: "series" },
                      data: strategyAttr.snapshots.map(snap => {
                        const w = snap.strategy_weights[st];
                        return w ? w.weight : 0;
                      }),
                    })),
                    color: ["#2563eb", "#7c3aed", "#db2777", "#ea580c", "#16a34a", "#0891b2", "#64748b", "#d4a017"],
                  }}
                  style={{ height: 320 }}
                  notMerge
                />
              </div>

              {/* Latest strategy allocation pie + detail */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                {/* Pie chart */}
                <div className="bg-card border border-border rounded p-3">
                  <h3 className="text-[13px] font-medium mb-2">最新策略配置</h3>
                  {(() => {
                    const latest = strategyAttr.snapshots[strategyAttr.snapshots.length - 1];
                    const pieData = Object.entries(latest.strategy_weights)
                      .map(([st, w]) => ({ name: st, value: parseFloat((w.weight * 100).toFixed(1)) }))
                      .sort((a, b) => b.value - a.value);
                    return (
                      <ReactECharts
                        option={{
                          tooltip: { trigger: "item", formatter: "{b}: {c}% ({d}%)" },
                          series: [{
                            type: "pie",
                            radius: ["30%", "65%"],
                            label: { fontSize: 11, formatter: "{b}\n{d}%" },
                            data: pieData,
                          }],
                          color: ["#2563eb", "#7c3aed", "#db2777", "#ea580c", "#16a34a", "#0891b2", "#64748b", "#d4a017"],
                        }}
                        style={{ height: 280 }}
                        notMerge
                      />
                    );
                  })()}
                </div>

                {/* Strategy detail table */}
                <div className="bg-card border border-border rounded overflow-hidden">
                  <h3 className="text-[13px] font-medium p-3 pb-2">策略明细 ({strategyAttr.snapshots[strategyAttr.snapshots.length - 1].valuation_date})</h3>
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-muted/40">
                        <TableHead className="h-8 text-[11px] font-normal">策略</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">权重</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">市值</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-center">基金数</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {(() => {
                        const latest = strategyAttr.snapshots[strategyAttr.snapshots.length - 1];
                        return Object.entries(latest.strategy_weights)
                          .sort(([, a], [, b]) => b.weight - a.weight)
                          .map(([st, w]) => (
                            <TableRow key={st} className="text-[12px]">
                              <TableCell className="py-2 font-medium">{st}</TableCell>
                              <TableCell className="py-2 text-right tabular-nums">{(w.weight * 100).toFixed(1)}%</TableCell>
                              <TableCell className="py-2 text-right tabular-nums">{fmtMoney(w.market_value)}</TableCell>
                              <TableCell className="py-2 text-center">{w.fund_count}</TableCell>
                            </TableRow>
                          ));
                      })()}
                    </TableBody>
                  </Table>
                </div>
              </div>

              {/* Return contribution */}
              {strategyAttr.return_contribution.length > 0 && (
                <div className="bg-card border border-border rounded p-3">
                  <h3 className="text-[13px] font-medium mb-2 flex items-center gap-1.5">
                    <Activity className="h-3.5 w-3.5 text-green-600 opacity-70" />
                    收益贡献分解
                  </h3>
                  {strategyAttr.return_contribution.map((rc, idx) => {
                    const contribs = Object.entries(rc.contributions)
                      .filter(([, c]) => c.contribution != null)
                      .sort(([, a], [, b]) => (b.contribution ?? 0) - (a.contribution ?? 0));
                    return (
                      <div key={idx} className="mb-3">
                        <p className="text-[11px] text-muted-foreground mb-2">
                          {rc.period_start} ~ {rc.period_end} | 总收益: <span className={`font-medium ${rc.total_return >= 0 ? "text-green-600" : "text-red-600"}`}>{(rc.total_return * 100).toFixed(2)}%</span>
                        </p>
                        <ReactECharts
                          option={{
                            tooltip: {
                              trigger: "axis",
                              formatter: (params: any) => {
                                if (!Array.isArray(params)) return "";
                                let tip = params[0]?.name || "";
                                for (const p of params) {
                                  tip += `<br/>${p.seriesName}: ${p.value != null ? (p.value * 100).toFixed(3) + "%" : "N/A"}`;
                                }
                                return tip;
                              },
                            },
                            grid: { left: 80, right: 30, top: 15, bottom: 25 },
                            xAxis: {
                              type: "category",
                              data: contribs.map(([st]) => st),
                              axisLabel: { fontSize: 10 },
                            },
                            yAxis: {
                              type: "value",
                              axisLabel: { fontSize: 10, formatter: (v: number) => `${(v * 100).toFixed(2)}%` },
                            },
                            series: [
                              {
                                name: "策略收益",
                                type: "bar",
                                data: contribs.map(([, c]) => c.strategy_return),
                                itemStyle: { color: "#94a3b8" },
                                barGap: "0%",
                              },
                              {
                                name: "收益贡献",
                                type: "bar",
                                data: contribs.map(([, c]) => c.contribution),
                                itemStyle: {
                                  color: (params: any) => (params.value ?? 0) >= 0 ? "#16a34a" : "#dc2626",
                                },
                              },
                            ],
                            legend: { data: ["策略收益", "收益贡献"], textStyle: { fontSize: 10 }, bottom: 0 },
                          }}
                          style={{ height: 240 }}
                          notMerge
                        />
                      </div>
                    );
                  })}
                </div>
              )}
            </>
          ) : (
            <div className="bg-card border border-border rounded p-8 text-center text-muted-foreground text-[12px]">
              暂无策略归因数据。请先上传估值表以获取子基金持仓信息。
            </div>
          )}
        </TabsContent>

        {/* Factor Exposure tab */}
        <TabsContent value="factor" className="mt-3 space-y-4">
          {factorExposure && !factorExposure.error && factorExposure.static ? (
            <>
              {/* Static regression summary */}
              <div className="bg-card border border-border rounded p-3">
                <h3 className="text-[13px] font-medium mb-3 flex items-center gap-1.5">
                  <Target className="h-3.5 w-3.5 text-purple-600 opacity-70" />
                  因子暴露分析（全区间回归）
                </h3>
                <p className="text-[11px] text-muted-foreground mb-3">
                  数据区间: {factorExposure.date_range.start?.slice(0, 10)} ~ {factorExposure.date_range.end?.slice(0, 10)} |
                  数据点: {factorExposure.data_points} |
                  R² = {factorExposure.static.r_squared?.toFixed(4)} |
                  Alpha(年化) = {factorExposure.static.alpha_annualized != null ? `${(factorExposure.static.alpha_annualized * 100).toFixed(2)}%` : "N/A"}
                </p>

                {/* Factor beta bar chart */}
                {(() => {
                  const betas = factorExposure.static!.betas;
                  const entries = Object.entries(betas).sort(([, a], [, b]) => Math.abs(b.beta ?? 0) - Math.abs(a.beta ?? 0));
                  return (
                    <ReactECharts
                      option={{
                        tooltip: {
                          trigger: "axis",
                          formatter: (params: any) => {
                            const p = Array.isArray(params) ? params[0] : params;
                            const name = p?.name || "";
                            const entry = betas[name];
                            if (!entry) return name;
                            const sig = Math.abs(entry.t_stat ?? 0) > 2.58 ? "***" : Math.abs(entry.t_stat ?? 0) > 1.96 ? "**" : Math.abs(entry.t_stat ?? 0) > 1.65 ? "*" : "";
                            return `<b>${name}</b><br/>Beta: ${entry.beta?.toFixed(4)}<br/>t值: ${entry.t_stat?.toFixed(2)} ${sig}<br/>指数: ${factorExposure.factors[name] || ""}`;
                          },
                        },
                        grid: { left: 80, right: 30, top: 15, bottom: 25 },
                        xAxis: {
                          type: "category",
                          data: entries.map(([name]) => name),
                          axisLabel: { fontSize: 10 },
                        },
                        yAxis: {
                          type: "value",
                          axisLabel: { fontSize: 10 },
                          splitLine: { lineStyle: { color: "#f3f4f6" } },
                        },
                        series: [{
                          type: "bar",
                          data: entries.map(([name, b]) => ({
                            value: b.beta,
                            itemStyle: {
                              color: Math.abs(b.t_stat ?? 0) >= 1.96 ? "#2563eb" : "#cbd5e1",
                            },
                          })),
                          label: {
                            show: true,
                            position: "top",
                            fontSize: 10,
                            formatter: (p: any) => p.value?.toFixed(3),
                          },
                        }],
                      }}
                      style={{ height: 260 }}
                      notMerge
                    />
                  );
                })()}

                {/* Factor detail table */}
                <Table>
                  <TableHeader>
                    <TableRow className="bg-muted/40">
                      <TableHead className="h-8 text-[11px] font-normal">因子</TableHead>
                      <TableHead className="h-8 text-[11px] font-normal">代理指数</TableHead>
                      <TableHead className="h-8 text-[11px] font-normal text-right">Beta</TableHead>
                      <TableHead className="h-8 text-[11px] font-normal text-right">t值</TableHead>
                      <TableHead className="h-8 text-[11px] font-normal text-center">显著性</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {Object.entries(factorExposure.static!.betas)
                      .sort(([, a], [, b]) => Math.abs(b.beta ?? 0) - Math.abs(a.beta ?? 0))
                      .map(([name, b]) => {
                        const absT = Math.abs(b.t_stat ?? 0);
                        const sig = absT > 2.58 ? "***" : absT > 1.96 ? "**" : absT > 1.65 ? "*" : "";
                        const sigColor = absT > 1.96 ? "text-green-600 font-medium" : "text-muted-foreground";
                        return (
                          <TableRow key={name} className="text-[12px]">
                            <TableCell className="py-2 font-medium">{name}</TableCell>
                            <TableCell className="py-2 text-muted-foreground text-[11px]">{factorExposure.factors[name] || ""}</TableCell>
                            <TableCell className="py-2 text-right tabular-nums">{b.beta?.toFixed(4) ?? "—"}</TableCell>
                            <TableCell className="py-2 text-right tabular-nums">{b.t_stat?.toFixed(2) ?? "—"}</TableCell>
                            <TableCell className={`py-2 text-center ${sigColor}`}>{sig || "—"}</TableCell>
                          </TableRow>
                        );
                      })}
                  </TableBody>
                </Table>
              </div>

              {/* Rolling R² chart */}
              {factorExposure.rolling.length > 0 && (
                <div className="bg-card border border-border rounded p-3">
                  <h3 className="text-[13px] font-medium mb-2">滚动R²（{factorExposure.rolling_window}日窗口）</h3>
                  <ReactECharts
                    option={{
                      tooltip: { trigger: "axis", formatter: (p: any) => `${p[0]?.axisValue?.slice(0, 10)}<br/>R²: ${p[0]?.value?.toFixed(4)}` },
                      grid: { left: 50, right: 20, top: 15, bottom: 25 },
                      xAxis: {
                        type: "category",
                        data: factorExposure.rolling.map(r => r.date.slice(0, 10)),
                        axisLabel: { fontSize: 9, rotate: 30 },
                      },
                      yAxis: { type: "value", min: 0, max: 1, axisLabel: { fontSize: 10 } },
                      series: [{
                        type: "line",
                        data: factorExposure.rolling.map(r => r.r_squared),
                        smooth: true,
                        symbol: "none",
                        lineStyle: { width: 2, color: "#7c3aed" },
                        areaStyle: { color: { type: "linear", x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: "rgba(124,58,237,0.15)" }, { offset: 1, color: "rgba(124,58,237,0.01)" }] } },
                      }],
                    }}
                    style={{ height: 200 }}
                    notMerge
                  />
                </div>
              )}

              {/* Rolling factor betas chart */}
              {factorExposure.rolling.length > 0 && (
                <div className="bg-card border border-border rounded p-3">
                  <h3 className="text-[13px] font-medium mb-2">滚动因子暴露（{factorExposure.rolling_window}日窗口）</h3>
                  <ReactECharts
                    option={{
                      tooltip: { trigger: "axis" },
                      legend: {
                        data: Object.keys(factorExposure.static!.betas),
                        textStyle: { fontSize: 10 },
                        bottom: 0,
                      },
                      grid: { left: 50, right: 20, top: 15, bottom: 40 },
                      xAxis: {
                        type: "category",
                        data: factorExposure.rolling.map(r => r.date.slice(0, 10)),
                        axisLabel: { fontSize: 9, rotate: 30 },
                      },
                      yAxis: { type: "value", axisLabel: { fontSize: 10 } },
                      series: Object.keys(factorExposure.static!.betas).map(name => ({
                        name,
                        type: "line",
                        data: factorExposure.rolling.map(r => r.betas[name]?.beta ?? null),
                        smooth: true,
                        symbol: "none",
                        lineStyle: { width: 1.5 },
                      })),
                      color: ["#2563eb", "#7c3aed", "#db2777", "#ea580c", "#16a34a", "#0891b2"],
                    }}
                    style={{ height: 320 }}
                    notMerge
                  />
                </div>
              )}

              {/* Nominal vs Actual exposure comparison */}
              {Object.keys(strategyTypeWeights).length > 0 && (
                <div className="bg-card border border-border rounded p-3">
                  <h3 className="text-[13px] font-medium mb-2">名义配置 vs 实际暴露对比</h3>
                  <p className="text-[11px] text-muted-foreground mb-3">
                    蓝色：估值表中一级标签名义权重 | 橙色：回归因子Beta（归一化）
                  </p>
                  {(() => {
                    const betas = factorExposure.static!.betas;
                    // Factor name → strategy_type mapping
                    const factorToStrategy: Record<string, string> = {
                      "股票多头": "股票多头", "CTA": "期货策略", "市场中性": "股票对冲",
                      "套利策略": "套利策略", "期权策略": "期权策略", "债券": "债券策略",
                    };
                    // Use factor names as x-axis, sorted by nominal weight
                    const factorNames = Object.keys(betas);
                    const entries = factorNames.map(fn => {
                      const stType = factorToStrategy[fn] || fn;
                      const nominal = strategyTypeWeights[stType] ?? 0;
                      const beta = Math.max(0, betas[fn]?.beta ?? 0);
                      return { factor: fn, strategy: stType, nominal, beta };
                    }).sort((a, b) => b.nominal - a.nominal);

                    const betaSum = entries.reduce((s, e) => s + e.beta, 0) || 1;
                    const categories = entries.map(e => e.factor);
                    const nominalData = entries.map(e => e.nominal);
                    const betaData = entries.map(e => e.beta / betaSum);

                    return (
                      <ReactECharts
                        option={{
                          tooltip: { trigger: "axis", formatter: (params: any) => {
                            const items = Array.isArray(params) ? params : [params];
                            let tip = items[0]?.name || "";
                            for (const p of items) {
                              tip += `<br/>${p.seriesName}: ${(p.value * 100).toFixed(1)}%`;
                            }
                            return tip;
                          }},
                          legend: { data: ["名义权重", "实际暴露"], textStyle: { fontSize: 10 } },
                          grid: { left: 60, right: 20, top: 30, bottom: 25 },
                          xAxis: { type: "category", data: categories, axisLabel: { fontSize: 10 } },
                          yAxis: { type: "value", axisLabel: { fontSize: 10, formatter: (v: number) => `${(v * 100).toFixed(0)}%` } },
                          series: [
                            { name: "名义权重", type: "bar", data: nominalData, itemStyle: { color: "#2563eb" }, barGap: "10%" },
                            { name: "实际暴露", type: "bar", data: betaData, itemStyle: { color: "#ea580c" } },
                          ],
                        }}
                        style={{ height: 260 }}
                        notMerge
                      />
                    );
                  })()}
                </div>
              )}
            </>
          ) : (
            <div className="bg-card border border-border rounded p-8 text-center text-muted-foreground text-[12px]">
              {factorExposure?.error || "暂无因子暴露数据。需要产品有足够的日频净值数据（至少60个交易日）。"}
            </div>
          )}
        </TabsContent>

        {/* Attribution tab (Brinson-based) */}
        <TabsContent value="attribution" className="mt-3 space-y-4">
          {/* Period selector */}
          <div className="flex items-center gap-3 flex-wrap">
            <div className="flex items-center gap-1.5">
              <label className="text-[11px] text-muted-foreground">起始</label>
              <input type="date" value={attrStart} onChange={(e) => setAttrStart(e.target.value)}
                className="h-8 text-[12px] px-2 border border-border rounded bg-background" />
            </div>
            <div className="flex items-center gap-1.5">
              <label className="text-[11px] text-muted-foreground">结束</label>
              <input type="date" value={attrEnd} onChange={(e) => setAttrEnd(e.target.value)}
                className="h-8 text-[12px] px-2 border border-border rounded bg-background" />
            </div>
            <Button size="sm" className="h-8 text-[12px]" onClick={loadAttribution} disabled={attrLoading || !attrStart || !attrEnd}>
              {attrLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : null}
              计算归因
            </Button>
          </div>

          {/* Attribution summary cards */}
          {attribution && (
            <>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <div className="bg-card border border-border rounded p-3 text-center">
                  <p className="text-[11px] text-muted-foreground mb-1">超额收益</p>
                  <p className={`text-[16px] font-semibold tabular-nums ${attribution.cumulative_excess >= 0 ? "text-green-600" : "text-red-600"}`}>
                    {(attribution.cumulative_excess * 100).toFixed(2)}%
                  </p>
                </div>
                <div className="bg-card border border-border rounded p-3 text-center">
                  <p className="text-[11px] text-muted-foreground mb-1">配置效应</p>
                  <p className="text-[16px] font-semibold tabular-nums text-indigo-600">
                    {(attribution.cumulative_allocation * 100).toFixed(2)}%
                  </p>
                </div>
                <div className="bg-card border border-border rounded p-3 text-center">
                  <p className="text-[11px] text-muted-foreground mb-1">选择效应</p>
                  <p className="text-[16px] font-semibold tabular-nums text-blue-600">
                    {(attribution.cumulative_selection * 100).toFixed(2)}%
                  </p>
                </div>
                <div className="bg-card border border-border rounded p-3 text-center">
                  <p className="text-[11px] text-muted-foreground mb-1">交互效应</p>
                  <p className="text-[16px] font-semibold tabular-nums text-amber-600">
                    {(attribution.cumulative_interaction * 100).toFixed(2)}%
                  </p>
                </div>
              </div>

              {/* Attribution bar chart (ECharts) */}
              {attribution.aggregated_categories.length > 0 && (
                <div className="bg-card border border-border rounded p-3">
                  <h3 className="text-[13px] font-medium mb-2">分类归因</h3>
                  <ReactECharts
                    option={{
                      tooltip: { trigger: "axis" },
                      legend: { data: ["配置效应", "选择效应", "交互效应"], textStyle: { fontSize: 10 } },
                      grid: { left: 50, right: 20, top: 40, bottom: 40 },
                      xAxis: {
                        type: "category",
                        data: attribution.aggregated_categories.map((c) => c.category_name),
                        axisLabel: { fontSize: 10, rotate: 30 },
                      },
                      yAxis: {
                        type: "value",
                        axisLabel: { fontSize: 10, formatter: (v: number) => `${(v * 100).toFixed(1)}%` },
                      },
                      series: [
                        {
                          name: "配置效应",
                          type: "bar",
                          data: attribution.aggregated_categories.map((c) => c.allocation_effect),
                          itemStyle: { color: "#1e3a5f" },
                        },
                        {
                          name: "选择效应",
                          type: "bar",
                          data: attribution.aggregated_categories.map((c) => c.selection_effect),
                          itemStyle: { color: "#4f46e5" },
                        },
                        {
                          name: "交互效应",
                          type: "bar",
                          data: attribution.aggregated_categories.map((c) => c.interaction_effect),
                          itemStyle: { color: "#d4a017" },
                        },
                      ],
                    }}
                    style={{ height: 300 }}
                  />
                </div>
              )}

              {/* Attribution detail table */}
              {attribution.aggregated_categories.length > 0 && (
                <div className="bg-card border border-border rounded overflow-hidden">
                  <h3 className="text-[13px] font-medium p-3 pb-2">归因分解明细</h3>
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-muted/40">
                        <TableHead className="h-8 text-[11px] font-normal">分类</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">基准权重</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">实际权重</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">基准收益</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">实际收益</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">配置效应</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">选择效应</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">交互效应</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">合计</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {attribution.aggregated_categories.map((c) => (
                        <TableRow key={c.category} className="text-[12px]">
                          <TableCell className="py-2 font-medium">{c.category_name}</TableCell>
                          <TableCell className="py-2 text-right tabular-nums">{(c.benchmark_weight * 100).toFixed(2)}%</TableCell>
                          <TableCell className="py-2 text-right tabular-nums">{(c.actual_weight * 100).toFixed(2)}%</TableCell>
                          <TableCell className="py-2 text-right tabular-nums">{(c.benchmark_return * 100).toFixed(2)}%</TableCell>
                          <TableCell className="py-2 text-right tabular-nums">{(c.actual_return * 100).toFixed(2)}%</TableCell>
                          <TableCell className={`py-2 text-right tabular-nums ${c.allocation_effect >= 0 ? "text-green-600" : "text-red-600"}`}>
                            {(c.allocation_effect * 100).toFixed(3)}%
                          </TableCell>
                          <TableCell className={`py-2 text-right tabular-nums ${c.selection_effect >= 0 ? "text-green-600" : "text-red-600"}`}>
                            {(c.selection_effect * 100).toFixed(3)}%
                          </TableCell>
                          <TableCell className={`py-2 text-right tabular-nums ${c.interaction_effect >= 0 ? "text-green-600" : "text-red-600"}`}>
                            {(c.interaction_effect * 100).toFixed(3)}%
                          </TableCell>
                          <TableCell className={`py-2 text-right tabular-nums font-medium ${c.total_effect >= 0 ? "text-green-600" : "text-red-600"}`}>
                            {(c.total_effect * 100).toFixed(3)}%
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}

              {attribution.aggregated_categories.length === 0 && (
                <div className="bg-card border border-border rounded p-6 text-center text-muted-foreground text-[12px]">
                  无归因数据。请确保产品已配置基准，且在选定期间内有估值快照。
                </div>
              )}
            </>
          )}

          {!attribution && !attrLoading && (
            <div className="bg-card border border-border rounded p-8 text-center text-muted-foreground text-[12px]">
              选择日期区间后点击「计算归因」查看Brinson归因分析结果
            </div>
          )}
        </TabsContent>

        {/* Holdings tab — hierarchical table */}
        <TabsContent value="holdings" className="mt-3">
          <div className="bg-card border border-border rounded overflow-hidden">
            {holdings.length === 0 ? (
              <div className="h-40 flex items-center justify-center text-muted-foreground text-[12px]">
                暂无持仓明细，请先上传估值表
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="bg-muted/40">
                    <TableHead className="h-8 text-[11px] font-normal">科目代码</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal">科目名称</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">数量</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">成本</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">市值</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">占净值%</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">估值增值</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {l1Items.map((l1) => {
                    const isExpL1 = expandedL1.has(l1.item_code);
                    const children2 = getChildren(l1.item_code, 2);
                    const hasChildren = children2.length > 0;

                    return (
                      <HoldingRows
                        key={l1.item_code}
                        item={l1}
                        level={1}
                        expanded={isExpL1}
                        hasChildren={hasChildren}
                        onToggle={() => toggleL1(l1.item_code)}
                      >
                        {isExpL1 && children2.map((l2) => {
                          const isExpL2 = expandedL2.has(l2.item_code);
                          const children3 = getChildren(l2.item_code, 3);
                          const children4 = getChildren(l2.item_code, 4);
                          const subItems = [...children3, ...children4];
                          const hasSubChildren = subItems.length > 0;

                          return (
                            <HoldingRows
                              key={l2.item_code}
                              item={l2}
                              level={2}
                              expanded={isExpL2}
                              hasChildren={hasSubChildren}
                              onToggle={() => toggleL2(l2.item_code)}
                            >
                              {isExpL2 && subItems.map((l34) => (
                                <HoldingRows
                                  key={l34.item_code}
                                  item={l34}
                                  level={l34.level}
                                  expanded={false}
                                  hasChildren={false}
                                  onToggle={() => {}}
                                />
                              ))}
                            </HoldingRows>
                          );
                        })}
                      </HoldingRows>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </div>
        </TabsContent>

        {/* Valuation history tab */}
        <TabsContent value="history" className="mt-3">
          <div className="bg-card border border-border rounded overflow-hidden">
            {allSnapshots.length === 0 ? (
              <div className="h-40 flex items-center justify-center text-muted-foreground text-[12px]">
                暂无估值记录
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="bg-muted/40">
                    <TableHead className="h-8 text-[11px] font-normal w-12 text-center">序号</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal">估值日期</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">单位净值</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">资产净值</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal">来源文件</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal">导入时间</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {allSnapshots.map((s, i) => (
                    <TableRow key={s.id} className="text-[12px]">
                      <TableCell className="py-2 text-center text-muted-foreground">{i + 1}</TableCell>
                      <TableCell className="py-2 tabular-nums font-medium">{s.valuation_date}</TableCell>
                      <TableCell className="py-2 text-right tabular-nums">{s.unit_nav?.toFixed(4) ?? "--"}</TableCell>
                      <TableCell className="py-2 text-right tabular-nums">{fmtMoney(s.total_nav)}</TableCell>
                      <TableCell className="py-2 text-muted-foreground text-[11px] max-w-[200px] truncate">
                        {s.source_file || "--"}
                      </TableCell>
                      <TableCell className="py-2 text-muted-foreground text-[11px] tabular-nums">
                        {s.imported_at ? s.imported_at.slice(0, 16).replace("T", " ") : "--"}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </div>
        </TabsContent>
      </Tabs>

      {/* Product notes */}
      {product.notes && (
        <div className="bg-card border border-border rounded p-3">
          <h3 className="text-[13px] font-medium mb-1">备注</h3>
          <p className="text-[12px] text-muted-foreground whitespace-pre-wrap">{product.notes}</p>
        </div>
      )}

      {/* Report generation dialog */}
      {reportDialogOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-background border border-border rounded-lg shadow-lg w-[420px] p-5 space-y-4">
            <h2 className="text-[15px] font-semibold">生成产品报告</h2>
            <div className="space-y-3">
              <div>
                <label className="text-[11px] text-muted-foreground block mb-1">报告类型</label>
                <select value={reportType} onChange={(e) => setReportType(e.target.value)}
                  className="h-8 w-full text-[12px] px-2 border border-border rounded bg-background">
                  <option value="monthly">月度报告</option>
                  <option value="weekly">周度报告</option>
                </select>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="text-[11px] text-muted-foreground block mb-1">起始日期</label>
                  <input type="date" value={reportStart} onChange={(e) => setReportStart(e.target.value)}
                    className="h-8 w-full text-[12px] px-2 border border-border rounded bg-background" />
                </div>
                <div>
                  <label className="text-[11px] text-muted-foreground block mb-1">结束日期</label>
                  <input type="date" value={reportEnd} onChange={(e) => setReportEnd(e.target.value)}
                    className="h-8 w-full text-[12px] px-2 border border-border rounded bg-background" />
                </div>
              </div>
            </div>
            <div className="flex justify-end gap-2 pt-2">
              <Button variant="outline" size="sm" className="h-8 text-[12px]" onClick={() => setReportDialogOpen(false)}>
                取消
              </Button>
              <Button size="sm" className="h-8 text-[12px] gap-1.5" onClick={handleGenerateReport}
                disabled={generating || !reportStart || !reportEnd}>
                {generating ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <FileText className="h-3.5 w-3.5" />}
                生成并下载PDF
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* --- InfoCard --- */

function InfoCard({ icon: Icon, label, value, sub }: {
  icon: React.ElementType;
  label: string;
  value: string;
  sub: string;
}) {
  return (
    <div className="bg-card border border-border rounded p-3">
      <div className="flex items-center gap-1.5 mb-1">
        <Icon className="h-3.5 w-3.5 text-muted-foreground" />
        <span className="text-[11px] text-muted-foreground">{label}</span>
      </div>
      <p className="text-[16px] font-semibold tabular-nums">{value}</p>
      <p className="text-[10px] text-muted-foreground mt-0.5">{sub}</p>
    </div>
  );
}

/* --- HoldingRows (recursive hierarchical table row) --- */

function HoldingRows({ item, level, expanded, hasChildren, onToggle, children }: {
  item: HoldingItem;
  level: number;
  expanded: boolean;
  hasChildren: boolean;
  onToggle: () => void;
  children?: React.ReactNode;
}) {
  const indent = (level - 1) * 16;
  const bgClass = level === 1 ? "bg-muted/20 font-medium" : level === 2 ? "bg-muted/10" : "";

  return (
    <>
      <TableRow className={`text-[12px] ${bgClass}`}>
        <TableCell className="py-1.5" style={{ paddingLeft: 8 + indent }}>
          <div className="flex items-center gap-1">
            {hasChildren ? (
              <button onClick={onToggle} className="p-0.5 hover:bg-muted rounded">
                {expanded
                  ? <ChevronDown className="h-3 w-3 text-muted-foreground" />
                  : <ChevronRight className="h-3 w-3 text-muted-foreground" />
                }
              </button>
            ) : (
              <span className="w-4" />
            )}
            <span className="text-[11px] text-muted-foreground tabular-nums">{item.item_code}</span>
          </div>
        </TableCell>
        <TableCell className="py-1.5">{item.item_name}</TableCell>
        <TableCell className="py-1.5 text-right tabular-nums">{fmtNum(item.quantity, 0)}</TableCell>
        <TableCell className="py-1.5 text-right tabular-nums">{fmtNum(item.cost_amount)}</TableCell>
        <TableCell className="py-1.5 text-right tabular-nums">{fmtNum(item.market_value)}</TableCell>
        <TableCell className="py-1.5 text-right tabular-nums">{fmtPct(item.value_pct_nav)}</TableCell>
        <TableCell className={`py-1.5 text-right tabular-nums ${
          (item.value_diff ?? 0) >= 0 ? "text-green-600" : "text-red-600"
        }`}>
          {fmtNum(item.value_diff)}
        </TableCell>
      </TableRow>
      {children}
    </>
  );
}

function fmtNum(v: number | null | undefined, dp = 2): string {
  if (v == null || !isFinite(v)) return "--";
  return v.toLocaleString("zh-CN", { minimumFractionDigits: dp, maximumFractionDigits: dp });
}

function fmtPct(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return "--";
  return `${v.toFixed(2)}%`;
}

function fmtMoney(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return "--";
  if (Math.abs(v) >= 1e8) return `${(v / 1e8).toFixed(2)}亿`;
  if (Math.abs(v) >= 1e4) return `${(v / 1e4).toFixed(2)}万`;
  return v.toFixed(2);
}
