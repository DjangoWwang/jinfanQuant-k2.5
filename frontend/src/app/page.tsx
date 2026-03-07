"use client";

import { useEffect, useState, useMemo } from "react";
import { useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import {
  TrendingUp,
  TrendingDown,
  ArrowUpRight,
  ArrowDownRight,
  BarChart3,
  Activity,
  Database,
  GitCompare,
  Briefcase,
  Loader2,
  AlertTriangle,
  AlertCircle,
  Bell,
  ShieldAlert,
  ChevronRight,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/layout/page-header";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { fetchApi, fetchApiAuth, type AlertDashboard, type AlertEvent } from "@/lib/api";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

/* --- Types --- */

interface FundStats {
  total: number;
  daily_count: number;
  weekly_count: number;
  strategy_breakdown: { strategy_type: string; count: number }[];
}

interface PoolCounts {
  basic: number;
  watch: number;
  investment: number;
}

interface ProductSummary {
  product_id: number;
  product_name: string;
  product_type: string;
  unit_nav: number | null;
  total_nav: number | null;
  valuation_date: string | null;
  daily_return_pct: number | null;
}

interface DashboardData {
  date: string;
  live_products: ProductSummary[];
  simulation_products: ProductSummary[];
}

interface NavPoint {
  date: string;
  unit_nav: number | null;
  total_nav: number | null;
}

interface SubFundAlloc {
  filing_number: string;
  fund_name: string;
  market_value: number | null;
  weight_pct: number | null;
  appreciation: number | null;
  linked_fund_id: number | null;
}

interface ValuationSnapshot {
  id: number;
  valuation_date: string;
  unit_nav: number | null;
  total_nav: number | null;
  sub_fund_allocations: SubFundAlloc[];
}

interface ProductMetrics {
  annualized_return?: number | null;
  max_drawdown?: number | null;
  sharpe_ratio?: number | null;
  week_return?: number | null;
  month_return?: number | null;
  ytd_return?: number | null;
}

/* --- Helpers --- */

function MetricCell({ label, value, sub, up }: { label: string; value: string; sub?: string; up?: boolean }) {
  return (
    <div className="px-4 py-2.5">
      <div className="text-[11px] text-muted-foreground mb-0.5">{label}</div>
      <div className="tabular-nums text-lg font-semibold leading-tight">{value}</div>
      {sub !== undefined && (
        <div className={`tabular-nums text-[11px] mt-0.5 flex items-center gap-0.5 ${up === undefined ? "text-muted-foreground" : up ? "text-jf-up" : "text-jf-down"}`}>
          {up !== undefined && (up ? <ArrowUpRight className="h-3 w-3" /> : <ArrowDownRight className="h-3 w-3" />)}
          {sub}
        </div>
      )}
    </div>
  );
}

function fmtPct(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return "--";
  return `${v >= 0 ? "+" : ""}${(v * 100).toFixed(2)}%`;
}

function fmtMoney(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return "--";
  if (Math.abs(v) >= 1e8) return `${(v / 1e8).toFixed(2)}亿`;
  if (Math.abs(v) >= 1e4) return `${(v / 1e4).toFixed(2)}万`;
  return v.toFixed(2);
}

/* --- Page --- */

export default function DashboardPage() {
  const router = useRouter();
  const [fundStats, setFundStats] = useState<FundStats | null>(null);
  const [poolCounts, setPoolCounts] = useState<PoolCounts | null>(null);
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [navSeries, setNavSeries] = useState<NavPoint[]>([]);
  const [subFunds, setSubFunds] = useState<SubFundAlloc[]>([]);
  const [metrics, setMetrics] = useState<ProductMetrics>({});
  const [loading, setLoading] = useState(true);
  const [navPeriod, setNavPeriod] = useState("ALL");
  const [selectedProductId, setSelectedProductId] = useState<number | null>(null);
  const [alertDashboard, setAlertDashboard] = useState<AlertDashboard | null>(null);
  const [alertAuthError, setAlertAuthError] = useState(false);

  // Load core stats + dashboard
  useEffect(() => {
    // Load alert dashboard (auth-protected, fails gracefully)
    fetchApiAuth<AlertDashboard>("/alerts/dashboard")
      .then(data => { setAlertDashboard(data); setAlertAuthError(false); })
      .catch(e => {
        if (e instanceof Error && e.message === "AUTH_REQUIRED") setAlertAuthError(true);
      });

    Promise.allSettled([
      fetchApi<{ total: number; items: Array<{ nav_frequency: string; strategy_type: string | null }> }>("/funds/?page=1&page_size=1")
        .then(res => {
          return fetchApi<Array<{ strategy_type: string; total: number }>>("/funds/strategy-categories")
            .then(cats => {
              const total = cats.reduce((sum, c) => sum + c.total, 0);
              return {
                total: res.total || total,
                daily_count: 0,
                weekly_count: 0,
                strategy_breakdown: cats.map(c => ({ strategy_type: c.strategy_type, count: c.total })),
              } as FundStats;
            });
        }),
      fetchApi<PoolCounts>("/pools/counts"),
      fetchApi<DashboardData>("/mobile/dashboard"),
    ]).then(([statsResult, poolsResult, dashResult]) => {
      if (statsResult.status === "fulfilled") setFundStats(statsResult.value);
      if (poolsResult.status === "fulfilled") setPoolCounts(poolsResult.value);
      if (dashResult.status === "fulfilled") {
        setDashboard(dashResult.value);
        // Auto-select first live product WITH data, fallback to first with any data
        const liveProds = dashResult.value.live_products;
        const simProds = dashResult.value.simulation_products;
        const allProds = [...liveProds, ...simProds];
        const withData = allProds.find(p => p.unit_nav != null);
        if (withData) {
          setSelectedProductId(withData.product_id);
        } else if (allProds.length > 0) {
          setSelectedProductId(allProds[0].product_id);
        }
      }
      setLoading(false);
    });
  }, []);

  // Load NAV + holdings for selected product
  useEffect(() => {
    if (!selectedProductId) return;
    Promise.allSettled([
      fetchApi<{ nav_series: NavPoint[] }>(`/products/${selectedProductId}/nav`),
      fetchApi<{ items: ValuationSnapshot[]; total: number }>(`/products/${selectedProductId}/valuations?page_size=1`),
    ]).then(async ([navResult, valResult]) => {
      if (navResult.status === "fulfilled") {
        setNavSeries(navResult.value.nav_series);
        // Compute simple metrics from nav series
        const series = navResult.value.nav_series.filter(n => n.unit_nav != null);
        if (series.length >= 2) {
          const latest = series[series.length - 1].unit_nav!;
          const first = series[0].unit_nav!;
          const m: ProductMetrics = {};
          // YTD
          const yearStart = series.find(n => n.date >= `${new Date().getFullYear()}-01-01`);
          if (yearStart?.unit_nav) m.ytd_return = (latest - yearStart.unit_nav) / yearStart.unit_nav;
          // 1M
          const oneMonthAgo = new Date(); oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
          const monthStr = oneMonthAgo.toISOString().slice(0, 10);
          const monthPoint = series.find(n => n.date >= monthStr);
          if (monthPoint?.unit_nav) m.month_return = (latest - monthPoint.unit_nav) / monthPoint.unit_nav;
          // 1W
          const oneWeekAgo = new Date(); oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);
          const weekStr = oneWeekAgo.toISOString().slice(0, 10);
          const weekPoint = series.find(n => n.date >= weekStr);
          if (weekPoint?.unit_nav) m.week_return = (latest - weekPoint.unit_nav) / weekPoint.unit_nav;
          // Max drawdown
          let peak = series[0].unit_nav!;
          let maxDD = 0;
          for (const p of series) {
            if (p.unit_nav! > peak) peak = p.unit_nav!;
            const dd = (p.unit_nav! - peak) / peak;
            if (dd < maxDD) maxDD = dd;
          }
          m.max_drawdown = maxDD;
          // Annualized
          const days = (new Date(series[series.length - 1].date).getTime() - new Date(series[0].date).getTime()) / 86400000;
          if (days > 0 && first > 0) {
            m.annualized_return = Math.pow(latest / first, 365 / days) - 1;
          }
          setMetrics(m);
        } else {
          setMetrics({});
        }
      }
      if (valResult.status === "fulfilled" && valResult.value.items.length > 0) {
        // Load the latest snapshot detail to get sub_fund_allocations
        try {
          const snap = await fetchApi<ValuationSnapshot>(
            `/products/${selectedProductId}/valuation/${valResult.value.items[0].id}`
          );
          setSubFunds(snap.sub_fund_allocations || []);
        } catch {
          setSubFunds([]);
        }
      } else {
        setSubFunds([]);
      }
    });
  }, [selectedProductId]);

  // Active product info
  const activeProduct = useMemo(() => {
    if (!dashboard || !selectedProductId) return null;
    return [...dashboard.live_products, ...dashboard.simulation_products]
      .find(p => p.product_id === selectedProductId) || null;
  }, [dashboard, selectedProductId]);

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

  // NAV chart option
  const navChartOption = useMemo(() => ({
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
      data: filteredNav.map(n => n.date),
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
        data: filteredNav.map(n => n.unit_nav),
        smooth: true,
        symbol: "none",
        lineStyle: { width: 2, color: "#4f46e5" },
        areaStyle: {
          color: {
            type: "linear",
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: "rgba(79,70,229,0.15)" },
              { offset: 1, color: "rgba(79,70,229,0.01)" },
            ],
          },
        },
      },
    ],
  }), [filteredNav]);

  // Alerts: sub-funds with notable weekly changes
  const watchAlerts = useMemo(() => {
    if (subFunds.length === 0) return [];
    // We don't have live weekly return data from sub-funds directly
    // Show sub-fund appreciation as alerts
    return subFunds
      .filter(sf => sf.appreciation != null && sf.appreciation !== 0)
      .sort((a, b) => Math.abs(b.appreciation!) - Math.abs(a.appreciation!))
      .slice(0, 6)
      .map(sf => ({
        name: sf.fund_name,
        value: sf.appreciation!,
        reason: sf.appreciation! >= 0 ? "浮盈" : "浮亏",
        positive: sf.appreciation! >= 0,
        linked_fund_id: sf.linked_fund_id,
      }));
  }, [subFunds]);

  return (
    <div className="space-y-3">
      <PageHeader
        title="概览"
        description={activeProduct?.valuation_date ? `数据截至 ${activeProduct.valuation_date}` : "晋帆投研FOF平台"}
      />

      {/* 快捷入口 */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
        {[
          { label: "基金数据库", icon: Database, href: "/fund-database", count: fundStats?.total ?? "—", sub: "只基金" },
          { label: "基金比较", icon: GitCompare, href: "/fund-research/comparison", count: "—", sub: "对比分析" },
          { label: "基金池", icon: Briefcase, href: "/fund-research/pools", count: poolCounts ? `${poolCounts.watch + poolCounts.investment}` : "—", sub: "关注中" },
          { label: "策略分类", icon: BarChart3, href: "/fund-database", count: fundStats?.strategy_breakdown?.filter(s => !["未分类", "FOF", "其他"].includes(s.strategy_type)).length ?? "—", sub: "个策略" },
        ].map(item => (
          <button
            key={item.label}
            onClick={() => router.push(item.href)}
            className="bg-card border border-border rounded px-4 py-3 text-left hover:border-primary/30 hover:shadow-sm transition-all group"
          >
            <div className="flex items-center justify-between mb-1.5">
              <item.icon className="h-4 w-4 text-muted-foreground group-hover:text-primary transition-colors" />
              <span className="text-[10px] text-muted-foreground">{item.sub}</span>
            </div>
            <div className="text-xl font-bold tabular-nums">
              {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : item.count}
            </div>
            <div className="text-[11px] text-muted-foreground mt-0.5">{item.label}</div>
          </button>
        ))}
      </div>

      {/* Product Summary Strip */}
      {activeProduct ? (
        <div className="bg-card border border-border rounded">
          <div className="flex items-center border-b border-border px-4 py-1.5">
            <button
              className="text-[13px] font-semibold hover:text-primary transition-colors"
              onClick={() => router.push(`/product-ops/${activeProduct.product_id}`)}
            >
              {activeProduct.product_name}
            </button>
            <Badge className="ml-2 text-[10px] bg-primary/10 text-primary border-primary/20 hover:bg-primary/10">
              {activeProduct.product_type === "live" ? "实盘" : "模拟"}
            </Badge>
            {/* Product selector if multiple */}
            {dashboard && [...dashboard.live_products, ...dashboard.simulation_products].length > 1 && (
              <select
                className="ml-3 text-[11px] bg-transparent border border-border rounded px-2 py-0.5"
                value={selectedProductId ?? ""}
                onChange={e => setSelectedProductId(Number(e.target.value))}
              >
                {dashboard.live_products.map(p => (
                  <option key={p.product_id} value={p.product_id}>[实盘] {p.product_name}</option>
                ))}
                {dashboard.simulation_products.map(p => (
                  <option key={p.product_id} value={p.product_id}>[模拟] {p.product_name}</option>
                ))}
              </select>
            )}
            <span className="ml-auto text-[10px] text-muted-foreground">
              {activeProduct.valuation_date ?? ""}
            </span>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 divide-x divide-border">
            <MetricCell
              label="最新净值"
              value={activeProduct.unit_nav?.toFixed(4) ?? "--"}
              sub={activeProduct.daily_return_pct != null ? `${activeProduct.daily_return_pct >= 0 ? "+" : ""}${activeProduct.daily_return_pct.toFixed(2)}%` : undefined}
              up={activeProduct.daily_return_pct != null ? activeProduct.daily_return_pct >= 0 : undefined}
            />
            <MetricCell
              label="近一周"
              value={metrics.week_return != null ? fmtPct(metrics.week_return) : "--"}
              up={metrics.week_return != null ? metrics.week_return >= 0 : undefined}
            />
            <MetricCell
              label="近一月"
              value={metrics.month_return != null ? fmtPct(metrics.month_return) : "--"}
              up={metrics.month_return != null ? metrics.month_return >= 0 : undefined}
            />
            <MetricCell
              label="今年以来"
              value={metrics.ytd_return != null ? fmtPct(metrics.ytd_return) : "--"}
              up={metrics.ytd_return != null ? metrics.ytd_return >= 0 : undefined}
            />
            <MetricCell
              label="最大回撤"
              value={metrics.max_drawdown != null ? `${(metrics.max_drawdown * 100).toFixed(2)}%` : "--"}
            />
            <MetricCell
              label="年化收益"
              value={metrics.annualized_return != null ? fmtPct(metrics.annualized_return) : "--"}
            />
            <MetricCell
              label="基金入库"
              value={loading ? "..." : `${fundStats?.total ?? 0}只`}
              sub={poolCounts ? `观察${poolCounts.watch} / 投资${poolCounts.investment}` : undefined}
            />
          </div>
        </div>
      ) : !loading ? (
        <div className="bg-card border border-border rounded p-6 text-center text-muted-foreground text-[12px]">
          <AlertTriangle className="h-5 w-5 mx-auto mb-2 opacity-40" />
          暂无产品数据，请先在「产品运营」中创建产品并上传估值表
        </div>
      ) : null}

      {/* NAV Chart */}
      <div className="bg-card border border-border rounded">
        <div className="flex items-center justify-between px-4 py-1.5 border-b border-border">
          <span className="text-[13px] font-medium">净值走势</span>
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
        {filteredNav.length > 0 ? (
          <div className="px-2 py-1">
            <ReactECharts option={navChartOption} style={{ height: 220 }} />
          </div>
        ) : (
          <div className="h-48 flex items-center justify-center text-muted-foreground">
            <div className="text-center space-y-1">
              <BarChart3 className="mx-auto h-7 w-7 opacity-25" />
              <p className="text-[12px] opacity-60">
                {selectedProductId ? "暂无净值数据，请上传估值表" : "选择产品查看净值走势"}
              </p>
            </div>
          </div>
        )}
      </div>

      {/* 风险预警 */}
      <div className="bg-card border border-border rounded">
        <div className="flex items-center justify-between px-4 py-1.5 border-b border-border">
          <div className="flex items-center gap-2">
            <ShieldAlert className="h-3.5 w-3.5 text-muted-foreground opacity-50" />
            <span className="text-[13px] font-medium">风险预警</span>
            {alertDashboard && alertDashboard.unread_total > 0 && (
              <span className="flex items-center gap-1">
                {(alertDashboard.alerts_by_severity?.critical ?? 0) > 0 && (
                  <Badge className="text-[10px] bg-red-500/10 text-red-600 border-red-200 hover:bg-red-500/10 px-1.5 py-0">
                    <AlertCircle className="h-3 w-3 mr-0.5" />
                    {alertDashboard.alerts_by_severity.critical}
                  </Badge>
                )}
                {(alertDashboard.alerts_by_severity?.warning ?? 0) > 0 && (
                  <Badge className="text-[10px] bg-amber-500/10 text-amber-600 border-amber-200 hover:bg-amber-500/10 px-1.5 py-0">
                    <AlertTriangle className="h-3 w-3 mr-0.5" />
                    {alertDashboard.alerts_by_severity.warning}
                  </Badge>
                )}
              </span>
            )}
          </div>
          <button
            onClick={() => router.push("/risk-alerts")}
            className="flex items-center gap-0.5 text-[11px] text-muted-foreground hover:text-primary transition-colors"
          >
            查看全部
            <ChevronRight className="h-3 w-3" />
          </button>
        </div>
        {alertAuthError ? (
          <div className="h-24 flex items-center justify-center text-muted-foreground text-[12px]">
            请登录后查看风险预警
          </div>
        ) : alertDashboard && alertDashboard.recent_events.length > 0 ? (
          <div className="divide-y divide-border">
            {alertDashboard.recent_events.slice(0, 5).map((event) => (
              <div
                key={event.id}
                className={`px-4 py-2 hover:bg-muted/30 transition-colors flex items-center gap-3 ${!event.is_read ? "bg-primary/[0.02]" : ""}`}
              >
                {event.severity === "critical" ? (
                  <AlertCircle className="h-4 w-4 text-red-500 shrink-0" />
                ) : (
                  <AlertTriangle className="h-4 w-4 text-amber-500 shrink-0" />
                )}
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-[12px] font-medium truncate">{event.target_name}</span>
                    {!event.is_read && (
                      <span className="h-1.5 w-1.5 rounded-full bg-primary shrink-0" />
                    )}
                  </div>
                  <div className="text-[11px] text-muted-foreground truncate">{event.message}</div>
                </div>
                <span className="text-[10px] text-muted-foreground tabular-nums shrink-0 whitespace-nowrap">
                  {(() => {
                    try {
                      const d = new Date(event.created_at);
                      const diff = Date.now() - d.getTime();
                      if (diff < 60000) return "刚刚";
                      if (diff < 3600000) return `${Math.floor(diff / 60000)}分钟前`;
                      if (diff < 86400000) return `${Math.floor(diff / 3600000)}小时前`;
                      return d.toLocaleDateString("zh-CN", { month: "2-digit", day: "2-digit" });
                    } catch { return event.created_at; }
                  })()}
                </span>
              </div>
            ))}
          </div>
        ) : (
          <div className="h-24 flex items-center justify-center text-muted-foreground text-[12px]">
            <div className="text-center space-y-1">
              <Bell className="mx-auto h-5 w-5 opacity-25" />
              <p className="opacity-60">暂无预警事件</p>
            </div>
          </div>
        )}
      </div>

      {/* Holdings + Alerts */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
        {/* Holdings Table */}
        <div className="lg:col-span-2 bg-card border border-border rounded">
          <div className="flex items-center justify-between px-4 py-1.5 border-b border-border">
            <span className="text-[13px] font-medium">子基金持仓</span>
            <span className="text-[11px] text-muted-foreground">
              {subFunds.length > 0 ? `${subFunds.length} 只` : "暂无数据"}
            </span>
          </div>
          {subFunds.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="h-7 text-[11px] font-normal">基金名称</TableHead>
                  <TableHead className="h-7 text-[11px] font-normal text-right">权重</TableHead>
                  <TableHead className="h-7 text-[11px] font-normal text-right">市值</TableHead>
                  <TableHead className="h-7 text-[11px] font-normal text-right">浮盈</TableHead>
                  <TableHead className="h-7 text-[11px] font-normal text-center">关联</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {subFunds.map((sf, i) => (
                  <TableRow key={i} className="text-[12px]">
                    <TableCell className="py-1.5 font-medium">
                      {sf.linked_fund_id ? (
                        <button
                          className="text-primary hover:underline text-left"
                          onClick={() => router.push(`/fund-database/${sf.linked_fund_id}`)}
                        >
                          {sf.fund_name}
                        </button>
                      ) : sf.fund_name}
                    </TableCell>
                    <TableCell className="py-1.5 text-right tabular-nums text-muted-foreground">
                      {sf.weight_pct != null ? `${sf.weight_pct.toFixed(2)}%` : "--"}
                    </TableCell>
                    <TableCell className="py-1.5 text-right tabular-nums">{fmtMoney(sf.market_value)}</TableCell>
                    <TableCell className={`py-1.5 text-right tabular-nums ${
                      (sf.appreciation ?? 0) >= 0 ? "text-jf-up" : "text-jf-down"
                    }`}>
                      {fmtMoney(sf.appreciation)}
                    </TableCell>
                    <TableCell className="py-1.5 text-center">
                      {sf.linked_fund_id ? (
                        <Badge variant="outline" className="text-[10px] border-green-300 text-green-600">已关联</Badge>
                      ) : (
                        <span className="text-[10px] text-muted-foreground">未关联</span>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <div className="h-32 flex items-center justify-center text-muted-foreground text-[12px]">
              {selectedProductId ? "暂无持仓数据" : "选择产品查看持仓"}
            </div>
          )}
        </div>

        {/* Alerts */}
        <div className="bg-card border border-border rounded">
          <div className="flex items-center justify-between px-4 py-1.5 border-b border-border">
            <span className="text-[13px] font-medium">子基金浮盈监控</span>
            <Activity className="h-3.5 w-3.5 text-muted-foreground opacity-50" />
          </div>
          {watchAlerts.length > 0 ? (
            <div className="divide-y divide-border">
              {watchAlerts.map((a, i) => (
                <div
                  key={i}
                  className="px-4 py-2 hover:bg-muted/30 transition-colors cursor-pointer"
                  onClick={() => a.linked_fund_id ? router.push(`/fund-database/${a.linked_fund_id}`) : null}
                >
                  <div className="flex items-center justify-between">
                    <span className="text-[12px] font-medium truncate mr-2">{a.name}</span>
                    <span className={`tabular-nums text-[12px] font-semibold flex items-center gap-0.5 shrink-0 ${a.positive ? "text-jf-up" : "text-jf-down"}`}>
                      {a.positive ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
                      {fmtMoney(a.value)}
                    </span>
                  </div>
                  <div className="text-[11px] text-muted-foreground mt-0.5">{a.reason}</div>
                </div>
              ))}
            </div>
          ) : (
            <div className="h-32 flex items-center justify-center text-muted-foreground text-[12px]">
              暂无异动数据
            </div>
          )}
        </div>
      </div>

      {/* 策略分布 (真实数据) */}
      {fundStats?.strategy_breakdown && fundStats.strategy_breakdown.length > 0 && (
        <div className="bg-card border border-border rounded">
          <div className="px-4 py-1.5 border-b border-border">
            <span className="text-[13px] font-medium">策略分布</span>
          </div>
          <div className="px-4 py-3 flex flex-wrap gap-2">
            {fundStats.strategy_breakdown.map(s => (
              <div
                key={s.strategy_type}
                className="flex items-center gap-2 px-3 py-1.5 bg-muted/50 rounded text-[12px] hover:bg-muted transition-colors cursor-pointer"
                onClick={() => router.push(`/fund-database?strategy_type=${encodeURIComponent(s.strategy_type)}`)}
              >
                <span className="font-medium">{s.strategy_type}</span>
                <span className="text-muted-foreground tabular-nums">{s.count}只</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
