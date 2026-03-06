"use client";

import { useEffect, useState, useMemo } from "react";
import { useParams, useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import { ArrowLeft, Loader2, Calendar, TrendingUp, TrendingDown, Activity, BarChart3, FlaskConical } from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import { fetchApi } from "@/lib/api";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

/* ─── Types ─── */

interface FundDetail {
  id: number;
  fund_name: string;
  filing_number: string | null;
  manager_name: string | null;
  inception_date: string | null;
  strategy_type: string | null;
  strategy_sub: string | null;
  latest_nav: number | null;
  latest_nav_date: string | null;
  nav_frequency: string;
  data_source: string;
  status: string;
}

interface NavRecord {
  nav_date: string;
  unit_nav: number;
  cumulative_nav: number | null;
  daily_return: number | null;
}

interface NavResponse {
  fund_id: number;
  fund_name: string;
  frequency: string | null;
  records: NavRecord[];
  total_count: number;
}

interface Metrics {
  fund_id: number;
  fund_name: string;
  start_date: string | null;
  end_date: string | null;
  total_return: number | null;
  annualized_return: number | null;
  max_drawdown: number | null;
  max_dd_peak: string | null;
  max_dd_trough: string | null;
  annualized_volatility: number | null;
  sharpe_ratio: number | null;
  sortino_ratio: number | null;
  calmar_ratio: number | null;
}

/* ─── Helpers ─── */

function pct(v: number | null | undefined, digits = 2): string {
  if (v == null || !isFinite(v)) return "—";
  // Cap display at ±9999% to avoid astronomical numbers from short-history funds
  const capped = Math.max(Math.min(v, 99.99), -9.999);
  const s = (capped * 100).toFixed(digits);
  const display = capped >= 0 ? `+${s}%` : `${s}%`;
  return Math.abs(v) > 99.99 ? `${display}*` : display;
}

function num(v: number | null | undefined, digits = 2): string {
  if (v == null || !isFinite(v)) return "—";
  // Cap ratios at ±999.99 for display
  const capped = Math.max(Math.min(v, 999.99), -999.99);
  return capped.toFixed(digits);
}

/** 计算回撤序列 */
function calcDrawdown(navs: { date: string; nav: number }[]) {
  let peak = -Infinity;
  return navs.map(({ date, nav }) => {
    if (nav > peak) peak = nav;
    const dd = peak > 0 ? (nav - peak) / peak : 0;
    return { date, dd };
  });
}

/* ─── Interval Presets ─── */
const intervals = [
  { key: "1m", label: "近1月" },
  { key: "3m", label: "近3月" },
  { key: "6m", label: "近6月" },
  { key: "ytd", label: "今年以来" },
  { key: "1y", label: "近1年" },
  { key: "3y", label: "近3年" },
  { key: "inception", label: "成立以来" },
];

/* ─── Metric Card ─── */

function MetricCard({ label, value, sub, positive }: {
  label: string; value: string; sub?: string; positive?: boolean;
}) {
  return (
    <div className="bg-card border border-border rounded px-4 py-3">
      <div className="text-[11px] text-muted-foreground mb-1">{label}</div>
      <div className={`text-lg font-semibold tabular-nums leading-tight ${
        positive === true ? "text-jf-up" : positive === false ? "text-jf-down" : ""
      }`}>
        {value}
      </div>
      {sub && <div className="text-[10px] text-muted-foreground mt-0.5">{sub}</div>}
    </div>
  );
}

/* ─── Page ─── */

export default function FundDetailPage() {
  const params = useParams();
  const router = useRouter();
  const fundId = params.fundId as string;

  const [fund, setFund] = useState<FundDetail | null>(null);
  const [navData, setNavData] = useState<NavResponse | null>(null);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [interval, setInterval] = useState("inception");

  // 加载基金信息和净值
  useEffect(() => {
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const [fundRes, navRes] = await Promise.all([
          fetchApi<FundDetail>(`/funds/${fundId}`),
          fetchApi<NavResponse>(`/funds/${fundId}/nav`),
        ]);
        setFund(fundRes);
        setNavData(navRes);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "加载失败");
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [fundId]);

  // 加载指标（依赖区间）
  useEffect(() => {
    async function loadMetrics() {
      try {
        const m = await fetchApi<Metrics>(`/funds/${fundId}/metrics?preset=${interval}`);
        setMetrics(m);
      } catch {
        // 指标可能不可用，静默处理
      }
    }
    loadMetrics();
  }, [fundId, interval]);

  // 图表数据
  const navSeries = useMemo(() => {
    if (!navData?.records?.length) return [];
    return navData.records.map(r => ({ date: r.nav_date, nav: r.unit_nav }));
  }, [navData]);

  const drawdownSeries = useMemo(() => calcDrawdown(navSeries), [navSeries]);

  // 净值走势图配置
  const navChartOption = useMemo(() => {
    if (!navSeries.length) return {};
    return {
      tooltip: {
        trigger: "axis",
        formatter: (params: Array<{ axisValue: string; value: number; seriesName: string }>) => {
          const p = params[0];
          return `${p.axisValue}<br/>净值: <b>${p.value.toFixed(4)}</b>`;
        },
      },
      grid: { left: 50, right: 20, top: 10, bottom: 30 },
      xAxis: {
        type: "category",
        data: navSeries.map(d => d.date),
        axisLabel: { fontSize: 10, color: "#999" },
        axisLine: { lineStyle: { color: "#e5e7eb" } },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: { fontSize: 10, color: "#999", formatter: (v: number) => v.toFixed(2) },
        splitLine: { lineStyle: { color: "#f3f4f6" } },
      },
      series: [{
        type: "line",
        data: navSeries.map(d => d.nav),
        smooth: true,
        symbol: "none",
        lineStyle: { width: 2, color: "#4f46e5" },
        areaStyle: {
          color: {
            type: "linear", x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: "rgba(79,70,229,0.15)" },
              { offset: 1, color: "rgba(79,70,229,0.01)" },
            ],
          },
        },
      }],
    };
  }, [navSeries]);

  // 回撤图配置
  const ddChartOption = useMemo(() => {
    if (!drawdownSeries.length) return {};
    return {
      tooltip: {
        trigger: "axis",
        formatter: (params: Array<{ axisValue: string; value: number }>) => {
          const p = params[0];
          return `${p.axisValue}<br/>回撤: <b>${(p.value * 100).toFixed(2)}%</b>`;
        },
      },
      grid: { left: 50, right: 20, top: 10, bottom: 30 },
      xAxis: {
        type: "category",
        data: drawdownSeries.map(d => d.date),
        axisLabel: { fontSize: 10, color: "#999" },
        axisLine: { lineStyle: { color: "#e5e7eb" } },
      },
      yAxis: {
        type: "value",
        max: 0,
        axisLabel: { fontSize: 10, color: "#999", formatter: (v: number) => `${(v * 100).toFixed(0)}%` },
        splitLine: { lineStyle: { color: "#f3f4f6" } },
      },
      series: [{
        type: "line",
        data: drawdownSeries.map(d => d.dd),
        smooth: true,
        symbol: "none",
        lineStyle: { width: 1.5, color: "#ef4444" },
        areaStyle: {
          color: {
            type: "linear", x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: "rgba(239,68,68,0.01)" },
              { offset: 1, color: "rgba(239,68,68,0.15)" },
            ],
          },
        },
      }],
    };
  }, [drawdownSeries]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (error || !fund) {
    return (
      <div className="space-y-3">
        <Button variant="ghost" size="sm" className="gap-1 text-[12px]" onClick={() => router.back()}>
          <ArrowLeft className="h-3.5 w-3.5" />返回
        </Button>
        <div className="bg-destructive/10 border border-destructive/20 rounded px-4 py-3 text-[12px] text-destructive">
          {error || "基金不存在"}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {/* 顶部导航 */}
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="sm" className="gap-1 text-[12px] h-7" onClick={() => router.back()}>
          <ArrowLeft className="h-3.5 w-3.5" />返回列表
        </Button>
        <div className="flex-1" />
        <Button
          variant="outline"
          size="sm"
          className="gap-1.5 text-[12px] h-7 border-primary/30 text-primary hover:bg-primary/5"
          onClick={() => router.push(`/fund-research/portfolio/create?addFund=${fundId}&fundName=${encodeURIComponent(fund.fund_name)}&freq=${fund.nav_frequency}`)}
        >
          <FlaskConical className="h-3.5 w-3.5" />加入组合回测
        </Button>
      </div>

      <PageHeader
        title={fund.fund_name}
        description={[
          fund.strategy_type,
          fund.strategy_sub,
          fund.nav_frequency === "daily" ? "日频" : "周频",
        ].filter(Boolean).join(" · ")}
      />

      {/* 基金基本信息条 */}
      <div className="bg-card border border-border rounded px-4 py-2.5 flex flex-wrap gap-x-6 gap-y-1 text-[12px]">
        <div><span className="text-muted-foreground">备案号:</span> <span className="tabular-nums">{fund.filing_number || "—"}</span></div>
        <div><span className="text-muted-foreground">管理人:</span> {fund.manager_name || "—"}</div>
        <div><span className="text-muted-foreground">成立日期:</span> <span className="tabular-nums">{fund.inception_date || "—"}</span></div>
        <div><span className="text-muted-foreground">最新净值:</span> <span className="tabular-nums font-medium">{fund.latest_nav != null ? Number(fund.latest_nav).toFixed(4) : "—"}</span></div>
        <div><span className="text-muted-foreground">净值日期:</span> <span className="tabular-nums">{fund.latest_nav_date || "—"}</span></div>
        <div><span className="text-muted-foreground">数据源:</span> {fund.data_source}</div>
        <div>
          <span className="text-muted-foreground">频率:</span>{" "}
          <span className={`inline-block px-1.5 rounded text-[10px] leading-relaxed ${
            fund.nav_frequency === "daily" ? "bg-blue-50 text-blue-600" : "bg-amber-50 text-amber-600"
          }`}>
            {fund.nav_frequency === "daily" ? "日频" : "周频"}
          </span>
        </div>
      </div>

      {/* 区间选择器 */}
      <div className="flex gap-1">
        {intervals.map(iv => (
          <button
            key={iv.key}
            onClick={() => setInterval(iv.key)}
            className={`px-2.5 py-1 rounded text-[11px] transition-colors ${
              interval === iv.key
                ? "bg-primary text-primary-foreground"
                : "bg-muted text-muted-foreground hover:bg-muted/80"
            }`}
          >
            {iv.label}
          </button>
        ))}
      </div>

      {/* 指标卡片 */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2">
        <MetricCard
          label="区间收益"
          value={pct(metrics?.total_return)}
          positive={metrics?.total_return != null ? metrics.total_return >= 0 : undefined}
        />
        <MetricCard
          label="年化收益"
          value={pct(metrics?.annualized_return)}
          positive={metrics?.annualized_return != null ? metrics.annualized_return >= 0 : undefined}
        />
        <MetricCard
          label="最大回撤"
          value={pct(metrics?.max_drawdown)}
          positive={false}
          sub={metrics?.max_dd_peak && metrics?.max_dd_trough ? `${metrics.max_dd_peak} ~ ${metrics.max_dd_trough}` : undefined}
        />
        <MetricCard label="年化波动率" value={pct(metrics?.annualized_volatility)} />
        <MetricCard label="夏普比率" value={num(metrics?.sharpe_ratio)} />
        <MetricCard label="Calmar比率" value={num(metrics?.calmar_ratio)} />
      </div>

      {/* 净值走势图 */}
      <div className="bg-card border border-border rounded">
        <div className="flex items-center justify-between px-4 py-2 border-b border-border">
          <span className="text-[13px] font-medium flex items-center gap-1.5">
            <TrendingUp className="h-3.5 w-3.5 text-primary opacity-60" />
            净值走势
          </span>
          <span className="text-[11px] text-muted-foreground tabular-nums">
            {navData?.total_count ?? 0} 条数据
          </span>
        </div>
        {navSeries.length > 0 ? (
          <ReactECharts option={navChartOption} style={{ height: 280 }} notMerge />
        ) : (
          <div className="h-48 flex items-center justify-center text-muted-foreground">
            <div className="text-center space-y-1">
              <BarChart3 className="mx-auto h-7 w-7 opacity-25" />
              <p className="text-[12px] opacity-60">暂无净值数据</p>
            </div>
          </div>
        )}
      </div>

      {/* 回撤图 */}
      <div className="bg-card border border-border rounded">
        <div className="flex items-center px-4 py-2 border-b border-border">
          <span className="text-[13px] font-medium flex items-center gap-1.5">
            <TrendingDown className="h-3.5 w-3.5 text-destructive opacity-60" />
            历史回撤
          </span>
        </div>
        {drawdownSeries.length > 0 ? (
          <ReactECharts option={ddChartOption} style={{ height: 180 }} notMerge />
        ) : (
          <div className="h-32 flex items-center justify-center text-muted-foreground">
            <p className="text-[12px] opacity-60">暂无数据</p>
          </div>
        )}
      </div>

      {/* 净值数据表（最近20条） */}
      {navData && navData.records.length > 0 && (
        <div className="bg-card border border-border rounded">
          <div className="flex items-center justify-between px-4 py-2 border-b border-border">
            <span className="text-[13px] font-medium flex items-center gap-1.5">
              <Calendar className="h-3.5 w-3.5 text-muted-foreground opacity-60" />
              净值明细
            </span>
            <span className="text-[11px] text-muted-foreground">最近 {Math.min(20, navData.records.length)} 条</span>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-[12px]">
              <thead>
                <tr className="bg-muted/40 text-[11px] text-muted-foreground">
                  <th className="px-4 py-1.5 text-left font-normal">日期</th>
                  <th className="px-4 py-1.5 text-right font-normal">单位净值</th>
                  <th className="px-4 py-1.5 text-right font-normal">累计净值</th>
                  <th className="px-4 py-1.5 text-right font-normal">日收益率</th>
                </tr>
              </thead>
              <tbody>
                {navData.records.slice(-20).reverse().map(r => (
                  <tr key={r.nav_date} className="border-t border-border hover:bg-muted/20">
                    <td className="px-4 py-1.5 tabular-nums">{r.nav_date}</td>
                    <td className="px-4 py-1.5 text-right tabular-nums font-medium">{r.unit_nav.toFixed(4)}</td>
                    <td className="px-4 py-1.5 text-right tabular-nums">{r.cumulative_nav?.toFixed(4) ?? "—"}</td>
                    <td className={`px-4 py-1.5 text-right tabular-nums ${
                      r.daily_return != null ? (r.daily_return >= 0 ? "text-jf-up" : "text-jf-down") : ""
                    }`}>
                      {r.daily_return != null ? `${(r.daily_return * 100).toFixed(2)}%` : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
