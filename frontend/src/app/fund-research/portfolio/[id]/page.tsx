"use client";

import { useEffect, useState, useMemo, useCallback } from "react";
import { useParams, useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import {
  ArrowLeft, Loader2, Play, Trash2, Clock,
  TrendingUp, TrendingDown, PieChart, Settings2,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { fetchApi } from "@/lib/api";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

/* --- Types --- */

interface PortfolioWeight {
  fund_id: number | null;
  index_code: string | null;
  fund_name: string;
  strategy_type: string | null;
  nav_frequency: string | null;
  weight: number; // 0-1
}

interface PortfolioDetail {
  id: number;
  name: string;
  description: string | null;
  portfolio_type: string;
  allocation_model: string | null;
  rebalance_freq: string;
  created_at: string | null;
  weights: PortfolioWeight[];
}

interface BacktestHistoryItem {
  backtest_id: number;
  status: string;
  metrics: Record<string, number | null> | null;
  run_date: string | null;
}

interface BacktestFullResult {
  backtest_id: number | null;
  portfolio_id: number | null;
  status: string;
  config: Record<string, unknown> | null;
  metrics: Record<string, number | null> | null;
  nav_series: { date: string; nav: number }[];
  drawdown_series: { date: string; drawdown: number }[];
  monthly_returns: { year: number; month: number; return_pct: number }[];
  run_date: string | null;
}

/* --- Helpers --- */

function pct(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return "--";
  return `${(v * 100).toFixed(2)}%`;
}

function num(v: number | null | undefined, dp = 2): string {
  if (v == null || !isFinite(v)) return "--";
  return Math.max(Math.min(v, 999.99), -999.99).toFixed(dp);
}

const rebalanceLabel: Record<string, string> = {
  daily: "每日", weekly: "每周", monthly: "每月", quarterly: "每季度",
};

const historyModes = [
  { key: "intersection", label: "严格交集" },
  { key: "dynamic_entry", label: "动态入场" },
  { key: "truncate", label: "截断排除" },
] as const;

/* --- Page --- */

export default function PortfolioDetailPage() {
  const params = useParams();
  const router = useRouter();
  const portfolioId = params.id as string;

  const [portfolio, setPortfolio] = useState<PortfolioDetail | null>(null);
  const [history, setHistory] = useState<BacktestHistoryItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Backtest execution
  const [backtesting, setBacktesting] = useState(false);
  const [btError, setBtError] = useState<string | null>(null);

  // View backtest result
  const [viewingResult, setViewingResult] = useState<BacktestFullResult | null>(null);
  const [viewLoading, setViewLoading] = useState(false);

  // Backtest config panel
  const [showConfig, setShowConfig] = useState(false);
  const [startDate, setStartDate] = useState("2024-01-01");
  const [endDate, setEndDate] = useState("2026-03-01");
  const [rebalanceFreq, setRebalanceFreq] = useState("monthly");
  const [historyMode, setHistoryMode] = useState("intersection");
  const [costBps, setCostBps] = useState(0);
  const [riskFreeRate, setRiskFreeRate] = useState(0.02);

  // Delete
  const [deleting, setDeleting] = useState(false);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [p, h] = await Promise.all([
        fetchApi<PortfolioDetail>(`/portfolios/${portfolioId}`),
        fetchApi<BacktestHistoryItem[]>(`/backtest/history/${portfolioId}`),
      ]);
      setPortfolio(p);
      setHistory(h);
      setRebalanceFreq(p.rebalance_freq);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "加载失败");
    } finally {
      setLoading(false);
    }
  }, [portfolioId]);

  useEffect(() => { loadData(); }, [loadData]);

  // Auto-load latest backtest result
  useEffect(() => {
    if (history.length > 0 && !viewingResult) {
      handleViewResult(history[0].backtest_id);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [history]);

  async function handleViewResult(backtestId: number) {
    setViewLoading(true);
    try {
      const res = await fetchApi<BacktestFullResult>(`/backtest/results/${backtestId}`);
      setViewingResult(res);
    } catch {
      // ignore
    } finally {
      setViewLoading(false);
    }
  }

  async function handleRunBacktest() {
    setBacktesting(true);
    setBtError(null);
    try {
      const result = await fetchApi<{ backtest_id: number }>("/backtest/run", {
        method: "POST",
        body: JSON.stringify({
          portfolio_id: parseInt(portfolioId),
          start_date: startDate,
          end_date: endDate,
          rebalance_frequency: rebalanceFreq,
          transaction_cost_bps: costBps,
          risk_free_rate: riskFreeRate,
          history_mode: historyMode,
          initial_capital: 10000000,
        }),
      });
      setShowConfig(false);
      // Reload history and view the new result
      const h = await fetchApi<BacktestHistoryItem[]>(`/backtest/history/${portfolioId}`);
      setHistory(h);
      if (result.backtest_id) {
        await handleViewResult(result.backtest_id);
      }
    } catch (e: unknown) {
      setBtError(e instanceof Error ? e.message : "回测失败");
    } finally {
      setBacktesting(false);
    }
  }

  async function handleDelete() {
    if (!confirm("确定删除此组合？此操作不可恢复。")) return;
    setDeleting(true);
    try {
      await fetchApi(`/portfolios/${portfolioId}`, { method: "DELETE" });
      router.push("/fund-research/portfolio");
    } catch {
      setDeleting(false);
    }
  }

  // Pie chart for weight allocation
  const pieOption = useMemo(() => {
    if (!portfolio?.weights?.length) return {};
    return {
      tooltip: { trigger: "item", formatter: "{b}: {d}%" },
      series: [{
        type: "pie",
        radius: ["40%", "70%"],
        avoidLabelOverlap: true,
        itemStyle: { borderRadius: 4, borderColor: "#fff", borderWidth: 2 },
        label: { show: true, fontSize: 10, formatter: "{b}\n{d}%" },
        data: portfolio.weights.map((w, i) => ({
          name: w.fund_name,
          value: parseFloat((w.weight * 100).toFixed(2)),
          itemStyle: { color: ["#4f46e5", "#06b6d4", "#f59e0b", "#ef4444", "#8b5cf6", "#10b981", "#ec4899", "#6366f1"][i % 8] },
        })),
      }],
    };
  }, [portfolio]);

  // NAV chart
  const navChartOption = useMemo(() => {
    if (!viewingResult?.nav_series?.length) return {};
    return {
      tooltip: { trigger: "axis" },
      grid: { left: 50, right: 20, top: 10, bottom: 30 },
      xAxis: { type: "category", data: viewingResult.nav_series.map(d => d.date), axisLabel: { fontSize: 10, color: "#999" } },
      yAxis: { type: "value", scale: true, axisLabel: { fontSize: 10, color: "#999" } },
      series: [{ type: "line", data: viewingResult.nav_series.map(d => d.nav), smooth: true, symbol: "none", lineStyle: { width: 2, color: "#4f46e5" }, areaStyle: { color: { type: "linear", x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: "rgba(79,70,229,0.12)" }, { offset: 1, color: "rgba(79,70,229,0.01)" }] } } }],
    };
  }, [viewingResult]);

  // Drawdown chart
  const ddChartOption = useMemo(() => {
    if (!viewingResult?.drawdown_series?.length) return {};
    return {
      tooltip: { trigger: "axis" },
      grid: { left: 50, right: 20, top: 10, bottom: 30 },
      xAxis: { type: "category", data: viewingResult.drawdown_series.map(d => d.date), axisLabel: { fontSize: 10, color: "#999" } },
      yAxis: { type: "value", max: 0, axisLabel: { fontSize: 10, color: "#999", formatter: (v: number) => `${(v * 100).toFixed(0)}%` } },
      series: [{ type: "line", data: viewingResult.drawdown_series.map(d => d.drawdown), smooth: true, symbol: "none", lineStyle: { width: 1.5, color: "#ef4444" }, areaStyle: { color: { type: "linear", x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: "rgba(239,68,68,0.01)" }, { offset: 1, color: "rgba(239,68,68,0.12)" }] } } }],
    };
  }, [viewingResult]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (error || !portfolio) {
    return (
      <div className="space-y-3">
        <Button variant="ghost" size="sm" className="gap-1 text-[12px]" onClick={() => router.back()}>
          <ArrowLeft className="h-3.5 w-3.5" />返回
        </Button>
        <div className="bg-destructive/10 border border-destructive/20 rounded px-4 py-3 text-[12px] text-destructive">
          {error || "组合不存在"}
        </div>
      </div>
    );
  }

  const m = viewingResult?.metrics;

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="sm" className="gap-1 text-[12px] h-7" onClick={() => router.push("/fund-research/portfolio")}>
          <ArrowLeft className="h-3.5 w-3.5" />返回列表
        </Button>
        <div className="flex-1" />
        <Button
          variant="outline"
          size="sm"
          className="gap-1 text-[12px] h-7"
          onClick={() => setShowConfig(!showConfig)}
        >
          <Settings2 className="h-3.5 w-3.5" />运行回测
        </Button>
        <Button
          variant="outline"
          size="sm"
          className="gap-1 text-[12px] h-7 text-destructive border-destructive/30 hover:bg-destructive/5"
          onClick={handleDelete}
          disabled={deleting}
        >
          <Trash2 className="h-3.5 w-3.5" />删除
        </Button>
      </div>

      <PageHeader
        title={portfolio.name}
        description={[
          portfolio.portfolio_type === "live" ? "实盘" : "模拟",
          rebalanceLabel[portfolio.rebalance_freq] || portfolio.rebalance_freq,
          portfolio.created_at ? `创建于 ${portfolio.created_at.slice(0, 10)}` : "",
        ].filter(Boolean).join(" | ")}
      />

      {/* Backtest config panel */}
      {showConfig && (
        <div className="bg-card border border-border rounded px-5 py-4 space-y-3">
          <div className="text-[13px] font-medium mb-2">回测参数</div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div>
              <label className="text-[11px] text-muted-foreground block mb-1">开始日期</label>
              <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)}
                className="h-8 px-2 text-[12px] border border-border rounded bg-background w-full tabular-nums" />
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground block mb-1">结束日期</label>
              <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)}
                className="h-8 px-2 text-[12px] border border-border rounded bg-background w-full tabular-nums" />
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground block mb-1">交易成本 (bps)</label>
              <input type="number" min={0} max={100} step={1} value={costBps}
                onChange={e => setCostBps(parseFloat(e.target.value) || 0)}
                className="h-8 px-2 text-[12px] border border-border rounded bg-background w-full tabular-nums" />
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground block mb-1">无风险利率 (%)</label>
              <input type="number" min={0} max={10} step={0.1} value={(riskFreeRate * 100).toFixed(1)}
                onChange={e => setRiskFreeRate((parseFloat(e.target.value) || 0) / 100)}
                className="h-8 px-2 text-[12px] border border-border rounded bg-background w-full tabular-nums" />
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex gap-1">
              {historyModes.map(hm => (
                <button key={hm.key} onClick={() => setHistoryMode(hm.key)}
                  className={`px-2.5 py-1 rounded text-[11px] border transition-colors ${
                    historyMode === hm.key
                      ? "bg-primary text-primary-foreground border-primary"
                      : "bg-background text-muted-foreground border-border hover:border-primary/50"
                  }`}
                >
                  {hm.label}
                </button>
              ))}
            </div>
            <div className="flex-1" />
            <Button size="sm" className="h-8 text-[12px] gap-1 px-5" onClick={handleRunBacktest} disabled={backtesting}>
              {backtesting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
              {backtesting ? "运行中..." : "执行回测"}
            </Button>
          </div>
          {btError && (
            <div className="bg-destructive/10 border border-destructive/20 rounded px-3 py-2 text-[12px] text-destructive">
              {btError}
            </div>
          )}
        </div>
      )}

      {/* Two-column: Weight table + Pie chart */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Weight table */}
        <div className="lg:col-span-2 bg-card border border-border rounded overflow-hidden">
          <div className="px-4 py-2 border-b border-border text-[13px] font-medium flex items-center gap-1.5">
            <PieChart className="h-3.5 w-3.5 text-primary opacity-60" />
            权重配置
            <span className="text-[11px] text-muted-foreground ml-auto">{portfolio.weights.length} 个资产</span>
          </div>
          <Table>
            <TableHeader>
              <TableRow className="bg-muted/40">
                <TableHead className="h-7 text-[11px] font-normal w-8 text-center">#</TableHead>
                <TableHead className="h-7 text-[11px] font-normal">资产名称</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-center">类型</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-center">频率</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-right">权重</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {portfolio.weights.map((w, i) => (
                <TableRow key={`${w.fund_id}-${w.index_code}-${i}`} className="text-[12px]">
                  <TableCell className="py-1.5 text-center text-muted-foreground">{i + 1}</TableCell>
                  <TableCell className="py-1.5 font-medium">{w.fund_name}</TableCell>
                  <TableCell className="py-1.5 text-center">
                    <span className={`px-1.5 rounded text-[10px] leading-relaxed ${
                      w.index_code ? "bg-purple-50 text-purple-600" : "bg-slate-50 text-slate-600"
                    }`}>
                      {w.index_code ? "指数" : "基金"}
                    </span>
                  </TableCell>
                  <TableCell className="py-1.5 text-center">
                    {w.nav_frequency ? (
                      <span className={`px-1.5 rounded text-[10px] leading-relaxed ${
                        w.nav_frequency === "daily" ? "bg-blue-50 text-blue-600" : "bg-amber-50 text-amber-600"
                      }`}>
                        {w.nav_frequency === "daily" ? "日频" : "周频"}
                      </span>
                    ) : (
                      <span className="px-1.5 rounded text-[10px] leading-relaxed bg-blue-50 text-blue-600">日频</span>
                    )}
                  </TableCell>
                  <TableCell className="py-1.5 text-right tabular-nums font-semibold">
                    {(w.weight * 100).toFixed(2)}%
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        {/* Pie chart */}
        <div className="bg-card border border-border rounded">
          <div className="px-4 py-2 border-b border-border text-[13px] font-medium">配置分布</div>
          {portfolio.weights.length > 0 ? (
            <ReactECharts option={pieOption} style={{ height: 240 }} notMerge />
          ) : (
            <div className="h-48 flex items-center justify-center text-muted-foreground text-[12px]">无配置</div>
          )}
        </div>
      </div>

      {/* Backtest result display */}
      {viewLoading && (
        <div className="h-32 flex items-center justify-center">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        </div>
      )}

      {viewingResult && !viewLoading && (
        <div className="space-y-4">
          {/* Section header */}
          <div className="flex items-center gap-2">
            <TrendingUp className="h-4 w-4 text-primary" />
            <span className="text-[14px] font-semibold">回测结果</span>
            {viewingResult.run_date && (
              <span className="text-[11px] text-muted-foreground ml-2">
                {viewingResult.run_date.slice(0, 10)}
              </span>
            )}
          </div>

          {/* Metrics cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-2">
            {[
              { label: "总收益", value: pct(m?.total_return) },
              { label: "年化收益", value: pct(m?.annualized_return) },
              { label: "最大回撤", value: pct(m?.max_drawdown) },
              { label: "年化波动率", value: pct(m?.annualized_volatility ?? m?.volatility) },
              { label: "夏普比率", value: num(m?.sharpe_ratio) },
              { label: "Sortino", value: num(m?.sortino_ratio) },
              { label: "Calmar", value: num(m?.calmar_ratio) },
            ].map(metric => (
              <div key={metric.label} className="bg-card border border-border rounded px-3 py-2.5">
                <div className="text-[10px] text-muted-foreground mb-0.5">{metric.label}</div>
                <div className="text-[15px] font-semibold tabular-nums">{metric.value}</div>
              </div>
            ))}
          </div>

          {/* NAV chart */}
          <div className="bg-card border border-border rounded">
            <div className="px-4 py-2 border-b border-border text-[13px] font-medium flex items-center gap-1.5">
              <TrendingUp className="h-3.5 w-3.5 text-primary opacity-60" />
              净值走势
              <span className="text-[11px] text-muted-foreground ml-auto tabular-nums">
                {viewingResult.nav_series.length} 个交易日
              </span>
            </div>
            <ReactECharts option={navChartOption} style={{ height: 280 }} notMerge />
          </div>

          {/* Drawdown chart */}
          <div className="bg-card border border-border rounded">
            <div className="px-4 py-2 border-b border-border text-[13px] font-medium flex items-center gap-1.5">
              <TrendingDown className="h-3.5 w-3.5 text-destructive opacity-60" />
              历史回撤
            </div>
            <ReactECharts option={ddChartOption} style={{ height: 180 }} notMerge />
          </div>

          {/* Monthly returns */}
          {viewingResult.monthly_returns.length > 0 && (
            <div className="bg-card border border-border rounded">
              <div className="px-4 py-2 border-b border-border text-[13px] font-medium">月度收益</div>
              <div className="overflow-x-auto">
                <table className="w-full text-[11px]">
                  <thead>
                    <tr className="bg-muted/40">
                      <th className="px-2 py-1.5 text-left font-normal">年份</th>
                      {Array.from({ length: 12 }, (_, i) => (
                        <th key={i} className="px-2 py-1.5 text-right font-normal">{i + 1}月</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {Array.from(new Set(viewingResult.monthly_returns.map(r => r.year))).sort().map(year => (
                      <tr key={year} className="border-t border-border">
                        <td className="px-2 py-1 font-medium">{year}</td>
                        {Array.from({ length: 12 }, (_, i) => {
                          const item = viewingResult.monthly_returns.find(r => r.year === year && r.month === i + 1);
                          return (
                            <td key={i} className={`px-2 py-1 text-right tabular-nums ${
                              item ? (item.return_pct >= 0 ? "text-jf-up" : "text-jf-down") : "text-muted-foreground"
                            }`}>
                              {item ? `${item.return_pct > 0 ? "+" : ""}${item.return_pct.toFixed(2)}%` : "--"}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {/* No backtest yet */}
      {!viewingResult && !viewLoading && history.length === 0 && (
        <div className="bg-card border border-border rounded py-12 flex flex-col items-center gap-3 text-muted-foreground">
          <Play className="h-8 w-8 opacity-20" />
          <p className="text-[13px]">暂无回测记录</p>
          <p className="text-[11px] opacity-60">点击上方"运行回测"开始模拟</p>
        </div>
      )}

      {/* Backtest history */}
      {history.length > 1 && (
        <div className="bg-card border border-border rounded overflow-hidden">
          <div className="px-4 py-2 border-b border-border text-[13px] font-medium flex items-center gap-1.5">
            <Clock className="h-3.5 w-3.5 text-muted-foreground opacity-60" />
            历史回测记录
          </div>
          <Table>
            <TableHeader>
              <TableRow className="bg-muted/40">
                <TableHead className="h-7 text-[11px] font-normal">运行时间</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-right">总收益</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-right">年化收益</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-right">最大回撤</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-right">夏普</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-center">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {history.map(h => (
                <TableRow
                  key={h.backtest_id}
                  className={`text-[12px] cursor-pointer transition-colors ${
                    viewingResult?.backtest_id === h.backtest_id ? "bg-primary/5" : "hover:bg-muted/20"
                  }`}
                  onClick={() => handleViewResult(h.backtest_id)}
                >
                  <TableCell className="py-1.5 tabular-nums text-muted-foreground text-[11px]">
                    {h.run_date ? h.run_date.slice(0, 16).replace("T", " ") : "--"}
                  </TableCell>
                  <TableCell className="py-1.5 text-right tabular-nums">
                    {pct(h.metrics?.total_return)}
                  </TableCell>
                  <TableCell className="py-1.5 text-right tabular-nums">
                    {pct(h.metrics?.annualized_return)}
                  </TableCell>
                  <TableCell className="py-1.5 text-right tabular-nums">
                    {pct(h.metrics?.max_drawdown)}
                  </TableCell>
                  <TableCell className="py-1.5 text-right tabular-nums">
                    {num(h.metrics?.sharpe_ratio)}
                  </TableCell>
                  <TableCell className="py-1.5 text-center">
                    <button className="text-[11px] text-primary hover:underline">
                      {viewingResult?.backtest_id === h.backtest_id ? "当前查看" : "查看"}
                    </button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
