"use client";

import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import {
  Search, Plus, Trash2, Loader2, Play, PieChart, X,
  ChevronRight, AlertTriangle,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { fetchApi } from "@/lib/api";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

/* --- Types --- */

interface FundSearchItem {
  id: number;
  fund_name: string;
  strategy_type: string | null;
  nav_frequency: string;
}

interface WeightItem {
  fund_id: number;
  fund_name: string;
  nav_frequency: string;
  weight: number;
}

interface PortfolioItem {
  id: number;
  name: string;
  description: string | null;
  portfolio_type: string;
  allocation_model: string | null;
  rebalance_freq: string;
  fund_count: number;
  created_at: string | null;
}

interface BacktestResult {
  backtest_id: number;
  history_mode: string;
  metrics: {
    total_return: number | null;
    annualized_return: number | null;
    max_drawdown: number | null;
    sharpe_ratio: number | null;
    sortino_ratio: number | null;
    calmar_ratio: number | null;
    volatility: number | null;
  };
  nav_series: { date: string; nav: number }[];
  drawdown_series: { date: string; drawdown: number }[];
  monthly_returns: { year: number; month: number; return_pct: number }[];
  fund_names: Record<string, string>;
  excluded_funds: string[];
  entry_log: { date: string; funds_entered: string[] }[];
}

const historyModes = [
  { key: "intersection", label: "严格交集", desc: "所有基金必须有完整数据" },
  { key: "dynamic_entry", label: "动态入场", desc: "基金数据出现时自动纳入" },
  { key: "truncate", label: "截断排除", desc: "排除数据不足的基金" },
] as const;

/* --- Helpers --- */

function pct(v: number | null | undefined): string {
  if (v == null) return "--";
  return `${(v * 100).toFixed(2)}%`;
}

function num(v: number | null | undefined, dp = 2): string {
  if (v == null) return "--";
  return v.toFixed(dp);
}

function FreqBadge({ freq }: { freq: string }) {
  const isDaily = freq === "daily";
  return (
    <span className={`px-1.5 rounded text-[10px] leading-relaxed ${
      isDaily ? "bg-blue-50 text-blue-600" : "bg-amber-50 text-amber-600"
    }`}>
      {isDaily ? "日频" : "周频"}
    </span>
  );
}

/* --- Page --- */

type ViewMode = "list" | "create" | "result";

export default function PortfolioPage() {
  const router = useRouter();
  const [viewMode, setViewMode] = useState<ViewMode>("list");

  // 组合列表
  const [portfolios, setPortfolios] = useState<PortfolioItem[]>([]);
  const [listLoading, setListLoading] = useState(true);

  // 创建组合
  const [portfolioName, setPortfolioName] = useState("");
  const [rebalanceFreq, setRebalanceFreq] = useState("monthly");
  const [weights, setWeights] = useState<WeightItem[]>([]);
  const [search, setSearch] = useState("");
  const [searchResults, setSearchResults] = useState<FundSearchItem[]>([]);
  const [searching, setSearching] = useState(false);

  // 回测
  const [startDate, setStartDate] = useState("2024-01-01");
  const [endDate, setEndDate] = useState("2026-03-01");
  const [historyMode, setHistoryMode] = useState("intersection");
  const [backtesting, setBacktesting] = useState(false);
  const [btResult, setBtResult] = useState<BacktestResult | null>(null);
  const [btError, setBtError] = useState<string | null>(null);

  // 加载组合列表
  const loadPortfolios = useCallback(async () => {
    setListLoading(true);
    try {
      const res = await fetchApi<{ items: PortfolioItem[] }>("/portfolios/");
      setPortfolios(res.items);
    } catch {
      setPortfolios([]);
    } finally {
      setListLoading(false);
    }
  }, []);

  useEffect(() => {
    loadPortfolios();
  }, [loadPortfolios]);

  // 搜索基金
  useEffect(() => {
    if (!search || search.length < 2) {
      setSearchResults([]);
      return;
    }
    const timer = setTimeout(async () => {
      setSearching(true);
      try {
        const res = await fetchApi<{ items: FundSearchItem[] }>(
          `/funds/?search=${encodeURIComponent(search)}&page_size=8`
        );
        setSearchResults(
          res.items.filter(f => !weights.some(w => w.fund_id === f.id))
        );
      } catch {
        setSearchResults([]);
      } finally {
        setSearching(false);
      }
    }, 300);
    return () => clearTimeout(timer);
  }, [search, weights]);

  function addFund(fund: FundSearchItem) {
    setWeights(prev => [
      ...prev,
      {
        fund_id: fund.id,
        fund_name: fund.fund_name,
        nav_frequency: fund.nav_frequency,
        weight: 0,
      },
    ]);
    setSearch("");
    setSearchResults([]);
  }

  function removeFund(fundId: number) {
    setWeights(prev => prev.filter(w => w.fund_id !== fundId));
  }

  function setEqualWeights() {
    if (weights.length === 0) return;
    const w = 1.0 / weights.length;
    setWeights(prev => prev.map(item => ({ ...item, weight: parseFloat(w.toFixed(6)) })));
  }

  function updateWeight(fundId: number, value: number) {
    setWeights(prev =>
      prev.map(w => w.fund_id === fundId ? { ...w, weight: value } : w)
    );
  }

  const totalWeight = weights.reduce((sum, w) => sum + w.weight, 0);
  const hasMixedFreq = weights.length > 0 &&
    new Set(weights.map(w => w.nav_frequency)).size > 1;

  // 创建组合并回测
  async function handleBacktest() {
    if (weights.length < 2) return;
    if (Math.abs(totalWeight - 1.0) > 0.01) return;

    setBacktesting(true);
    setBtError(null);
    setBtResult(null);

    try {
      const result = await fetchApi<BacktestResult>("/backtest/run", {
        method: "POST",
        body: JSON.stringify({
          weights: weights.map(w => ({ fund_id: w.fund_id, weight: w.weight })),
          start_date: startDate,
          end_date: endDate,
          rebalance_frequency: rebalanceFreq,
          history_mode: historyMode,
          initial_capital: 10000000,
        }),
      });
      setBtResult(result);
      setViewMode("result");
    } catch (e: unknown) {
      setBtError(e instanceof Error ? e.message : "回测失败");
    } finally {
      setBacktesting(false);
    }
  }

  // 保存组合
  async function handleSave() {
    if (!portfolioName || weights.length < 2) return;
    try {
      await fetchApi("/portfolios/", {
        method: "POST",
        body: JSON.stringify({
          name: portfolioName,
          weights: weights.map(w => ({ fund_id: w.fund_id, weight: w.weight })),
          rebalance_frequency: rebalanceFreq,
        }),
      });
      loadPortfolios();
      setViewMode("list");
      setPortfolioName("");
      setWeights([]);
    } catch (e: unknown) {
      setBtError(e instanceof Error ? e.message : "保存失败");
    }
  }

  // NAV图表
  const navChartOption = btResult ? {
    tooltip: { trigger: "axis" },
    grid: { left: 50, right: 20, top: 10, bottom: 30 },
    xAxis: {
      type: "category",
      data: btResult.nav_series.map(p => p.date),
      axisLabel: { fontSize: 10, color: "#999" },
    },
    yAxis: {
      type: "value",
      scale: true,
      axisLabel: { fontSize: 10, color: "#999" },
      splitLine: { lineStyle: { color: "#f3f4f6" } },
    },
    series: [{
      type: "line",
      data: btResult.nav_series.map(p => p.nav),
      smooth: true,
      symbol: "none",
      lineStyle: { width: 2, color: "#4f46e5" },
      areaStyle: { color: "rgba(79,70,229,0.08)" },
    }],
  } : {};

  // 回撤图表
  const ddChartOption = btResult ? {
    tooltip: { trigger: "axis" },
    grid: { left: 50, right: 20, top: 10, bottom: 30 },
    xAxis: {
      type: "category",
      data: btResult.drawdown_series.map(p => p.date),
      axisLabel: { fontSize: 10, color: "#999" },
    },
    yAxis: {
      type: "value",
      axisLabel: {
        fontSize: 10,
        color: "#999",
        formatter: (v: number) => `${(v * 100).toFixed(1)}%`,
      },
      splitLine: { lineStyle: { color: "#f3f4f6" } },
    },
    series: [{
      type: "line",
      data: btResult.drawdown_series.map(p => p.drawdown),
      smooth: true,
      symbol: "none",
      lineStyle: { width: 1.5, color: "#ef4444" },
      areaStyle: { color: "rgba(239,68,68,0.1)" },
    }],
  } : {};

  /* --- 列表视图 --- */
  if (viewMode === "list") {
    return (
      <div className="space-y-3">
        <PageHeader
          title="组合研究"
          description="构建FOF组合、配置权重、运行回测"
          actions={
            <Button size="sm" className="h-7 text-[12px] gap-1" onClick={() => setViewMode("create")}>
              <Plus className="h-3 w-3" />新建组合
            </Button>
          }
        />

        <div className="bg-card border border-border rounded overflow-hidden">
          {listLoading ? (
            <div className="h-32 flex items-center justify-center">
              <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
            </div>
          ) : portfolios.length === 0 ? (
            <div className="h-32 flex items-center justify-center text-muted-foreground">
              <div className="text-center space-y-2">
                <PieChart className="mx-auto h-7 w-7 opacity-25" />
                <p className="text-[12px]">暂无组合，点击"新建组合"开始</p>
              </div>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow className="bg-muted/40">
                  <TableHead className="h-7 text-[11px] font-normal">组合名称</TableHead>
                  <TableHead className="h-7 text-[11px] font-normal text-center">类型</TableHead>
                  <TableHead className="h-7 text-[11px] font-normal text-center">基金数</TableHead>
                  <TableHead className="h-7 text-[11px] font-normal text-center">再平衡</TableHead>
                  <TableHead className="h-7 text-[11px] font-normal text-center">创建时间</TableHead>
                  <TableHead className="h-7 text-[11px] font-normal text-center">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {portfolios.map(p => (
                  <TableRow key={p.id} className="text-[12px] hover:bg-muted/20">
                    <TableCell className="py-1.5 font-medium">{p.name}</TableCell>
                    <TableCell className="py-1.5 text-center">
                      <span className={`px-1.5 rounded text-[10px] ${
                        p.portfolio_type === "live" ? "bg-green-50 text-green-600" : "bg-blue-50 text-blue-600"
                      }`}>
                        {p.portfolio_type === "live" ? "实盘" : "模拟"}
                      </span>
                    </TableCell>
                    <TableCell className="py-1.5 text-center tabular-nums">{p.fund_count}</TableCell>
                    <TableCell className="py-1.5 text-center text-muted-foreground">{p.rebalance_freq}</TableCell>
                    <TableCell className="py-1.5 text-center tabular-nums text-muted-foreground text-[11px]">
                      {p.created_at ? p.created_at.slice(0, 10) : "--"}
                    </TableCell>
                    <TableCell className="py-1.5 text-center">
                      <button className="text-[11px] text-primary hover:underline inline-flex items-center gap-0.5">
                        详情 <ChevronRight className="h-3 w-3" />
                      </button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </div>
      </div>
    );
  }

  /* --- 回测结果视图 --- */
  if (viewMode === "result" && btResult) {
    const m = btResult.metrics;
    return (
      <div className="space-y-3">
        <PageHeader
          title="回测结果"
          description={`${startDate} ~ ${endDate}`}
          actions={
            <div className="flex gap-1.5">
              <Button variant="outline" size="sm" className="h-7 text-[12px]" onClick={() => setViewMode("create")}>
                返回编辑
              </Button>
              <Button size="sm" className="h-7 text-[12px]" onClick={handleSave} disabled={!portfolioName}>
                保存组合
              </Button>
            </div>
          }
        />

        {/* 指标卡片 */}
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-2">
          {[
            { label: "累计收益", value: pct(m.total_return), up: (m.total_return ?? 0) >= 0 },
            { label: "年化收益", value: pct(m.annualized_return), up: (m.annualized_return ?? 0) >= 0 },
            { label: "最大回撤", value: pct(m.max_drawdown), up: false },
            { label: "年化波动率", value: pct(m.volatility) },
            { label: "夏普比率", value: num(m.sharpe_ratio) },
            { label: "Sortino", value: num(m.sortino_ratio) },
            { label: "Calmar", value: num(m.calmar_ratio) },
          ].map(card => (
            <div key={card.label} className="bg-card border border-border rounded px-3 py-2">
              <div className="text-[10px] text-muted-foreground">{card.label}</div>
              <div className={`text-base font-semibold tabular-nums mt-0.5 ${
                card.up === true ? "text-jf-up" : card.up === false ? "text-jf-down" : ""
              }`}>
                {card.value}
              </div>
            </div>
          ))}
        </div>

        {/* 动态入场日志 / 排除基金提示 */}
        {btResult.entry_log.length > 0 && (
          <div className="bg-blue-50 border border-blue-200 rounded px-3 py-2 text-[11px] text-blue-700 space-y-0.5">
            <div className="font-medium">动态入场记录</div>
            {btResult.entry_log.map((e, i) => (
              <div key={i}>{e.date}: {e.funds_entered.map(f => btResult.fund_names[f] || f).join(", ")} 入场</div>
            ))}
          </div>
        )}
        {btResult.excluded_funds.length > 0 && (
          <div className="bg-amber-50 border border-amber-200 rounded px-3 py-2 text-[11px] text-amber-700">
            <span className="font-medium">因数据不足被排除的基金:</span>{" "}
            {btResult.excluded_funds.join(", ")}
          </div>
        )}

        {/* NAV 走势 */}
        <div className="bg-card border border-border rounded">
          <div className="px-4 py-2 border-b border-border text-[13px] font-medium">组合净值走势</div>
          <ReactECharts option={navChartOption} style={{ height: 280 }} notMerge />
        </div>

        {/* 回撤图 */}
        <div className="bg-card border border-border rounded">
          <div className="px-4 py-2 border-b border-border text-[13px] font-medium">回撤走势</div>
          <ReactECharts option={ddChartOption} style={{ height: 180 }} notMerge />
        </div>

        {/* 月度收益表 */}
        {btResult.monthly_returns.length > 0 && (
          <div className="bg-card border border-border rounded overflow-hidden">
            <div className="px-4 py-2 border-b border-border text-[13px] font-medium">月度收益</div>
            <div className="overflow-x-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="bg-muted/40">
                    <th className="px-2 py-1 text-left font-normal">年份</th>
                    {Array.from({ length: 12 }, (_, i) => (
                      <th key={i} className="px-2 py-1 text-right font-normal">{i + 1}月</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {Array.from(new Set(btResult.monthly_returns.map(r => r.year))).sort().map(year => (
                    <tr key={year} className="border-t border-border">
                      <td className="px-2 py-1 font-medium">{year}</td>
                      {Array.from({ length: 12 }, (_, i) => {
                        const item = btResult.monthly_returns.find(r => r.year === year && r.month === i + 1);
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
    );
  }

  /* --- 创建/编辑视图 --- */
  return (
    <div className="space-y-3">
      <PageHeader
        title="构建组合"
        description="选择基金、配置权重、运行回测"
        actions={
          <Button variant="outline" size="sm" className="h-7 text-[12px]" onClick={() => { setViewMode("list"); setWeights([]); }}>
            返回列表
          </Button>
        }
      />

      {/* 组合基本信息 */}
      <div className="bg-card border border-border rounded px-4 py-3 space-y-2">
        <div className="flex items-center gap-3">
          <div className="flex-1 max-w-xs">
            <label className="text-[11px] text-muted-foreground block mb-0.5">组合名称</label>
            <Input
              placeholder="输入组合名称"
              value={portfolioName}
              onChange={(e) => setPortfolioName(e.target.value)}
              className="h-7 text-[12px]"
            />
          </div>
          <div>
            <label className="text-[11px] text-muted-foreground block mb-0.5">再平衡频率</label>
            <select
              value={rebalanceFreq}
              onChange={(e) => setRebalanceFreq(e.target.value)}
              className="h-7 px-2 text-[12px] border border-border rounded bg-background"
            >
              <option value="daily">每日</option>
              <option value="weekly">每周</option>
              <option value="monthly">每月</option>
              <option value="quarterly">每季度</option>
            </select>
          </div>
        </div>
      </div>

      {/* 添加基金 */}
      <div className="bg-card border border-border rounded px-4 py-3 space-y-2">
        <div className="flex items-center gap-2">
          <div className="relative flex-1 max-w-sm">
            <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="搜索基金添加到组合..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-8 h-7 text-[12px]"
            />
            {searching && <Loader2 className="absolute right-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 animate-spin text-muted-foreground" />}
            {searchResults.length > 0 && (
              <div className="absolute z-50 top-full mt-1 left-0 w-full bg-card border border-border rounded shadow-lg max-h-48 overflow-y-auto">
                {searchResults.map(f => (
                  <button
                    key={f.id}
                    className="w-full text-left px-3 py-1.5 text-[12px] hover:bg-muted/50 flex items-center justify-between"
                    onClick={() => addFund(f)}
                  >
                    <span>{f.fund_name}</span>
                    <span className="flex items-center gap-1.5">
                      <FreqBadge freq={f.nav_frequency} />
                      <Plus className="h-3.5 w-3.5 text-primary" />
                    </span>
                  </button>
                ))}
              </div>
            )}
          </div>
          <Button variant="outline" size="sm" className="h-7 text-[12px]" onClick={setEqualWeights} disabled={weights.length === 0}>
            等权分配
          </Button>
        </div>

        {/* 混频提示 */}
        {hasMixedFreq && (
          <div className="bg-amber-50 border border-amber-200 rounded px-3 py-1.5 text-[11px] text-amber-700 flex items-center gap-1.5">
            <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
            选中基金包含日频和周频数据，回测将自动降频至周频对齐
          </div>
        )}

        {/* 权重表 */}
        {weights.length > 0 && (
          <Table>
            <TableHeader>
              <TableRow className="bg-muted/40">
                <TableHead className="h-7 text-[11px] font-normal">基金名称</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-center">频率</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-center w-32">权重</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-center w-12">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {weights.map(w => (
                <TableRow key={w.fund_id} className="text-[12px]">
                  <TableCell className="py-1">{w.fund_name}</TableCell>
                  <TableCell className="py-1 text-center"><FreqBadge freq={w.nav_frequency} /></TableCell>
                  <TableCell className="py-1 text-center">
                    <div className="flex items-center gap-1 justify-center">
                      <input
                        type="number"
                        min={0} max={1} step={0.01}
                        value={w.weight || ""}
                        onChange={(e) => updateWeight(w.fund_id, parseFloat(e.target.value) || 0)}
                        className="w-16 h-6 px-1.5 text-[12px] text-center border border-border rounded bg-background tabular-nums"
                      />
                      <span className="text-[10px] text-muted-foreground">{(w.weight * 100).toFixed(1)}%</span>
                    </div>
                  </TableCell>
                  <TableCell className="py-1 text-center">
                    <button onClick={() => removeFund(w.fund_id)} className="text-destructive hover:text-destructive/80">
                      <Trash2 className="h-3.5 w-3.5" />
                    </button>
                  </TableCell>
                </TableRow>
              ))}
              <TableRow className="bg-muted/20 text-[12px]">
                <TableCell className="py-1 font-medium">合计</TableCell>
                <TableCell className="py-1" />
                <TableCell className={`py-1 text-center font-medium tabular-nums ${Math.abs(totalWeight - 1.0) > 0.01 ? "text-destructive" : "text-jf-up"}`}>
                  {(totalWeight * 100).toFixed(1)}%
                </TableCell>
                <TableCell className="py-1" />
              </TableRow>
            </TableBody>
          </Table>
        )}
      </div>

      {/* 回测参数 + 执行 */}
      {weights.length >= 2 && (
        <div className="bg-card border border-border rounded px-4 py-3">
          <div className="flex items-center gap-3 flex-wrap">
            <div>
              <label className="text-[11px] text-muted-foreground block mb-0.5">开始日期</label>
              <input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="h-7 px-2 text-[12px] border border-border rounded bg-background"
              />
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground block mb-0.5">结束日期</label>
              <input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="h-7 px-2 text-[12px] border border-border rounded bg-background"
              />
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground block mb-0.5">历史数据处理</label>
              <div className="flex gap-1">
                {historyModes.map(m => (
                  <button
                    key={m.key}
                    onClick={() => setHistoryMode(m.key)}
                    title={m.desc}
                    className={`px-2 py-1 rounded text-[11px] transition-colors ${
                      historyMode === m.key
                        ? "bg-primary text-primary-foreground shadow-sm"
                        : "bg-muted text-muted-foreground hover:bg-muted/80"
                    }`}
                  >
                    {m.label}
                  </button>
                ))}
              </div>
            </div>
            <div className="flex-1" />
            <Button
              size="sm"
              className="h-8 text-[12px] gap-1"
              disabled={backtesting || weights.length < 2 || Math.abs(totalWeight - 1.0) > 0.01}
              onClick={handleBacktest}
            >
              {backtesting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
              运行回测
            </Button>
          </div>

          {btError && (
            <div className="mt-2 bg-destructive/10 border border-destructive/20 rounded px-3 py-1.5 text-[12px] text-destructive">
              {btError}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
