"use client";

import { Suspense, useState, useEffect, useCallback, useRef } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import dynamic from "next/dynamic";
import {
  ArrowLeft, Plus, Trash2, Loader2, Play, Search, X,
  AlertTriangle, Check, ChevronRight, Settings2, Save,
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

interface AssetSearchItem {
  asset_type: "fund" | "index";
  asset_id: string;
  fund_id: number | null;
  index_code: string | null;
  name: string;
  sub_label: string;
  strategy: string;
  frequency: string;
  latest_nav: number | null;
}

interface WeightItem {
  asset_id: string;
  fund_id: number | null;
  index_code: string | null;
  name: string;
  frequency: string;
  weight: number; // percentage: 30 = 30%
}

interface BacktestResult {
  backtest_id: number | null;
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

/* --- Helpers --- */

function pct(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return "--";
  const capped = Math.max(Math.min(v, 99.99), -9.999);
  return `${(capped * 100).toFixed(2)}%`;
}

function num(v: number | null | undefined, dp = 2): string {
  if (v == null || !isFinite(v)) return "--";
  return Math.max(Math.min(v, 999.99), -999.99).toFixed(dp);
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

function AssetTypeBadge({ type }: { type: string }) {
  return (
    <span className={`px-1.5 rounded text-[10px] leading-relaxed ${
      type === "index" ? "bg-purple-50 text-purple-600" : "bg-slate-50 text-slate-600"
    }`}>
      {type === "index" ? "指数" : "基金"}
    </span>
  );
}

const STEPS = [
  { num: 1, label: "配置方案" },
  { num: 2, label: "确定方案" },
  { num: 3, label: "模拟回测" },
];

const historyModes = [
  { key: "intersection", label: "严格交集", desc: "所有资产必须有完整数据" },
  { key: "dynamic_entry", label: "动态入场", desc: "资产数据出现时自动纳入" },
  { key: "truncate", label: "截断排除", desc: "排除数据不足的资产" },
] as const;

/* --- Page --- */

export default function CreatePortfolioPageWrapper() {
  return (
    <Suspense fallback={<div className="flex items-center justify-center h-40"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>}>
      <CreatePortfolioPage />
    </Suspense>
  );
}

function CreatePortfolioPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const addFundHandled = useRef(false);
  const [step, setStep] = useState(1);

  // Step 1: 配置
  const [portfolioName, setPortfolioName] = useState("");
  const [rebalanceFreq, setRebalanceFreq] = useState("monthly");
  const [weights, setWeights] = useState<WeightItem[]>([]);

  // 资产选择弹窗
  const [showAssetModal, setShowAssetModal] = useState(false);
  const [assetTab, setAssetTab] = useState<"fund" | "index">("fund");
  const [assetSearch, setAssetSearch] = useState("");
  const [assetResults, setAssetResults] = useState<AssetSearchItem[]>([]);
  const [assetSearching, setAssetSearching] = useState(false);
  const [selectedAssets, setSelectedAssets] = useState<AssetSearchItem[]>([]);

  // Step 2: 回测参数
  const [startDate, setStartDate] = useState("2024-01-01");
  const [endDate, setEndDate] = useState("2026-03-01");
  const [historyMode, setHistoryMode] = useState("intersection");
  const [costBps, setCostBps] = useState(0);
  const [riskFreeRate, setRiskFreeRate] = useState(0.02);

  // Step 3: 回测结果
  const [backtesting, setBacktesting] = useState(false);
  const [btResult, setBtResult] = useState<BacktestResult | null>(null);
  const [btError, setBtError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [showSaveModal, setShowSaveModal] = useState(false);

  // 从基金详情页跳转: 自动添加基金
  useEffect(() => {
    const addFundId = searchParams.get("addFund");
    const fundName = searchParams.get("fundName");
    const freq = searchParams.get("freq") || "daily";
    if (addFundId && !addFundHandled.current) {
      addFundHandled.current = true;
      const fid = parseInt(addFundId, 10);
      if (!isNaN(fid) && fundName) {
        setWeights(prev => {
          if (prev.some(w => w.asset_id === `fund_${fid}`)) return prev;
          return [...prev, {
            asset_id: `fund_${fid}`, fund_id: fid, index_code: null,
            name: fundName, frequency: freq, weight: 0,
          }];
        });
      }
    }
  }, [searchParams]);

  // 搜索资产 (弹窗内) — 传 asset_type 到后端，无需客户端过滤
  useEffect(() => {
    if (!showAssetModal) return;
    const query = assetSearch.trim();
    const timer = setTimeout(async () => {
      setAssetSearching(true);
      try {
        const url = `/backtest/search-assets?q=${encodeURIComponent(query)}&asset_type=${assetTab}&limit=15`;
        const data = await fetchApi<AssetSearchItem[]>(url);
        setAssetResults(data);
      } catch {
        setAssetResults([]);
      } finally {
        setAssetSearching(false);
      }
    }, query ? 200 : 0);
    return () => clearTimeout(timer);
  }, [assetSearch, assetTab, showAssetModal]);

  function toggleAssetSelection(asset: AssetSearchItem) {
    setSelectedAssets(prev => {
      const exists = prev.some(a => a.asset_id === asset.asset_id);
      if (exists) return prev.filter(a => a.asset_id !== asset.asset_id);
      return [...prev, asset];
    });
  }

  function confirmAssetSelection() {
    const newWeights = selectedAssets
      .filter(a => !weights.some(w => w.asset_id === a.asset_id))
      .map(a => ({
        asset_id: a.asset_id,
        fund_id: a.fund_id,
        index_code: a.index_code,
        name: a.name,
        frequency: a.frequency,
        weight: 0,
      }));
    setWeights(prev => [...prev, ...newWeights]);
    setShowAssetModal(false);
    setSelectedAssets([]);
    setAssetSearch("");
  }

  function removeWeight(assetId: string) {
    setWeights(prev => prev.filter(w => w.asset_id !== assetId));
  }

  function setEqualWeights() {
    if (weights.length === 0) return;
    const w = parseFloat((100 / weights.length).toFixed(2));
    setWeights(prev => prev.map(item => ({ ...item, weight: w })));
  }

  function updateWeight(assetId: string, value: number) {
    setWeights(prev => prev.map(w => w.asset_id === assetId ? { ...w, weight: value } : w));
  }

  const totalWeight = weights.reduce((sum, w) => sum + w.weight, 0);
  const hasMixedFreq = weights.length > 0 && new Set(weights.map(w => w.frequency)).size > 1;
  const canProceedStep1 = weights.length >= 2 && Math.abs(totalWeight - 100) <= 1;

  // 运行回测
  async function handleBacktest() {
    if (!canProceedStep1) return;
    setBacktesting(true);
    setBtError(null);
    setBtResult(null);
    setStep(3);

    try {
      const result = await fetchApi<BacktestResult>("/backtest/run", {
        method: "POST",
        body: JSON.stringify({
          weights: weights.map(w => ({
            fund_id: w.fund_id,
            index_code: w.index_code,
            weight: w.weight / 100,
          })),
          start_date: startDate,
          end_date: endDate,
          rebalance_frequency: rebalanceFreq,
          transaction_cost_bps: costBps,
          risk_free_rate: riskFreeRate,
          history_mode: historyMode,
          initial_capital: 10000000,
        }),
      });
      setBtResult(result);
    } catch (e: unknown) {
      setBtError(e instanceof Error ? e.message : "回测失败");
    } finally {
      setBacktesting(false);
    }
  }

  // 保存组合
  async function handleSaveConfirm() {
    const name = portfolioName.trim();
    if (!name) return;
    setSaving(true);
    try {
      await fetchApi("/portfolios/", {
        method: "POST",
        body: JSON.stringify({
          name,
          description: null,
          weights: weights.map(w => ({
            fund_id: w.fund_id,
            index_code: w.index_code,
            weight: w.weight / 100,
          })),
          rebalance_frequency: rebalanceFreq,
        }),
      });
      setSaved(true);
      setShowSaveModal(false);
    } catch (e: unknown) {
      setBtError(e instanceof Error ? e.message : "保存失败");
    } finally {
      setSaving(false);
    }
  }

  // Charts
  const navChartOption = btResult ? {
    tooltip: { trigger: "axis" },
    grid: { left: 50, right: 20, top: 10, bottom: 30 },
    xAxis: { type: "category", data: btResult.nav_series.map(d => d.date), axisLabel: { fontSize: 10 } },
    yAxis: { type: "value", scale: true, axisLabel: { fontSize: 10 } },
    series: [{ type: "line", data: btResult.nav_series.map(d => d.nav), smooth: true, symbol: "none", lineStyle: { width: 2, color: "#4f46e5" }, areaStyle: { color: { type: "linear", x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: "rgba(79,70,229,0.12)" }, { offset: 1, color: "rgba(79,70,229,0.01)" }] } } }],
  } : {};

  const ddChartOption = btResult ? {
    tooltip: { trigger: "axis" },
    grid: { left: 50, right: 20, top: 10, bottom: 30 },
    xAxis: { type: "category", data: btResult.drawdown_series.map(d => d.date), axisLabel: { fontSize: 10 } },
    yAxis: { type: "value", max: 0, axisLabel: { fontSize: 10, formatter: (v: number) => `${(v * 100).toFixed(0)}%` } },
    series: [{ type: "line", data: btResult.drawdown_series.map(d => d.drawdown), smooth: true, symbol: "none", lineStyle: { width: 1.5, color: "#ef4444" }, areaStyle: { color: { type: "linear", x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: "rgba(239,68,68,0.01)" }, { offset: 1, color: "rgba(239,68,68,0.12)" }] } } }],
  } : {};

  return (
    <div className="space-y-4">
      {/* 返回 + 标题 */}
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="sm" className="gap-1 text-[12px] h-7" onClick={() => router.push("/fund-research/portfolio")}>
          <ArrowLeft className="h-3.5 w-3.5" />返回组合列表
        </Button>
      </div>

      {/* 步骤条 */}
      <div className="flex items-center justify-center gap-0 py-2">
        {STEPS.map((s, i) => (
          <div key={s.num} className="flex items-center">
            <button
              onClick={() => { if (s.num < step || (s.num <= 2)) setStep(s.num); }}
              className={`flex items-center gap-2 px-3 py-1 rounded-full text-[12px] transition-colors ${
                step === s.num
                  ? "bg-primary text-primary-foreground"
                  : step > s.num
                    ? "bg-primary/10 text-primary"
                    : "bg-muted text-muted-foreground"
              }`}
            >
              <span className={`w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-bold ${
                step > s.num ? "bg-primary text-primary-foreground" : ""
              }`}>
                {step > s.num ? <Check className="h-3 w-3" /> : s.num}
              </span>
              {s.label}
            </button>
            {i < STEPS.length - 1 && (
              <div className={`w-16 h-[2px] mx-1 ${step > s.num ? "bg-primary/40" : "bg-border"}`} />
            )}
          </div>
        ))}
      </div>

      {/* Step 1: 配置方案 */}
      {step === 1 && (
        <div className="space-y-4">
          {/* 基本设置 */}
          <div className="bg-card border border-border rounded px-5 py-4 space-y-3">
            <h3 className="text-[13px] font-semibold flex items-center gap-2">
              <Settings2 className="h-4 w-4 text-primary opacity-70" />
              基本设置
            </h3>
            <div className="flex items-end gap-4">
              <div>
                <label className="text-[11px] text-muted-foreground block mb-1">再平衡频率</label>
                <select
                  value={rebalanceFreq}
                  onChange={(e) => setRebalanceFreq(e.target.value)}
                  className="h-8 px-3 text-[12px] border border-border rounded bg-background"
                >
                  <option value="daily">每日</option>
                  <option value="weekly">每周</option>
                  <option value="monthly">每月</option>
                  <option value="quarterly">每季度</option>
                </select>
              </div>
            </div>
          </div>

          {/* 资产选择 */}
          <div className="bg-card border border-border rounded px-5 py-4 space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="text-[13px] font-semibold">资产选择</h3>
              <div className="flex items-center gap-2">
                <Button variant="outline" size="sm" className="h-7 text-[11px]" onClick={setEqualWeights} disabled={weights.length === 0}>
                  等权分配
                </Button>
                <Button size="sm" className="h-7 text-[11px] gap-1" onClick={() => { setShowAssetModal(true); setSelectedAssets([]); }}>
                  <Plus className="h-3 w-3" />添加资产
                </Button>
              </div>
            </div>

            {/* 混频提示 */}
            {hasMixedFreq && (
              <div className="bg-amber-50 border border-amber-200 rounded px-3 py-2 text-[11px] text-amber-700 flex items-center gap-1.5">
                <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
                选中资产包含不同频率数据，回测将自动降频至周频对齐
              </div>
            )}

            {/* 权重表 */}
            {weights.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow className="bg-muted/40">
                    <TableHead className="h-7 text-[11px] font-normal w-10 text-center">序号</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal">资产名称</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center w-16">类型</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center w-16">频率</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center w-28">权重 (%)</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center w-14">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {weights.map((w, i) => (
                    <TableRow key={w.asset_id} className="text-[12px]">
                      <TableCell className="py-1.5 text-center text-muted-foreground">{i + 1}</TableCell>
                      <TableCell className="py-1.5 font-medium">{w.name}</TableCell>
                      <TableCell className="py-1.5 text-center">
                        <AssetTypeBadge type={w.fund_id ? "fund" : "index"} />
                      </TableCell>
                      <TableCell className="py-1.5 text-center"><FreqBadge freq={w.frequency} /></TableCell>
                      <TableCell className="py-1.5 text-center">
                        <div className="flex items-center gap-1 justify-center">
                          <input
                            type="number"
                            min={0} max={100} step={1}
                            value={w.weight || ""}
                            onChange={(e) => updateWeight(w.asset_id, parseFloat(e.target.value) || 0)}
                            className="w-16 h-6 px-1.5 text-[12px] text-center border border-border rounded bg-background tabular-nums"
                          />
                          <span className="text-[10px] text-muted-foreground">%</span>
                        </div>
                      </TableCell>
                      <TableCell className="py-1.5 text-center">
                        <button onClick={() => removeWeight(w.asset_id)} className="text-destructive hover:text-destructive/80">
                          <Trash2 className="h-3.5 w-3.5" />
                        </button>
                      </TableCell>
                    </TableRow>
                  ))}
                  <TableRow className="bg-muted/20 text-[12px]">
                    <TableCell className="py-1.5" />
                    <TableCell className="py-1.5 font-medium">合计</TableCell>
                    <TableCell className="py-1.5" />
                    <TableCell className="py-1.5" />
                    <TableCell className={`py-1.5 text-center font-semibold tabular-nums ${Math.abs(totalWeight - 100) > 1 ? "text-destructive" : "text-jf-up"}`}>
                      {totalWeight.toFixed(1)}%
                    </TableCell>
                    <TableCell className="py-1.5" />
                  </TableRow>
                </TableBody>
              </Table>
            ) : (
              <div className="h-24 flex items-center justify-center text-muted-foreground text-[12px] border border-dashed border-border rounded">
                点击"添加资产"选择基金或指数
              </div>
            )}
          </div>

          {/* 下一步 */}
          <div className="flex justify-end">
            <Button
              size="sm"
              className="h-8 text-[12px] gap-1 px-6"
              disabled={!canProceedStep1}
              onClick={() => setStep(2)}
            >
              下一步 <ChevronRight className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>
      )}

      {/* Step 2: 确定方案 — 回测参数 */}
      {step === 2 && (
        <div className="space-y-4">
          {/* 方案概览 */}
          <div className="bg-card border border-border rounded px-5 py-4 space-y-3">
            <h3 className="text-[13px] font-semibold">方案概览</h3>
            <div className="text-[12px] space-y-1">
              <p><span className="text-muted-foreground">资产数量:</span> {weights.length} 只</p>
              <p><span className="text-muted-foreground">权重分配:</span> {weights.map(w => `${w.name} ${w.weight}%`).join("、")}</p>
            </div>
          </div>

          {/* 回测参数 */}
          <div className="bg-card border border-border rounded px-5 py-4 space-y-4">
            <h3 className="text-[13px] font-semibold">回测参数</h3>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div>
                <label className="text-[11px] text-muted-foreground block mb-1">开始日期</label>
                <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)}
                  className="h-8 px-2 text-[12px] border border-border rounded bg-background w-full" />
              </div>
              <div>
                <label className="text-[11px] text-muted-foreground block mb-1">结束日期</label>
                <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)}
                  className="h-8 px-2 text-[12px] border border-border rounded bg-background w-full" />
              </div>
              <div>
                <label className="text-[11px] text-muted-foreground block mb-1">交易成本 (bps)</label>
                <input type="number" min={0} max={100} step={1} value={costBps}
                  onChange={(e) => setCostBps(parseFloat(e.target.value) || 0)}
                  className="h-8 px-2 text-[12px] border border-border rounded bg-background w-full tabular-nums" />
              </div>
              <div>
                <label className="text-[11px] text-muted-foreground block mb-1">无风险利率 (%)</label>
                <input type="number" min={0} max={10} step={0.1} value={(riskFreeRate * 100).toFixed(1)}
                  onChange={(e) => setRiskFreeRate((parseFloat(e.target.value) || 0) / 100)}
                  className="h-8 px-2 text-[12px] border border-border rounded bg-background w-full tabular-nums" />
              </div>
            </div>

            <div>
              <label className="text-[11px] text-muted-foreground block mb-1.5">历史数据处理</label>
              <div className="flex gap-2">
                {historyModes.map(m => (
                  <button
                    key={m.key}
                    onClick={() => setHistoryMode(m.key)}
                    className={`px-3 py-1.5 rounded text-[11px] border transition-colors ${
                      historyMode === m.key
                        ? "bg-primary text-primary-foreground border-primary"
                        : "bg-background text-muted-foreground border-border hover:border-primary/50"
                    }`}
                  >
                    {m.label}
                  </button>
                ))}
              </div>
              <p className="text-[10px] text-muted-foreground mt-1">
                {historyModes.find(m => m.key === historyMode)?.desc}
              </p>
            </div>
          </div>

          {/* 操作 */}
          <div className="flex justify-between">
            <Button variant="outline" size="sm" className="h-8 text-[12px]" onClick={() => setStep(1)}>
              <ArrowLeft className="h-3.5 w-3.5 mr-1" />上一步
            </Button>
            <Button size="sm" className="h-8 text-[12px] gap-1 px-6" onClick={handleBacktest}>
              <Play className="h-3.5 w-3.5" />运行回测
            </Button>
          </div>
        </div>
      )}

      {/* Step 3: 回测结果 */}
      {step === 3 && (
        <div className="space-y-4">
          {backtesting && (
            <div className="h-40 flex items-center justify-center">
              <div className="text-center space-y-2">
                <Loader2 className="h-6 w-6 animate-spin text-primary mx-auto" />
                <p className="text-[12px] text-muted-foreground">正在运行回测...</p>
              </div>
            </div>
          )}

          {btError && (
            <div className="bg-destructive/10 border border-destructive/20 rounded px-4 py-3 text-[12px] text-destructive">
              {btError}
            </div>
          )}

          {btResult && (
            <div className="space-y-4">
              {/* 指标卡片 */}
              <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-2">
                {[
                  { label: "总收益", value: pct(btResult.metrics.total_return) },
                  { label: "年化收益", value: pct(btResult.metrics.annualized_return) },
                  { label: "最大回撤", value: pct(btResult.metrics.max_drawdown) },
                  { label: "年化波动率", value: pct(btResult.metrics.volatility) },
                  { label: "夏普比率", value: num(btResult.metrics.sharpe_ratio) },
                  { label: "Sortino", value: num(btResult.metrics.sortino_ratio) },
                  { label: "Calmar", value: num(btResult.metrics.calmar_ratio) },
                ].map(m => (
                  <div key={m.label} className="bg-card border border-border rounded px-3 py-2.5">
                    <div className="text-[10px] text-muted-foreground mb-0.5">{m.label}</div>
                    <div className="text-[15px] font-semibold tabular-nums">{m.value}</div>
                  </div>
                ))}
              </div>

              {/* NAV 图 */}
              <div className="bg-card border border-border rounded">
                <div className="px-4 py-2 border-b border-border text-[13px] font-medium">净值走势</div>
                <ReactECharts option={navChartOption} style={{ height: 280 }} notMerge />
              </div>

              {/* 回撤图 */}
              <div className="bg-card border border-border rounded">
                <div className="px-4 py-2 border-b border-border text-[13px] font-medium">历史回撤</div>
                <ReactECharts option={ddChartOption} style={{ height: 180 }} notMerge />
              </div>

              {/* 月度收益表 */}
              {btResult.monthly_returns.length > 0 && (
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
          )}

          {/* 操作 */}
          <div className="flex justify-between">
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" className="h-8 text-[12px]" onClick={() => setStep(1)}>
                <ArrowLeft className="h-3.5 w-3.5 mr-1" />修改组合
              </Button>
              <Button variant="outline" size="sm" className="h-8 text-[12px]" onClick={() => setStep(2)}>
                修改参数
              </Button>
            </div>
            <div className="flex items-center gap-2">
              {btResult && !saved && (
                <Button size="sm" className="h-8 text-[12px] gap-1 px-5" onClick={() => setShowSaveModal(true)}>
                  <Save className="h-3.5 w-3.5" />保存组合
                </Button>
              )}
              {saved && (
                <span className="text-[12px] text-jf-up flex items-center gap-1">
                  <Check className="h-3.5 w-3.5" />已保存
                </span>
              )}
              <Button variant="outline" size="sm" className="h-8 text-[12px]" onClick={() => router.push("/fund-research/portfolio")}>
                返回组合列表
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* 资产选择弹窗 */}
      {showAssetModal && (
        <div className="fixed inset-0 z-50 flex items-start justify-center pt-16 bg-black/40" onClick={() => setShowAssetModal(false)}>
          <div className="bg-card border border-border rounded-lg shadow-xl w-[700px] max-h-[70vh] flex flex-col" onClick={e => e.stopPropagation()}>
            {/* 弹窗头部 */}
            <div className="flex items-center justify-between px-5 py-3 border-b border-border">
              <h3 className="text-[14px] font-semibold">选择资产</h3>
              <div className="flex items-center gap-3">
                <span className="text-[12px] text-muted-foreground">已选 ({selectedAssets.length})</span>
                <button onClick={() => setShowAssetModal(false)} className="text-muted-foreground hover:text-foreground">
                  <X className="h-4 w-4" />
                </button>
              </div>
            </div>

            {/* 搜索 + Tab */}
            <div className="px-5 py-3 space-y-2 border-b border-border">
              <div className="flex items-center gap-2">
                <div className="relative flex-1">
                  <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                  <Input
                    placeholder="输入名称/代码搜索..."
                    value={assetSearch}
                    onChange={(e) => setAssetSearch(e.target.value)}
                    className="pl-8 h-8 text-[12px]"
                    autoFocus
                  />
                </div>
              </div>
              <div className="flex gap-1">
                {(["fund", "index"] as const).map(tab => (
                  <button
                    key={tab}
                    onClick={() => setAssetTab(tab)}
                    className={`px-3 py-1 rounded text-[12px] transition-colors ${
                      assetTab === tab
                        ? "bg-primary text-primary-foreground"
                        : "text-muted-foreground hover:bg-muted"
                    }`}
                  >
                    {tab === "fund" ? "基金" : "指数"}
                  </button>
                ))}
              </div>
            </div>

            {/* 搜索结果列表 */}
            <div className="flex-1 overflow-y-auto px-5 py-2 min-h-[200px] max-h-[400px]">
              {assetSearching ? (
                <div className="h-32 flex items-center justify-center">
                  <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
                </div>
              ) : assetResults.length === 0 ? (
                <div className="h-32 flex items-center justify-center text-muted-foreground text-[12px]">
                  未找到匹配资产
                </div>
              ) : (
                <table className="w-full text-[12px]">
                  <thead>
                    <tr className="text-[11px] text-muted-foreground border-b border-border">
                      <th className="py-1.5 text-left font-normal w-8"></th>
                      <th className="py-1.5 text-left font-normal">名称</th>
                      <th className="py-1.5 text-left font-normal">{assetTab === "fund" ? "管理人" : "分类"}</th>
                      <th className="py-1.5 text-center font-normal">频率</th>
                      {assetTab === "fund" && <th className="py-1.5 text-right font-normal">最新净值</th>}
                    </tr>
                  </thead>
                  <tbody>
                    {assetResults.map(a => {
                      const isSelected = selectedAssets.some(s => s.asset_id === a.asset_id);
                      const alreadyAdded = weights.some(w => w.asset_id === a.asset_id);
                      return (
                        <tr
                          key={a.asset_id}
                          className={`border-b border-border/50 cursor-pointer transition-colors ${
                            alreadyAdded ? "opacity-40" : isSelected ? "bg-primary/5" : "hover:bg-muted/30"
                          }`}
                          onClick={() => !alreadyAdded && toggleAssetSelection(a)}
                        >
                          <td className="py-1.5">
                            <div className={`w-4 h-4 rounded border flex items-center justify-center ${
                              isSelected || alreadyAdded ? "bg-primary border-primary" : "border-border"
                            }`}>
                              {(isSelected || alreadyAdded) && <Check className="h-3 w-3 text-primary-foreground" />}
                            </div>
                          </td>
                          <td className="py-1.5 font-medium">{a.name}</td>
                          <td className="py-1.5 text-muted-foreground">{a.sub_label}</td>
                          <td className="py-1.5 text-center"><FreqBadge freq={a.frequency} /></td>
                          {assetTab === "fund" && (
                            <td className="py-1.5 text-right tabular-nums">
                              {a.latest_nav != null ? a.latest_nav.toFixed(4) : "--"}
                            </td>
                          )}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>

            {/* 弹窗底部 */}
            <div className="flex items-center justify-end gap-2 px-5 py-3 border-t border-border">
              <Button variant="outline" size="sm" className="h-8 text-[12px]" onClick={() => setShowAssetModal(false)}>
                取消
              </Button>
              <Button size="sm" className="h-8 text-[12px] px-6" onClick={confirmAssetSelection} disabled={selectedAssets.length === 0}>
                确定 ({selectedAssets.length})
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* 保存组合命名弹窗 */}
      {showSaveModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={() => setShowSaveModal(false)}>
          <div className="bg-card border border-border rounded-lg shadow-xl w-[400px] p-5 space-y-4" onClick={e => e.stopPropagation()}>
            <h3 className="text-[14px] font-semibold">保存组合</h3>
            <div>
              <label className="text-[11px] text-muted-foreground block mb-1">组合名称</label>
              <Input
                placeholder="输入组合名称"
                value={portfolioName}
                onChange={(e) => setPortfolioName(e.target.value)}
                className="h-9 text-[13px]"
                autoFocus
                onKeyDown={(e) => { if (e.key === "Enter" && portfolioName.trim()) handleSaveConfirm(); }}
              />
            </div>
            <div className="text-[11px] text-muted-foreground space-y-0.5">
              <p>资产: {weights.map(w => w.name).join("、")}</p>
              <p>权重: {weights.map(w => `${w.weight}%`).join(" / ")}</p>
            </div>
            <div className="flex justify-end gap-2">
              <Button variant="outline" size="sm" className="h-8 text-[12px]" onClick={() => setShowSaveModal(false)}>
                取消
              </Button>
              <Button size="sm" className="h-8 text-[12px] gap-1 px-5" onClick={handleSaveConfirm} disabled={saving || !portfolioName.trim()}>
                {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
                {saving ? "保存中..." : "确认保存"}
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
