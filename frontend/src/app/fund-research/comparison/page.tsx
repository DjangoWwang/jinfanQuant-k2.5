"use client";

import { useEffect, useState, useMemo, useCallback } from "react";
import dynamic from "next/dynamic";
import { Search, X, Loader2, BarChart3, AlertTriangle, Save, FolderOpen, Trash2 } from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { IntervalSelector } from "@/components/shared/interval-selector";
import { fetchApi } from "@/lib/api";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

/* ─── Types ─── */

interface FundSearchItem {
  id: number;
  fund_name: string;
  strategy_type: string | null;
  nav_frequency: string;
  latest_nav: number | null;
}

interface CompareSeriesItem {
  fund_id: number;
  fund_name: string;
  frequency: string | null;
  nav_series: { date: string; nav: number }[];
}

interface CompareMetricsItem {
  fund_id: number;
  fund_name: string;
  total_return: number | null;
  annualized_return: number | null;
  max_drawdown: number | null;
  annualized_volatility: number | null;
  sharpe_ratio: number | null;
  sortino_ratio: number | null;
  calmar_ratio: number | null;
}

interface CompareResponse {
  start_date: string | null;
  end_date: string | null;
  alignment_method: string;
  frequency_warning: string | null;
  series: CompareSeriesItem[];
  metrics: CompareMetricsItem[];
}

interface SavedComparison {
  id: string;
  name: string;
  fund_ids: number[];
  fund_names: string[];
  interval: string;
  created_at: string;
}

/* ─── Helpers ─── */

function pct(v: number | null | undefined): string {
  if (v == null) return "—";
  const s = (v * 100).toFixed(2);
  return v >= 0 ? `+${s}%` : `${s}%`;
}

function num(v: number | null | undefined): string {
  if (v == null) return "—";
  return v.toFixed(2);
}

const COLORS = ["#4f46e5", "#f59e0b", "#10b981", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4", "#f97316", "#6366f1", "#14b8a6"];

/* ─── Page ─── */

export default function ComparisonPage() {
  const [search, setSearch] = useState("");
  const [searchResults, setSearchResults] = useState<FundSearchItem[]>([]);
  const [searching, setSearching] = useState(false);
  const [selectedFunds, setSelectedFunds] = useState<FundSearchItem[]>([]);
  const [interval, setInterval] = useState("1y");
  const [compareData, setCompareData] = useState<CompareResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Saved comparisons
  const [savedComparisons, setSavedComparisons] = useState<SavedComparison[]>([]);
  const [saveName, setSaveName] = useState("");
  const [showSaveInput, setShowSaveInput] = useState(false);

  // Load saved comparisons from localStorage
  useEffect(() => {
    const saved = localStorage.getItem("jinfan_saved_comparisons");
    if (saved) {
      try {
        setSavedComparisons(JSON.parse(saved));
      } catch {
        setSavedComparisons([]);
      }
    }
  }, []);

  // Save comparisons to localStorage
  const persistSavedComparisons = useCallback((comparisons: SavedComparison[]) => {
    localStorage.setItem("jinfan_saved_comparisons", JSON.stringify(comparisons));
    setSavedComparisons(comparisons);
  }, []);

  // Save current comparison
  const saveComparison = useCallback(() => {
    if (selectedFunds.length < 2 || !saveName.trim()) return;
    const newComparison: SavedComparison = {
      id: Date.now().toString(),
      name: saveName.trim(),
      fund_ids: selectedFunds.map(f => f.id),
      fund_names: selectedFunds.map(f => f.fund_name),
      interval,
      created_at: new Date().toISOString(),
    };
    const updated = [newComparison, ...savedComparisons].slice(0, 20); // Keep last 20
    persistSavedComparisons(updated);
    setSaveName("");
    setShowSaveInput(false);
  }, [selectedFunds, saveName, interval, savedComparisons, persistSavedComparisons]);

  // Load a saved comparison
  const loadComparison = useCallback(async (comparison: SavedComparison) => {
    // Fetch fund details for the saved IDs
    try {
      const fundPromises = comparison.fund_ids.map(id =>
        fetchApi<FundSearchItem>(`/funds/${id}`).catch(() => null)
      );
      const funds = (await Promise.all(fundPromises)).filter((f): f is FundSearchItem => f !== null);
      if (funds.length >= 2) {
        setSelectedFunds(funds);
        setInterval(comparison.interval);
        setCompareData(null);
      }
    } catch {
      // Ignore errors
    }
  }, []);

  // Delete a saved comparison
  const deleteComparison = useCallback((id: string) => {
    const updated = savedComparisons.filter(c => c.id !== id);
    persistSavedComparisons(updated);
  }, [savedComparisons, persistSavedComparisons]);

  // 搜索基金
  useEffect(() => {
    if (!search.trim()) {
      setSearchResults([]);
      return;
    }
    const timer = setTimeout(async () => {
      setSearching(true);
      try {
        const res = await fetchApi<{ items: FundSearchItem[] }>(`/funds/?search=${encodeURIComponent(search)}&page_size=10`);
        setSearchResults(res.items.filter(f => !selectedFunds.some(s => s.id === f.id)));
      } catch {
        setSearchResults([]);
      } finally {
        setSearching(false);
      }
    }, 300);
    return () => clearTimeout(timer);
  }, [search, selectedFunds]);

  function addFund(fund: FundSearchItem) {
    if (selectedFunds.length >= 10) return;
    setSelectedFunds(prev => [...prev, fund]);
    setSearch("");
    setSearchResults([]);
  }

  function removeFund(id: number) {
    setSelectedFunds(prev => prev.filter(f => f.id !== id));
    setCompareData(null);
  }

  // 执行比较
  const doCompare = useCallback(async () => {
    if (selectedFunds.length < 2) return;
    setLoading(true);
    setError(null);
    try {
      const result = await fetchApi<CompareResponse>("/comparison/", {
        method: "POST",
        body: JSON.stringify({
          fund_ids: selectedFunds.map(f => f.id),
          preset: interval,
        }),
      });
      setCompareData(result);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "比较失败");
    } finally {
      setLoading(false);
    }
  }, [selectedFunds, interval]);

  // 归一化NAV图表
  const chartOption = useMemo(() => {
    if (!compareData?.series?.length) return {};
    // 归一化：每条线起始值设为1
    const allDates = new Set<string>();
    compareData.series.forEach(s => s.nav_series.forEach(p => allDates.add(p.date)));
    const dates = Array.from(allDates).sort();

    const series = compareData.series.map((s, i) => {
      const navMap = new Map(s.nav_series.map(p => [p.date, p.nav]));
      const firstNav = s.nav_series[0]?.nav || 1;
      return {
        name: s.fund_name,
        type: "line" as const,
        smooth: true,
        symbol: "none",
        lineStyle: { width: 2, color: COLORS[i % COLORS.length] },
        data: dates.map(d => {
          const nav = navMap.get(d);
          return nav != null ? nav / firstNav : null;
        }),
      };
    });

    return {
      tooltip: {
        trigger: "axis",
        formatter: (params: Array<{ seriesName: string; value: number | null; color: string }>) => {
          const date = params[0] && 'axisValue' in params[0] ? (params[0] as unknown as { axisValue: string }).axisValue : '';
          const lines = params
            .filter(p => p.value != null)
            .map(p => `<span style="color:${p.color}">${p.seriesName}</span>: ${((p.value! - 1) * 100).toFixed(2)}%`);
          return `${date}<br/>${lines.join("<br/>")}`;
        },
      },
      legend: {
        bottom: 0,
        textStyle: { fontSize: 11 },
      },
      grid: { left: 50, right: 20, top: 10, bottom: 40 },
      xAxis: {
        type: "category",
        data: dates,
        axisLabel: { fontSize: 10, color: "#999" },
        axisLine: { lineStyle: { color: "#e5e7eb" } },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: {
          fontSize: 10,
          color: "#999",
          formatter: (v: number) => `${((v - 1) * 100).toFixed(0)}%`,
        },
        splitLine: { lineStyle: { color: "#f3f4f6" } },
      },
      series,
    };
  }, [compareData]);

  return (
    <div className="space-y-3">
      <PageHeader title="基金比较" description="选择2-10只基金进行对比分析" />

      {/* Saved Comparisons */}
      {savedComparisons.length > 0 && (
        <div className="bg-card border border-border rounded px-4 py-2">
          <div className="flex items-center gap-2 mb-2">
            <FolderOpen className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-[12px] text-muted-foreground">已保存的对比方案</span>
          </div>
          <div className="flex gap-2 flex-wrap">
            {savedComparisons.map((comparison) => (
              <div
                key={comparison.id}
                className="inline-flex items-center gap-1.5 px-2 py-1 rounded text-[11px] bg-muted/50 border border-border hover:bg-muted transition-colors"
              >
                <button
                  onClick={() => loadComparison(comparison)}
                  className="hover:text-primary"
                >
                  {comparison.name}
                </button>
                <span className="text-muted-foreground">
                  ({comparison.fund_names.length}只·{comparison.interval})
                </span>
                <button
                  onClick={() => deleteComparison(comparison.id)}
                  className="hover:text-destructive ml-1"
                >
                  <Trash2 className="h-3 w-3" />
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 基金选择器 */}
      <div className="bg-card border border-border rounded px-4 py-3 space-y-2">
        <div className="flex items-center gap-2">
          <div className="relative flex-1 max-w-sm">
            <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="搜索基金名称添加到比较..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-8 h-8 text-[12px]"
            />
            {searching && <Loader2 className="absolute right-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 animate-spin text-muted-foreground" />}

            {/* 搜索下拉 */}
            {searchResults.length > 0 && (
              <div className="absolute z-50 top-full mt-1 left-0 w-full bg-card border border-border rounded shadow-lg max-h-48 overflow-y-auto">
                {searchResults.map(f => (
                  <button
                    key={f.id}
                    className="w-full text-left px-3 py-1.5 text-[12px] hover:bg-muted/50 flex items-center justify-between"
                    onClick={() => addFund(f)}
                  >
                    <span>{f.fund_name}</span>
                    <span className="text-[10px] text-muted-foreground">
                      {f.strategy_type || ""} · {f.nav_frequency === "daily" ? "日频" : "周频"}
                    </span>
                  </button>
                ))}
              </div>
            )}
          </div>
          <Button
            size="sm" className="h-8 text-[12px]"
            disabled={selectedFunds.length < 2 || loading}
            onClick={doCompare}
          >
            {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" /> : null}
            开始比较
          </Button>
          {compareData && (
            <Button
              variant="outline"
              size="sm"
              className="h-8 text-[12px] gap-1"
              onClick={() => setShowSaveInput(!showSaveInput)}
            >
              <Save className="h-3.5 w-3.5" />
              保存方案
            </Button>
          )}
        </div>

        {/* Save input */}
        {showSaveInput && (
          <div className="flex items-center gap-2 pt-1">
            <Input
              placeholder="输入方案名称..."
              value={saveName}
              onChange={(e) => setSaveName(e.target.value)}
              className="h-7 text-[12px] max-w-xs"
              onKeyDown={(e) => e.key === "Enter" && saveComparison()}
            />
            <Button size="sm" className="h-7 text-[11px] px-2" onClick={saveComparison} disabled={!saveName.trim()}>
              确认
            </Button>
            <Button variant="ghost" size="sm" className="h-7 text-[11px] px-2" onClick={() => setShowSaveInput(false)}>
              取消
            </Button>
          </div>
        )}

        {/* 已选基金标签 */}
        {selectedFunds.length > 0 && (
          <div className="flex gap-1.5 flex-wrap">
            {selectedFunds.map((f, i) => (
              <span
                key={f.id}
                className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] border"
                style={{ borderColor: COLORS[i % COLORS.length] + "40", backgroundColor: COLORS[i % COLORS.length] + "10", color: COLORS[i % COLORS.length] }}
              >
                {f.fund_name}
                <button onClick={() => removeFund(f.id)} className="hover:opacity-60">
                  <X className="h-3 w-3" />
                </button>
              </span>
            ))}
          </div>
        )}
      </div>

      {/* 区间选择 */}
      {selectedFunds.length >= 2 && (
        <IntervalSelector value={interval} onChange={setInterval} />
      )}

      {/* 错误提示 */}
      {error && (
        <div className="bg-destructive/10 border border-destructive/20 rounded px-4 py-2 text-[12px] text-destructive">
          {error}
        </div>
      )}

      {/* 频率警告 */}
      {compareData?.frequency_warning && (
        <div className="bg-amber-50 border border-amber-200 rounded px-4 py-2 text-[12px] text-amber-700 flex items-center gap-2">
          <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
          {compareData.frequency_warning}
        </div>
      )}

      {/* 归一化NAV图 */}
      {compareData && compareData.series.length > 0 ? (
        <div className="bg-card border border-border rounded">
          <div className="px-4 py-2 border-b border-border">
            <span className="text-[13px] font-medium">归一化净值走势</span>
            <span className="text-[11px] text-muted-foreground ml-2">
              {compareData.start_date} ~ {compareData.end_date}
            </span>
          </div>
          <ReactECharts option={chartOption} style={{ height: 320 }} notMerge />
        </div>
      ) : !loading && selectedFunds.length >= 2 && !compareData ? (
        <div className="bg-card border border-border rounded h-48 flex items-center justify-center text-muted-foreground">
          <div className="text-center space-y-1">
            <BarChart3 className="mx-auto h-7 w-7 opacity-25" />
            <p className="text-[12px] opacity-60">点击"开始比较"查看对比结果</p>
          </div>
        </div>
      ) : null}

      {/* 指标对比表 */}
      {compareData && compareData.metrics.length > 0 && (
        <div className="bg-card border border-border rounded overflow-hidden">
          <div className="px-4 py-2 border-b border-border">
            <span className="text-[13px] font-medium">指标对比</span>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-[12px]">
              <thead>
                <tr className="bg-muted/40 text-[11px] text-muted-foreground">
                  <th className="px-4 py-1.5 text-left font-normal">基金名称</th>
                  <th className="px-4 py-1.5 text-right font-normal">区间收益</th>
                  <th className="px-4 py-1.5 text-right font-normal">年化收益</th>
                  <th className="px-4 py-1.5 text-right font-normal">最大回撤</th>
                  <th className="px-4 py-1.5 text-right font-normal">年化波动率</th>
                  <th className="px-4 py-1.5 text-right font-normal">夏普比率</th>
                  <th className="px-4 py-1.5 text-right font-normal">Sortino</th>
                  <th className="px-4 py-1.5 text-right font-normal">Calmar</th>
                </tr>
              </thead>
              <tbody>
                {compareData.metrics.map((m, i) => (
                  <tr key={m.fund_id} className="border-t border-border hover:bg-muted/20">
                    <td className="px-4 py-1.5 font-medium" style={{ color: COLORS[i % COLORS.length] }}>
                      {m.fund_name}
                    </td>
                    <td className={`px-4 py-1.5 text-right tabular-nums ${(m.total_return ?? 0) >= 0 ? "text-jf-up" : "text-jf-down"}`}>
                      {pct(m.total_return)}
                    </td>
                    <td className={`px-4 py-1.5 text-right tabular-nums ${(m.annualized_return ?? 0) >= 0 ? "text-jf-up" : "text-jf-down"}`}>
                      {pct(m.annualized_return)}
                    </td>
                    <td className="px-4 py-1.5 text-right tabular-nums text-jf-down">{pct(m.max_drawdown)}</td>
                    <td className="px-4 py-1.5 text-right tabular-nums">{pct(m.annualized_volatility)}</td>
                    <td className="px-4 py-1.5 text-right tabular-nums">{num(m.sharpe_ratio)}</td>
                    <td className="px-4 py-1.5 text-right tabular-nums">{num(m.sortino_ratio)}</td>
                    <td className="px-4 py-1.5 text-right tabular-nums">{num(m.calmar_ratio)}</td>
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
