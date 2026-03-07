"use client";

import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { Search, Download, RefreshCw, ChevronLeft, ChevronRight, ChevronDown, ArrowUpDown, ArrowUp, ArrowDown, Loader2, FlaskConical } from "lucide-react";
import Link from "next/link";
import { PageHeader } from "@/components/layout/page-header";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { fetchApi } from "@/lib/api";

/* ─── Types ─── */

interface FundItem {
  id: number;
  fund_name: string;
  filing_number: string | null;
  manager_name: string | null;
  strategy_type: string | null;
  strategy_sub: string | null;
  nav_frequency: string;
  latest_nav: number | null;
  latest_nav_date: string | null;
  inception_date: string | null;
  is_private: boolean;
  status: string;
  nav_status: string | null;
  data_quality_score: number | null;
  data_quality_tags: string | null;
}

interface FundListResponse {
  total: number;
  page: number;
  page_size: number;
  items: FundItem[];
}

interface StrategyCategoryItem {
  strategy_type: string;
  total: number;
  subs: { name: string; count: number }[];
}

type SortField = "latest_nav" | "inception_date" | "fund_name" | null;
type SortDir = "asc" | "desc";

/* ─── Helpers ─── */

function FrequencyBadge({ frequency }: { frequency: string }) {
  const isDaily = frequency === "daily";
  return (
    <span className={`inline-block px-1.5 rounded text-[10px] leading-relaxed ${
      isDaily ? "bg-blue-50 text-blue-600" : "bg-amber-50 text-amber-600"
    }`}>
      {isDaily ? "日频" : "周频"}
    </span>
  );
}

function QualityBadge({ score }: { score: number | null }) {
  if (score == null) return <span className="text-[10px] text-muted-foreground">—</span>;
  const color = score >= 80 ? "bg-emerald-50 text-emerald-700" :
                score >= 60 ? "bg-blue-50 text-blue-600" :
                score >= 40 ? "bg-amber-50 text-amber-600" :
                              "bg-red-50 text-red-600";
  return (
    <span className={`inline-block px-1.5 rounded text-[10px] leading-relaxed tabular-nums font-medium ${color}`}>
      {score}
    </span>
  );
}

/* ─── Page ─── */

export default function FundDatabasePage() {
  const router = useRouter();
  const [search, setSearch] = useState("");
  // 多选策略
  const [selectedTypes, setSelectedTypes] = useState<Set<string>>(new Set());
  const [selectedSubs, setSelectedSubs] = useState<Set<string>>(new Set());
  const [expandedType, setExpandedType] = useState<string | null>(null);
  const [frequency, setFrequency] = useState("all");
  const [page, setPage] = useState(1);
  const [pageSize] = useState(50);
  const [sortField, setSortField] = useState<SortField>(null);
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const [data, setData] = useState<FundListResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // 动态策略分类
  const [categories, setCategories] = useState<StrategyCategoryItem[]>([]);

  // 加载策略分类
  useEffect(() => {
    fetchApi<StrategyCategoryItem[]>("/funds/strategy-categories")
      .then(setCategories)
      .catch((e) => console.error("加载策略分类失败:", e));
  }, []);

  // 加载基金列表
  const fetchFunds = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("page", String(page));
      params.set("page_size", String(pageSize));
      if (search) params.set("search", search);
      if (selectedTypes.size > 0) params.set("strategy_type", Array.from(selectedTypes).join(","));
      if (selectedSubs.size > 0) params.set("strategy_sub", Array.from(selectedSubs).join(","));
      if (frequency !== "all") params.set("nav_frequency", frequency);

      const result = await fetchApi<FundListResponse>(`/funds/?${params.toString()}`);
      setData(result);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "加载失败");
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, search, selectedTypes, selectedSubs, frequency]);

  useEffect(() => {
    fetchFunds();
  }, [fetchFunds]);

  // 筛选变化时重置到第1页
  useEffect(() => {
    setPage(1);
  }, [search, selectedTypes, selectedSubs, frequency]);

  const totalPages = data ? Math.ceil(data.total / pageSize) : 0;

  // 一级策略点击：多选切换
  function toggleType(name: string) {
    setSelectedTypes(prev => {
      const next = new Set(prev);
      if (next.has(name)) {
        next.delete(name);
        // 也清除该一级下的所有二级选中
        const cat = categories.find(c => c.strategy_type === name);
        if (cat) cat.subs.forEach(s => setSelectedSubs(p => { const n = new Set(p); n.delete(s.name); return n; }));
      } else {
        next.add(name);
      }
      return next;
    });
  }

  // 二级策略点击：多选切换
  function toggleSub(name: string) {
    setSelectedSubs(prev => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  }

  // 清除全部筛选
  function clearFilters() {
    setSelectedTypes(new Set());
    setSelectedSubs(new Set());
    setExpandedType(null);
  }

  // 客户端排序（当前页数据内）
  const sortedItems = (() => {
    if (!data?.items || !sortField) return data?.items ?? [];
    return [...data.items].sort((a, b) => {
      let va: string | number | null = null;
      let vb: string | number | null = null;
      if (sortField === "latest_nav") { va = a.latest_nav; vb = b.latest_nav; }
      else if (sortField === "inception_date") { va = a.inception_date; vb = b.inception_date; }
      else if (sortField === "fund_name") { va = a.fund_name; vb = b.fund_name; }
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      const cmp = va < vb ? -1 : va > vb ? 1 : 0;
      return sortDir === "asc" ? cmp : -cmp;
    });
  })();

  function toggleSort(field: SortField) {
    if (sortField === field) {
      setSortDir(d => d === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDir("desc");
    }
  }

  function SortIcon({ field }: { field: SortField }) {
    if (sortField !== field) return <ArrowUpDown className="h-3 w-3 opacity-30" />;
    return sortDir === "asc"
      ? <ArrowUp className="h-3 w-3 text-primary" />
      : <ArrowDown className="h-3 w-3 text-primary" />;
  }

  const hasFilters = selectedTypes.size > 0 || selectedSubs.size > 0;

  // 导出CSV
  async function handleExport() {
    const params = new URLSearchParams();
    if (search) params.set("search", search);
    if (selectedTypes.size > 0) params.set("strategy_type", Array.from(selectedTypes).join(","));
    if (selectedSubs.size > 0) params.set("strategy_sub", Array.from(selectedSubs).join(","));
    if (frequency !== "all") params.set("nav_frequency", frequency);

    const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";
    const url = `${API_BASE}/funds/export?${params.toString()}`;
    const res = await fetch(url);
    if (!res.ok) return;
    const blob = await res.blob();
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `funds_export_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
  }

  return (
    <div className="space-y-3">
      <PageHeader
        title="基金数据库"
        description={data ? `共 ${data.total} 只基金` : "加载中..."}
        actions={
          <div className="flex gap-1.5">
            <Link href="/fund-research/portfolio/create">
              <Button variant="outline" size="sm" className="h-7 text-[12px] gap-1 border-primary/30 text-primary hover:bg-primary/5">
                <FlaskConical className="h-3 w-3" />组合回测
              </Button>
            </Link>
            <Button variant="outline" size="sm" className="h-7 text-[12px] gap-1" onClick={handleExport}>
              <Download className="h-3 w-3" />导出
            </Button>
            <Button size="sm" className="h-7 text-[12px] gap-1" onClick={fetchFunds} disabled={loading}>
              <RefreshCw className={`h-3 w-3 ${loading ? "animate-spin" : ""}`} />刷新数据
            </Button>
          </div>
        }
      />

      {/* 筛选栏 */}
      <div className="bg-card border border-border rounded px-3 py-2 space-y-2">
        {/* 搜索 + 频率 */}
        <div className="flex items-center gap-2 flex-wrap">
          <div className="relative w-52">
            <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="搜索基金名称/备案号"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-8 h-7 text-[12px]"
            />
          </div>
          <div className="h-4 w-px bg-border" />
          <div className="flex gap-0.5">
            {[
              { key: "all", label: "全部" },
              { key: "daily", label: "日频" },
              { key: "weekly", label: "周频" },
            ].map((f) => (
              <button
                key={f.key}
                onClick={() => setFrequency(f.key)}
                className={`px-2 py-0.5 rounded text-[11px] transition-colors ${
                  frequency === f.key
                    ? "bg-primary text-primary-foreground"
                    : "text-muted-foreground hover:bg-muted"
                }`}
              >
                {f.label}
              </button>
            ))}
          </div>
          {hasFilters && (
            <>
              <div className="h-4 w-px bg-border" />
              <button onClick={clearFilters} className="text-[11px] text-destructive hover:underline">
                清除策略筛选
              </button>
            </>
          )}
        </div>

        {/* 一级策略标签（多选） */}
        <div className="flex items-center gap-1 flex-wrap">
          <span className="text-[10px] text-muted-foreground mr-1 shrink-0">一级策略:</span>
          {categories.map(cat => {
            const selected = selectedTypes.has(cat.strategy_type);
            const isExpanded = expandedType === cat.strategy_type;
            return (
              <div key={cat.strategy_type} className="relative">
                <button
                  onClick={() => toggleType(cat.strategy_type)}
                  onContextMenu={(e) => { e.preventDefault(); setExpandedType(isExpanded ? null : cat.strategy_type); }}
                  className={`inline-flex items-center gap-0.5 px-2 py-0.5 rounded text-[11px] transition-colors ${
                    selected
                      ? "bg-primary text-primary-foreground"
                      : "text-muted-foreground hover:bg-muted border border-transparent hover:border-border"
                  }`}
                >
                  {cat.strategy_type}
                  <span className="text-[9px] opacity-60">{cat.total}</span>
                  {cat.subs.length > 0 && (
                    <ChevronDown
                      className={`h-3 w-3 opacity-50 cursor-pointer transition-transform ${isExpanded ? "rotate-180" : ""}`}
                      onClick={(e) => { e.stopPropagation(); setExpandedType(isExpanded ? null : cat.strategy_type); }}
                    />
                  )}
                </button>
                {/* 二级策略下拉 */}
                {isExpanded && cat.subs.length > 0 && (
                  <div className="absolute z-50 top-full mt-1 left-0 bg-card border border-border rounded shadow-lg py-1 min-w-[140px]">
                    {cat.subs.map(sub => (
                      <button
                        key={sub.name}
                        onClick={() => toggleSub(sub.name)}
                        className={`w-full text-left px-3 py-1 text-[11px] hover:bg-muted/50 flex items-center justify-between ${
                          selectedSubs.has(sub.name) ? "text-primary font-medium" : "text-foreground"
                        }`}
                      >
                        <span>{sub.name}</span>
                        <span className="text-[9px] text-muted-foreground">{sub.count}</span>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* 已选二级策略标签 */}
        {selectedSubs.size > 0 && (
          <div className="flex items-center gap-1 flex-wrap">
            <span className="text-[10px] text-muted-foreground mr-1 shrink-0">二级策略:</span>
            {Array.from(selectedSubs).map(name => (
              <span
                key={name}
                className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] bg-primary/10 text-primary border border-primary/20"
              >
                {name}
                <button onClick={() => toggleSub(name)} className="hover:opacity-60">&times;</button>
              </span>
            ))}
          </div>
        )}
      </div>

      {/* 错误提示 */}
      {error && (
        <div className="bg-destructive/10 border border-destructive/20 rounded px-4 py-2 text-[12px] text-destructive">
          {error}
          <button className="ml-2 underline" onClick={fetchFunds}>重试</button>
        </div>
      )}

      {/* 数据表格 */}
      <div className="bg-card border border-border rounded overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow className="bg-muted/40">
              <TableHead className="h-7 text-[11px] font-normal w-8 text-center">#</TableHead>
              <TableHead className="h-7 text-[11px] font-normal cursor-pointer select-none" onClick={() => toggleSort("fund_name")}>
                <span className="flex items-center gap-1">基金名称 <SortIcon field="fund_name" /></span>
              </TableHead>
              <TableHead className="h-7 text-[11px] font-normal">管理人</TableHead>
              <TableHead className="h-7 text-[11px] font-normal">策略</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-right cursor-pointer select-none" onClick={() => toggleSort("latest_nav")}>
                <span className="flex items-center justify-end gap-1">最新净值 <SortIcon field="latest_nav" /></span>
              </TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-center">频率</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-center">净值日期</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-center cursor-pointer select-none" onClick={() => toggleSort("inception_date")}>
                <span className="flex items-center justify-center gap-1">成立日期 <SortIcon field="inception_date" /></span>
              </TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-center">质量</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-center">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow>
                <TableCell colSpan={10} className="h-32 text-center">
                  <Loader2 className="h-5 w-5 animate-spin mx-auto text-muted-foreground" />
                  <p className="text-[12px] text-muted-foreground mt-1">加载中...</p>
                </TableCell>
              </TableRow>
            ) : sortedItems.length === 0 ? (
              <TableRow>
                <TableCell colSpan={10} className="h-20 text-center text-[12px] text-muted-foreground">
                  暂无符合条件的基金数据
                </TableCell>
              </TableRow>
            ) : (
              sortedItems.map((f, i) => (
                <TableRow
                  key={f.id}
                  className="text-[12px] hover:bg-muted/20 cursor-pointer"
                  onClick={() => router.push(`/fund-database/${f.id}`)}
                >
                  <TableCell className="py-1.5 text-center text-muted-foreground tabular-nums">
                    {(page - 1) * pageSize + i + 1}
                  </TableCell>
                  <TableCell className="py-1.5">
                    <span className="text-primary hover:underline">{f.fund_name}</span>
                    {f.filing_number && (
                      <span className="ml-1.5 text-[10px] text-muted-foreground">{f.filing_number}</span>
                    )}
                  </TableCell>
                  <TableCell className="py-1.5 text-muted-foreground max-w-[120px] truncate">
                    {f.manager_name || "—"}
                  </TableCell>
                  <TableCell className="py-1.5">
                    <span className="text-muted-foreground">{f.strategy_type || "—"}</span>
                    {f.strategy_sub && (
                      <span className="ml-1 text-[10px] text-muted-foreground/60">/{f.strategy_sub}</span>
                    )}
                  </TableCell>
                  <TableCell className="py-1.5 text-right tabular-nums font-medium">
                    {f.latest_nav != null ? Number(f.latest_nav).toFixed(4) : "—"}
                  </TableCell>
                  <TableCell className="py-1.5 text-center">
                    <FrequencyBadge frequency={f.nav_frequency || "daily"} />
                  </TableCell>
                  <TableCell className="py-1.5 text-center tabular-nums text-muted-foreground text-[11px]">
                    {f.latest_nav_date || "—"}
                  </TableCell>
                  <TableCell className="py-1.5 text-center tabular-nums text-muted-foreground text-[11px]">
                    {f.inception_date || "—"}
                  </TableCell>
                  <TableCell className="py-1.5 text-center">
                    <QualityBadge score={f.data_quality_score} />
                  </TableCell>
                  <TableCell className="py-1.5 text-center">
                    <button
                      className="text-[11px] text-primary hover:underline"
                      onClick={(e) => { e.stopPropagation(); router.push(`/fund-database/${f.id}`); }}
                    >
                      详情
                    </button>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>

        {/* 分页 */}
        <div className="flex items-center justify-between px-4 py-1.5 border-t border-border bg-muted/20 text-[11px] text-muted-foreground">
          <span>
            共 {data?.total ?? 0} 条 · 第 {page}/{totalPages || 1} 页
          </span>
          <div className="flex items-center gap-1">
            <Button
              variant="ghost" size="sm" className="h-6 w-6 p-0"
              disabled={page <= 1}
              onClick={() => setPage(p => p - 1)}
            >
              <ChevronLeft className="h-3.5 w-3.5" />
            </Button>
            {Array.from({ length: Math.min(totalPages, 7) }, (_, i) => {
              let pageNum: number;
              if (totalPages <= 7) {
                pageNum = i + 1;
              } else if (page <= 4) {
                pageNum = i + 1;
              } else if (page >= totalPages - 3) {
                pageNum = totalPages - 6 + i;
              } else {
                pageNum = page - 3 + i;
              }
              return (
                <Button
                  key={pageNum}
                  variant={page === pageNum ? "default" : "ghost"}
                  size="sm" className="h-6 w-6 p-0 text-[11px]"
                  onClick={() => setPage(pageNum)}
                >
                  {pageNum}
                </Button>
              );
            })}
            <Button
              variant="ghost" size="sm" className="h-6 w-6 p-0"
              disabled={page >= totalPages}
              onClick={() => setPage(p => p + 1)}
            >
              <ChevronRight className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
