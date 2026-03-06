"use client";

import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { Search, Plus, Trash2, Loader2 } from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { fetchApi } from "@/lib/api";

/* ─── Types ─── */

interface PoolCounts {
  basic: number;
  watch: number;
  investment: number;
}

interface PoolFundItem {
  id: number;
  fund_id: number;
  fund_name: string | null;
  pool_type: string;
  notes: string | null;
  added_at: string | null;
}

interface PoolListResponse {
  pool_type: string;
  total: number;
  page: number;
  page_size: number;
  items: PoolFundItem[];
}

interface FundSearchItem {
  id: number;
  fund_name: string;
  strategy_type: string | null;
  nav_frequency: string;
}

const poolTabs = [
  { key: "basic", label: "基础池", desc: "全部入库基金" },
  { key: "watch", label: "观察池", desc: "关注跟踪的基金" },
  { key: "investment", label: "投资池", desc: "实际投资/拟投资基金" },
];

/* ─── Page ─── */

export default function FundPoolsPage() {
  const router = useRouter();
  const [activeTab, setActiveTab] = useState("basic");
  const [counts, setCounts] = useState<PoolCounts>({ basic: 0, watch: 0, investment: 0 });
  const [poolData, setPoolData] = useState<PoolListResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);

  // 搜索添加
  const [search, setSearch] = useState("");
  const [searchResults, setSearchResults] = useState<FundSearchItem[]>([]);
  const [searching, setSearching] = useState(false);
  const [adding, setAdding] = useState(false);

  // 加载计数
  useEffect(() => {
    fetchApi<PoolCounts>("/pools/counts").then(setCounts).catch(() => {});
  }, [poolData]);

  // 加载池子内容
  const loadPool = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetchApi<PoolListResponse>(`/pools/${activeTab}?page=${page}&page_size=50`);
      setPoolData(res);
    } catch {
      setPoolData(null);
    } finally {
      setLoading(false);
    }
  }, [activeTab, page]);

  useEffect(() => {
    loadPool();
  }, [loadPool]);

  useEffect(() => {
    setPage(1);
  }, [activeTab]);

  // 搜索基金
  useEffect(() => {
    if (!search.trim()) {
      setSearchResults([]);
      return;
    }
    const timer = setTimeout(async () => {
      setSearching(true);
      try {
        const res = await fetchApi<{ items: FundSearchItem[] }>(`/funds/?search=${encodeURIComponent(search)}&page_size=8`);
        setSearchResults(res.items);
      } catch {
        setSearchResults([]);
      } finally {
        setSearching(false);
      }
    }, 300);
    return () => clearTimeout(timer);
  }, [search]);

  async function addToPool(fundId: number) {
    setAdding(true);
    try {
      await fetchApi(`/pools/${activeTab}/funds`, {
        method: "POST",
        body: JSON.stringify({ fund_id: fundId }),
      });
      setSearch("");
      setSearchResults([]);
      loadPool();
    } catch {
      // 可能已存在
    } finally {
      setAdding(false);
    }
  }

  async function removeFromPool(fundId: number) {
    try {
      await fetchApi(`/pools/${activeTab}/funds/${fundId}`, { method: "DELETE" });
      loadPool();
    } catch {
      // ignore
    }
  }

  return (
    <div className="space-y-3">
      <PageHeader title="基金池" description="管理基础池、观察池、投资池" />

      {/* Tab栏 */}
      <div className="flex gap-1">
        {poolTabs.map(tab => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`px-3 py-1.5 rounded text-[12px] transition-colors ${
              activeTab === tab.key
                ? "bg-primary text-primary-foreground shadow-sm"
                : "bg-muted text-muted-foreground hover:bg-muted/80"
            }`}
          >
            {tab.label}
            <span className="ml-1.5 text-[10px] opacity-70">
              {counts[tab.key as keyof PoolCounts]}
            </span>
          </button>
        ))}
      </div>

      {/* 添加基金（观察池/投资池） */}
      {activeTab !== "basic" && (
        <div className="bg-card border border-border rounded px-4 py-2.5">
          <div className="relative max-w-sm">
            <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="搜索并添加基金到此池..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-8 h-8 text-[12px]"
            />
            {searching && <Loader2 className="absolute right-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 animate-spin text-muted-foreground" />}
            {searchResults.length > 0 && (
              <div className="absolute z-50 top-full mt-1 left-0 w-full bg-card border border-border rounded shadow-lg max-h-48 overflow-y-auto">
                {searchResults.map(f => (
                  <button
                    key={f.id}
                    className="w-full text-left px-3 py-1.5 text-[12px] hover:bg-muted/50 flex items-center justify-between"
                    onClick={() => addToPool(f.id)}
                    disabled={adding}
                  >
                    <span>{f.fund_name}</span>
                    <Plus className="h-3.5 w-3.5 text-primary" />
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* 表格 */}
      <div className="bg-card border border-border rounded overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow className="bg-muted/40">
              <TableHead className="h-7 text-[11px] font-normal w-8 text-center">#</TableHead>
              <TableHead className="h-7 text-[11px] font-normal">基金名称</TableHead>
              <TableHead className="h-7 text-[11px] font-normal">备注</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-center">添加时间</TableHead>
              {activeTab !== "basic" && (
                <TableHead className="h-7 text-[11px] font-normal text-center w-16">操作</TableHead>
              )}
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow>
                <TableCell colSpan={5} className="h-32 text-center">
                  <Loader2 className="h-5 w-5 animate-spin mx-auto text-muted-foreground" />
                </TableCell>
              </TableRow>
            ) : !poolData?.items?.length ? (
              <TableRow>
                <TableCell colSpan={5} className="h-20 text-center text-[12px] text-muted-foreground">
                  此基金池暂无数据
                </TableCell>
              </TableRow>
            ) : (
              poolData.items.map((item, i) => (
                <TableRow key={item.id} className="text-[12px] hover:bg-muted/20">
                  <TableCell className="py-1.5 text-center text-muted-foreground tabular-nums">
                    {(page - 1) * 50 + i + 1}
                  </TableCell>
                  <TableCell className="py-1.5">
                    <span
                      className="text-primary cursor-pointer hover:underline"
                      onClick={() => router.push(`/fund-database/${item.fund_id}`)}
                    >
                      {item.fund_name || `#${item.fund_id}`}
                    </span>
                  </TableCell>
                  <TableCell className="py-1.5 text-muted-foreground">{item.notes || "—"}</TableCell>
                  <TableCell className="py-1.5 text-center tabular-nums text-muted-foreground text-[11px]">
                    {item.added_at ? item.added_at.slice(0, 10) : "—"}
                  </TableCell>
                  {activeTab !== "basic" && (
                    <TableCell className="py-1.5 text-center">
                      <button
                        className="text-destructive hover:text-destructive/80"
                        onClick={() => removeFromPool(item.fund_id)}
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                      </button>
                    </TableCell>
                  )}
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
        <div className="flex items-center justify-between px-4 py-1.5 border-t border-border bg-muted/20 text-[11px] text-muted-foreground">
          <span>共 {poolData?.total ?? 0} 条</span>
        </div>
      </div>
    </div>
  );
}
