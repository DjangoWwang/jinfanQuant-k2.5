"use client";

import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import {
  Plus, Loader2, PieChart, ChevronRight, Trash2,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { fetchApi } from "@/lib/api";

/* --- Types --- */

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

/* --- Page --- */

export default function PortfolioListPage() {
  const router = useRouter();
  const [portfolios, setPortfolios] = useState<PortfolioItem[]>([]);
  const [loading, setLoading] = useState(true);

  const loadPortfolios = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetchApi<{ items: PortfolioItem[] }>("/portfolios/");
      setPortfolios(res.items);
    } catch {
      setPortfolios([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadPortfolios();
  }, [loadPortfolios]);

  const rebalanceLabel: Record<string, string> = {
    daily: "每日", weekly: "每周", monthly: "每月", quarterly: "每季度",
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="组合研究"
        description="构建FOF组合、配置权重、运行回测"
        actions={
          <Button
            size="sm"
            className="h-8 text-[12px] gap-1.5 px-4"
            onClick={() => router.push("/fund-research/portfolio/create")}
          >
            <Plus className="h-3.5 w-3.5" />新建组合
          </Button>
        }
      />

      <div className="bg-card border border-border rounded overflow-hidden">
        {loading ? (
          <div className="h-40 flex items-center justify-center">
            <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
          </div>
        ) : portfolios.length === 0 ? (
          <div className="h-48 flex flex-col items-center justify-center text-muted-foreground gap-3">
            <PieChart className="h-10 w-10 opacity-20" />
            <div className="text-center">
              <p className="text-[13px] mb-1">暂无组合</p>
              <p className="text-[11px] opacity-60">点击"新建组合"开始构建FOF投资组合</p>
            </div>
            <Button
              variant="outline"
              size="sm"
              className="text-[12px] gap-1 mt-1"
              onClick={() => router.push("/fund-research/portfolio/create")}
            >
              <Plus className="h-3.5 w-3.5" />新建组合
            </Button>
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="bg-muted/40">
                <TableHead className="h-8 text-[11px] font-normal w-12 text-center">序号</TableHead>
                <TableHead className="h-8 text-[11px] font-normal">组合名称</TableHead>
                <TableHead className="h-8 text-[11px] font-normal text-center">类型</TableHead>
                <TableHead className="h-8 text-[11px] font-normal text-center">基金数</TableHead>
                <TableHead className="h-8 text-[11px] font-normal text-center">再平衡</TableHead>
                <TableHead className="h-8 text-[11px] font-normal text-center">创建时间</TableHead>
                <TableHead className="h-8 text-[11px] font-normal text-center">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {portfolios.map((p, i) => (
                <TableRow key={p.id} className="text-[12px] hover:bg-muted/20 cursor-pointer" onClick={() => router.push(`/fund-research/portfolio/${p.id}`)}>
                  <TableCell className="py-2 text-center text-muted-foreground">{i + 1}</TableCell>
                  <TableCell className="py-2 font-medium">{p.name}</TableCell>
                  <TableCell className="py-2 text-center">
                    <span className={`px-1.5 py-0.5 rounded text-[10px] ${
                      p.portfolio_type === "live" ? "bg-green-50 text-green-600" : "bg-blue-50 text-blue-600"
                    }`}>
                      {p.portfolio_type === "live" ? "实盘" : "模拟"}
                    </span>
                  </TableCell>
                  <TableCell className="py-2 text-center tabular-nums">{p.fund_count}</TableCell>
                  <TableCell className="py-2 text-center text-muted-foreground">
                    {rebalanceLabel[p.rebalance_freq] || p.rebalance_freq}
                  </TableCell>
                  <TableCell className="py-2 text-center tabular-nums text-muted-foreground text-[11px]">
                    {p.created_at ? p.created_at.slice(0, 10) : "--"}
                  </TableCell>
                  <TableCell className="py-2 text-center">
                    <button
                      className="text-[11px] text-primary hover:underline inline-flex items-center gap-0.5"
                      onClick={(e) => { e.stopPropagation(); router.push(`/fund-research/portfolio/${p.id}`); }}
                    >
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
