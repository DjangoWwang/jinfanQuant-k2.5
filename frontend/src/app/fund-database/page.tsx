"use client";

import { useState } from "react";
import { Search, Download, RefreshCw } from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { Fund } from "@/types/fund";

/* ─── Mock Data ─── */

const mockFunds: (Fund & { cumReturn: number; weekReturn: number; monthReturn: number; ytdReturn: number; maxDD: number })[] = [
  { id: 1, fund_name: "明河价值精选1期", filing_number: "SMA123", manager_name: "张远明", strategy_type: "股票多头", strategy_sub: "价值型", nav_frequency: "daily", latest_nav: 1.5432, inception_date: "2021-03-15", is_private: true, cumReturn: 0.5432, weekReturn: 0.0123, monthReturn: 0.0345, ytdReturn: 0.0892, maxDD: -0.0812 },
  { id: 2, fund_name: "聚鸣多策略1号", filing_number: "SMB456", manager_name: "刘晓东", strategy_type: "多策略", strategy_sub: null, nav_frequency: "weekly", latest_nav: 1.2018, inception_date: "2020-06-22", is_private: true, cumReturn: 0.2018, weekReturn: -0.0034, monthReturn: 0.0167, ytdReturn: 0.0523, maxDD: -0.0634 },
  { id: 3, fund_name: "幻方量化超体500", filing_number: "SMC789", manager_name: "徐进", strategy_type: "量化多头", strategy_sub: "指增", nav_frequency: "daily", latest_nav: 1.8765, inception_date: "2022-01-10", is_private: true, cumReturn: 0.8765, weekReturn: 0.0089, monthReturn: 0.0234, ytdReturn: 0.1245, maxDD: -0.1234 },
  { id: 4, fund_name: "九坤宏观对冲2号", filing_number: "SMD012", manager_name: "王琛", strategy_type: "宏观对冲", strategy_sub: null, nav_frequency: "weekly", latest_nav: 0.9834, inception_date: "2019-11-05", is_private: true, cumReturn: -0.0166, weekReturn: -0.0012, monthReturn: -0.0056, ytdReturn: -0.0123, maxDD: -0.1567 },
  { id: 5, fund_name: "衍复CTA精选3期", filing_number: "SME345", manager_name: "高亢", strategy_type: "CTA", strategy_sub: "趋势", nav_frequency: "daily", latest_nav: 1.3421, inception_date: "2023-04-18", is_private: true, cumReturn: 0.3421, weekReturn: 0.0045, monthReturn: 0.0189, ytdReturn: 0.0678, maxDD: -0.0923 },
  { id: 6, fund_name: "诚奇量化对冲1号", filing_number: "SMF678", manager_name: "何文奇", strategy_type: "市场中性", strategy_sub: null, nav_frequency: "daily", latest_nav: 1.1234, inception_date: "2022-08-01", is_private: true, cumReturn: 0.1234, weekReturn: 0.0023, monthReturn: 0.0089, ytdReturn: 0.0345, maxDD: -0.0345 },
  { id: 7, fund_name: "白鹭精选成长2期", filing_number: "SMG901", manager_name: "陈磊", strategy_type: "股票多头", strategy_sub: "成长型", nav_frequency: "daily", latest_nav: 1.4567, inception_date: "2021-09-28", is_private: true, cumReturn: 0.4567, weekReturn: 0.0156, monthReturn: 0.0412, ytdReturn: 0.0934, maxDD: -0.0978 },
  { id: 8, fund_name: "启林量化中性5号", filing_number: "SMH234", manager_name: "方锐", strategy_type: "市场中性", strategy_sub: null, nav_frequency: "weekly", latest_nav: 1.0892, inception_date: "2023-01-15", is_private: true, cumReturn: 0.0892, weekReturn: 0.0008, monthReturn: 0.0034, ytdReturn: 0.0212, maxDD: -0.0234 },
  { id: 9, fund_name: "因诺资产CTA1号", filing_number: "SMI567", manager_name: "徐书楠", strategy_type: "CTA", strategy_sub: "混合", nav_frequency: "daily", latest_nav: 1.2567, inception_date: "2022-05-20", is_private: true, cumReturn: 0.2567, weekReturn: 0.0056, monthReturn: 0.0145, ytdReturn: 0.0534, maxDD: -0.0745 },
  { id: 10, fund_name: "盘京全球配置3号", filing_number: "SMJ890", manager_name: "庄涛", strategy_type: "宏观对冲", strategy_sub: "全球", nav_frequency: "weekly", latest_nav: 1.1789, inception_date: "2020-12-01", is_private: true, cumReturn: 0.1789, weekReturn: 0.0034, monthReturn: 0.0123, ytdReturn: 0.0456, maxDD: -0.0856 },
];

const strategyTypes = ["全部", "股票多头", "量化多头", "市场中性", "多策略", "CTA", "宏观对冲", "债券"];

function pct(v: number) {
  const s = (v * 100).toFixed(2);
  return v >= 0 ? `+${s}%` : `${s}%`;
}

/* ─── Page ─── */

export default function FundDatabasePage() {
  const [search, setSearch] = useState("");
  const [strategy, setStrategy] = useState("全部");
  const [frequency, setFrequency] = useState("all");

  const filtered = mockFunds.filter((f) => {
    const ms = !search || f.fund_name.includes(search) || f.filing_number.toLowerCase().includes(search.toLowerCase());
    const mt = strategy === "全部" || f.strategy_type === strategy;
    const mf = frequency === "all" || f.nav_frequency === frequency;
    return ms && mt && mf;
  });

  return (
    <div className="space-y-3">
      <PageHeader
        title="基金数据库"
        description={`共 ${filtered.length} 只基金`}
        actions={
          <div className="flex gap-1.5">
            <Button variant="outline" size="sm" className="h-7 text-[12px] gap-1">
              <Download className="h-3 w-3" />导出
            </Button>
            <Button size="sm" className="h-7 text-[12px] gap-1">
              <RefreshCw className="h-3 w-3" />刷新数据
            </Button>
          </div>
        }
      />

      {/* Filters */}
      <div className="bg-card border border-border rounded px-3 py-2 space-y-2">
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
            {strategyTypes.map((s) => (
              <button
                key={s}
                onClick={() => setStrategy(s)}
                className={`px-2 py-0.5 rounded text-[11px] transition-colors ${
                  strategy === s
                    ? "bg-primary text-primary-foreground"
                    : "text-muted-foreground hover:bg-muted"
                }`}
              >
                {s}
              </button>
            ))}
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
        </div>
      </div>

      {/* Data Table */}
      <div className="bg-card border border-border rounded overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow className="bg-muted/40">
              <TableHead className="h-7 text-[11px] font-normal w-8 text-center">#</TableHead>
              <TableHead className="h-7 text-[11px] font-normal">基金名称</TableHead>
              <TableHead className="h-7 text-[11px] font-normal">基金经理</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-right">最新净值</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-center">频率</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-right">近一周</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-right">近一月</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-right">今年以来</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-right">累计收益</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-right">最大回撤</TableHead>
              <TableHead className="h-7 text-[11px] font-normal text-center">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filtered.length === 0 ? (
              <TableRow>
                <TableCell colSpan={11} className="h-20 text-center text-[12px] text-muted-foreground">
                  暂无符合条件的基金数据
                </TableCell>
              </TableRow>
            ) : (
              filtered.map((f, i) => (
                <TableRow key={f.id} className="text-[12px] hover:bg-muted/20">
                  <TableCell className="py-1.5 text-center text-muted-foreground tabular-nums">{i + 1}</TableCell>
                  <TableCell className="py-1.5">
                    <span className="text-primary cursor-pointer hover:underline">{f.fund_name}</span>
                    <span className="ml-1.5 text-[10px] text-muted-foreground">{f.strategy_type}</span>
                  </TableCell>
                  <TableCell className="py-1.5 text-muted-foreground">{f.manager_name}</TableCell>
                  <TableCell className="py-1.5 text-right tabular-nums font-medium">{f.latest_nav?.toFixed(4) ?? "—"}</TableCell>
                  <TableCell className="py-1.5 text-center">
                    <span className={`inline-block px-1.5 rounded text-[10px] leading-relaxed ${
                      f.nav_frequency === "daily" ? "bg-blue-50 text-blue-600" : "bg-amber-50 text-amber-600"
                    }`}>
                      {f.nav_frequency === "daily" ? "日频" : "周频"}
                    </span>
                  </TableCell>
                  <TableCell className={`py-1.5 text-right tabular-nums ${f.weekReturn >= 0 ? "text-jf-up" : "text-jf-down"}`}>{pct(f.weekReturn)}</TableCell>
                  <TableCell className={`py-1.5 text-right tabular-nums ${f.monthReturn >= 0 ? "text-jf-up" : "text-jf-down"}`}>{pct(f.monthReturn)}</TableCell>
                  <TableCell className={`py-1.5 text-right tabular-nums ${f.ytdReturn >= 0 ? "text-jf-up" : "text-jf-down"}`}>{pct(f.ytdReturn)}</TableCell>
                  <TableCell className={`py-1.5 text-right tabular-nums font-medium ${f.cumReturn >= 0 ? "text-jf-up" : "text-jf-down"}`}>{pct(f.cumReturn)}</TableCell>
                  <TableCell className="py-1.5 text-right tabular-nums text-jf-down">{pct(f.maxDD)}</TableCell>
                  <TableCell className="py-1.5 text-center">
                    <button className="text-[11px] text-primary hover:underline">加入对比</button>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
        <div className="flex items-center justify-between px-4 py-1.5 border-t border-border bg-muted/20 text-[11px] text-muted-foreground">
          <span>共 {filtered.length} 条数据</span>
          <span>20 条/页</span>
        </div>
      </div>
    </div>
  );
}
