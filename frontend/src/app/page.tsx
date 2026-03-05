"use client";

import {
  TrendingUp,
  TrendingDown,
  ArrowUpRight,
  ArrowDownRight,
  BarChart3,
  Activity,
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

/* ─── Mock Data ─── */

const productSummary = {
  name: "博孚利鹭岛晋帆FOF",
  nav: 1.2345,
  dailyReturn: 0.0038,
  weekReturn: 0.0124,
  monthReturn: 0.0367,
  ytdReturn: 0.0892,
  maxDrawdown: -0.0234,
  sharpe: 2.31,
  navDate: "2026-03-04",
};

const subFunds = [
  { name: "明河价值精选1期", weight: "15.2%", nav: 1.5432, weekRet: "+1.23%", monthRet: "+3.45%", ytdRet: "+8.92%" },
  { name: "聚鸣多策略1号", weight: "12.8%", nav: 1.2018, weekRet: "-0.34%", monthRet: "+1.67%", ytdRet: "+5.23%" },
  { name: "幻方量化超体500", weight: "18.5%", nav: 1.8765, weekRet: "+0.89%", monthRet: "+2.34%", ytdRet: "+12.45%" },
  { name: "九坤宏观对冲2号", weight: "10.3%", nav: 0.9834, weekRet: "-0.12%", monthRet: "-0.56%", ytdRet: "-1.23%" },
  { name: "衍复CTA精选3期", weight: "8.6%", nav: 1.3421, weekRet: "+0.45%", monthRet: "+1.89%", ytdRet: "+6.78%" },
  { name: "诚奇量化对冲1号", weight: "11.2%", nav: 1.1234, weekRet: "+0.23%", monthRet: "+0.89%", ytdRet: "+3.45%" },
  { name: "白鹭精选成长2期", weight: "13.4%", nav: 1.4567, weekRet: "+1.56%", monthRet: "+4.12%", ytdRet: "+9.34%" },
  { name: "启林量化中性5号", weight: "10.0%", nav: 1.0892, weekRet: "+0.08%", monthRet: "+0.34%", ytdRet: "+2.12%" },
];

const watchAlerts = [
  { name: "明河价值精选1期", change: "+2.34%", reason: "近一周涨幅超2%", positive: true },
  { name: "九坤宏观对冲2号", change: "-1.87%", reason: "连续3周下跌", positive: false },
  { name: "幻方量化超体500", change: "+1.12%", reason: "创历史新高", positive: true },
  { name: "衍复CTA精选3期", change: "-0.45%", reason: "回撤接近-5%阈值", positive: false },
];

/* ─── Metric Cell ─── */

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

/* ─── Page ─── */

export default function DashboardPage() {
  return (
    <div className="space-y-3">
      <PageHeader title="概览" description={`数据截至 ${productSummary.navDate}`} />

      {/* Product Summary Strip */}
      <div className="bg-card border border-border rounded">
        <div className="flex items-center border-b border-border px-4 py-1.5">
          <span className="text-[13px] font-semibold">{productSummary.name}</span>
          <Badge className="ml-2 text-[10px] bg-primary/10 text-primary border-primary/20 hover:bg-primary/10">实盘</Badge>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 divide-x divide-border">
          <MetricCell label="最新净值" value={productSummary.nav.toFixed(4)} sub={`${(productSummary.dailyReturn * 100).toFixed(2)}%`} up={productSummary.dailyReturn > 0} />
          <MetricCell label="近一周" value={`${(productSummary.weekReturn * 100).toFixed(2)}%`} up={productSummary.weekReturn > 0} />
          <MetricCell label="近一月" value={`${(productSummary.monthReturn * 100).toFixed(2)}%`} up={productSummary.monthReturn > 0} />
          <MetricCell label="今年以来" value={`${(productSummary.ytdReturn * 100).toFixed(2)}%`} up={productSummary.ytdReturn > 0} />
          <MetricCell label="最大回撤" value={`${(productSummary.maxDrawdown * 100).toFixed(2)}%`} />
          <MetricCell label="夏普比率" value={productSummary.sharpe.toFixed(2)} />
          <MetricCell label="基金入库" value="128只" sub="日频96 / 周频32" />
        </div>
      </div>

      {/* Chart Placeholder */}
      <div className="bg-card border border-border rounded">
        <div className="flex items-center justify-between px-4 py-1.5 border-b border-border">
          <span className="text-[13px] font-medium">净值走势</span>
          <div className="flex gap-0.5">
            {["1M", "3M", "6M", "YTD", "1Y", "ALL"].map((p) => (
              <Button key={p} variant={p === "YTD" ? "default" : "ghost"} size="sm" className="h-6 px-2 text-[11px]">
                {p}
              </Button>
            ))}
          </div>
        </div>
        <div className="h-48 flex items-center justify-center text-muted-foreground">
          <div className="text-center space-y-1">
            <BarChart3 className="mx-auto h-7 w-7 opacity-25" />
            <p className="text-[12px] opacity-60">接入数据后展示净值走势图</p>
          </div>
        </div>
      </div>

      {/* Holdings + Alerts */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
        {/* Holdings Table */}
        <div className="lg:col-span-2 bg-card border border-border rounded">
          <div className="flex items-center justify-between px-4 py-1.5 border-b border-border">
            <span className="text-[13px] font-medium">子基金持仓</span>
            <span className="text-[11px] text-muted-foreground">{subFunds.length} 只</span>
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="h-7 text-[11px] font-normal">基金名称</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-right">权重</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-right">净值</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-right">近一周</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-right">近一月</TableHead>
                <TableHead className="h-7 text-[11px] font-normal text-right">今年以来</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {subFunds.map((f) => (
                <TableRow key={f.name} className="text-[12px]">
                  <TableCell className="py-1.5 text-primary cursor-pointer hover:underline">{f.name}</TableCell>
                  <TableCell className="py-1.5 text-right tabular-nums text-muted-foreground">{f.weight}</TableCell>
                  <TableCell className="py-1.5 text-right tabular-nums font-medium">{f.nav.toFixed(4)}</TableCell>
                  <TableCell className={`py-1.5 text-right tabular-nums ${f.weekRet.startsWith("-") ? "text-jf-down" : "text-jf-up"}`}>{f.weekRet}</TableCell>
                  <TableCell className={`py-1.5 text-right tabular-nums ${f.monthRet.startsWith("-") ? "text-jf-down" : "text-jf-up"}`}>{f.monthRet}</TableCell>
                  <TableCell className={`py-1.5 text-right tabular-nums ${f.ytdRet.startsWith("-") ? "text-jf-down" : "text-jf-up"}`}>{f.ytdRet}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        {/* Alerts */}
        <div className="bg-card border border-border rounded">
          <div className="flex items-center justify-between px-4 py-1.5 border-b border-border">
            <span className="text-[13px] font-medium">基金异动</span>
            <Activity className="h-3.5 w-3.5 text-muted-foreground opacity-50" />
          </div>
          <div className="divide-y divide-border">
            {watchAlerts.map((a) => (
              <div key={a.name} className="px-4 py-2 hover:bg-muted/30 transition-colors cursor-pointer">
                <div className="flex items-center justify-between">
                  <span className="text-[12px] font-medium">{a.name}</span>
                  <span className={`tabular-nums text-[12px] font-semibold flex items-center gap-0.5 ${a.positive ? "text-jf-up" : "text-jf-down"}`}>
                    {a.positive ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
                    {a.change}
                  </span>
                </div>
                <div className="text-[11px] text-muted-foreground mt-0.5">{a.reason}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
