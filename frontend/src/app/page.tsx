"use client";

import {
  TrendingUp,
  TrendingDown,
  BarChart3,
  Eye,
  Boxes,
  Database,
  PieChart,
  Layers,
  GitCompareArrows,
  FileSearch,
} from "lucide-react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { PageHeader } from "@/components/layout/page-header";

/* ─── Mock Data ──────────────────────────────────────── */

const metrics = [
  {
    label: "实盘产品净值",
    value: "1.2345",
    change: "+0.38%",
    positive: true,
    icon: BarChart3,
  },
  {
    label: "今日涨跌",
    value: "+0.0047",
    change: "+0.38%",
    positive: true,
    icon: TrendingUp,
  },
  {
    label: "关注基金数",
    value: "128",
    change: "+3",
    positive: true,
    icon: Eye,
  },
  {
    label: "组合数量",
    value: "5",
    change: "0",
    positive: true,
    icon: Boxes,
  },
];

const watchlistFunds = [
  {
    name: "明河价值精选1期",
    returnPct: "+2.34%",
    positive: true,
    tag: "股票多头",
  },
  {
    name: "聚鸣多策略1号",
    returnPct: "-0.87%",
    positive: false,
    tag: "多策略",
  },
  {
    name: "幻方量化超体500",
    returnPct: "+1.12%",
    positive: true,
    tag: "量化多头",
  },
  {
    name: "九坤宏观对冲2号",
    returnPct: "-0.45%",
    positive: false,
    tag: "宏观对冲",
  },
];

const quickActions = [
  { label: "基金数据库", icon: Database, href: "/fund-database" },
  { label: "基金池管理", icon: Layers, href: "/fund-research/pools" },
  { label: "基金比较", icon: GitCompareArrows, href: "/fund-research/comparison" },
  { label: "组合分析", icon: PieChart, href: "/fund-research/portfolio" },
];

/* ─── Dashboard Page ─────────────────────────────────── */

export default function DashboardPage() {
  return (
    <div className="mx-auto max-w-7xl space-y-6">
      <PageHeader
        title="概览"
        description="欢迎回来，这是您的投研工作台"
      />

      {/* Row 1: Metric Cards */}
      <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 xl:grid-cols-4">
        {metrics.map((m) => (
          <Card
            key={m.label}
            className="rounded-xl shadow-md hover:shadow-lg transition-shadow"
          >
            <CardContent className="flex items-start justify-between px-5 py-5">
              <div className="space-y-2">
                <p className="text-sm text-muted-foreground">{m.label}</p>
                <p className="font-tabular text-2xl font-bold tracking-tight">
                  {m.value}
                </p>
                <span
                  className={`inline-flex items-center gap-1 text-xs font-medium ${
                    m.positive ? "text-jf-positive" : "text-jf-negative"
                  }`}
                >
                  {m.positive ? (
                    <TrendingUp className="h-3 w-3" />
                  ) : (
                    <TrendingDown className="h-3 w-3" />
                  )}
                  {m.change}
                </span>
              </div>
              <div className="rounded-lg bg-primary/10 p-2.5">
                <m.icon className="h-5 w-5 text-primary" />
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Row 2: NAV Chart Placeholder */}
      <Card className="rounded-xl shadow-md">
        <CardHeader>
          <CardTitle className="text-base font-semibold">
            产品净值走势
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex h-64 items-center justify-center rounded-lg border border-dashed border-border bg-muted/40">
            <div className="text-center space-y-2">
              <BarChart3 className="mx-auto h-10 w-10 text-muted-foreground/50" />
              <p className="text-sm text-muted-foreground">
                净值走势图 - 接入数据后展示
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Row 3: Watchlist + Quick Actions */}
      <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
        {/* Fund Watchlist */}
        <Card className="rounded-xl shadow-md">
          <CardHeader>
            <CardTitle className="text-base font-semibold">
              关注基金异动
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-1 px-5">
            {watchlistFunds.map((fund) => (
              <div
                key={fund.name}
                className="flex items-center justify-between rounded-lg px-3 py-3 transition-colors hover:bg-muted/50"
              >
                <div className="flex items-center gap-3">
                  <FileSearch className="h-4 w-4 text-muted-foreground" />
                  <span className="text-sm font-medium">{fund.name}</span>
                  <Badge variant="secondary" className="text-[11px]">
                    {fund.tag}
                  </Badge>
                </div>
                <span
                  className={`font-tabular text-sm font-semibold ${
                    fund.positive ? "text-jf-positive" : "text-jf-negative"
                  }`}
                >
                  {fund.returnPct}
                </span>
              </div>
            ))}
          </CardContent>
        </Card>

        {/* Quick Actions */}
        <Card className="rounded-xl shadow-md">
          <CardHeader>
            <CardTitle className="text-base font-semibold">
              快捷操作
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-3">
              {quickActions.map((action) => (
                <Button
                  key={action.label}
                  variant="outline"
                  className="flex h-auto flex-col items-center gap-2.5 rounded-xl border-border/60 px-4 py-5 hover:border-primary/30 hover:bg-primary/5"
                  asChild
                >
                  <a href={action.href}>
                    <action.icon className="h-6 w-6 text-primary" />
                    <span className="text-sm font-medium">{action.label}</span>
                  </a>
                </Button>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
