"use client";

import { useState } from "react";
import { Search } from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Card, CardContent } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import type { Fund } from "@/types/fund";

/* ─── Mock Data ──────────────────────────────────────── */

const mockFunds: Fund[] = [
  {
    id: 1,
    fund_name: "明河价值精选1期",
    filing_number: "SMA123",
    manager_name: "张三",
    strategy_type: "股票多头",
    strategy_sub: "价值型",
    nav_frequency: "daily",
    latest_nav: 1.5432,
    inception_date: "2021-03-15",
    is_private: true,
  },
  {
    id: 2,
    fund_name: "聚鸣多策略1号",
    filing_number: "SMB456",
    manager_name: "李四",
    strategy_type: "多策略",
    strategy_sub: null,
    nav_frequency: "weekly",
    latest_nav: 1.2018,
    inception_date: "2020-06-22",
    is_private: true,
  },
  {
    id: 3,
    fund_name: "幻方量化超体500",
    filing_number: "SMC789",
    manager_name: "王五",
    strategy_type: "量化多头",
    strategy_sub: "指增",
    nav_frequency: "daily",
    latest_nav: 1.8765,
    inception_date: "2022-01-10",
    is_private: true,
  },
  {
    id: 4,
    fund_name: "九坤宏观对冲2号",
    filing_number: "SMD012",
    manager_name: "赵六",
    strategy_type: "宏观对冲",
    strategy_sub: null,
    nav_frequency: "weekly",
    latest_nav: 0.9834,
    inception_date: "2019-11-05",
    is_private: true,
  },
  {
    id: 5,
    fund_name: "衍复CTA精选3期",
    filing_number: "SME345",
    manager_name: "钱七",
    strategy_type: "CTA",
    strategy_sub: "趋势",
    nav_frequency: "daily",
    latest_nav: 1.3421,
    inception_date: "2023-04-18",
    is_private: true,
  },
];

const strategyTypes = [
  "全部",
  "股票多头",
  "量化多头",
  "多策略",
  "CTA",
  "宏观对冲",
  "债券",
];

/* ─── Fund Database Page ─────────────────────────────── */

export default function FundDatabasePage() {
  const [search, setSearch] = useState("");
  const [strategy, setStrategy] = useState("全部");
  const [frequency, setFrequency] = useState("all");

  const filteredFunds = mockFunds.filter((fund) => {
    const matchSearch =
      !search ||
      fund.fund_name.includes(search) ||
      fund.filing_number.toLowerCase().includes(search.toLowerCase());
    const matchStrategy =
      strategy === "全部" || fund.strategy_type === strategy;
    const matchFrequency =
      frequency === "all" ||
      (frequency === "daily" && fund.nav_frequency === "daily") ||
      (frequency === "weekly" && fund.nav_frequency === "weekly");
    return matchSearch && matchStrategy && matchFrequency;
  });

  return (
    <div className="mx-auto max-w-7xl space-y-6">
      <PageHeader
        title="基金数据库"
        description={`共 ${filteredFunds.length} 只基金`}
      />

      {/* Filters */}
      <Card className="rounded-xl shadow-md">
        <CardContent className="space-y-4 px-5 py-4">
          {/* Search + Strategy */}
          <div className="flex flex-wrap items-center gap-3">
            <div className="relative w-72">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="搜索基金名称或备案编号..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="pl-9"
              />
            </div>

            <div className="flex flex-wrap gap-1.5">
              {strategyTypes.map((s) => (
                <Button
                  key={s}
                  variant={strategy === s ? "default" : "outline"}
                  size="sm"
                  className="h-8 text-xs"
                  onClick={() => setStrategy(s)}
                >
                  {s}
                </Button>
              ))}
            </div>
          </div>

          {/* Frequency Tabs */}
          <Tabs
            value={frequency}
            onValueChange={setFrequency}
          >
            <TabsList>
              <TabsTrigger value="all">全部</TabsTrigger>
              <TabsTrigger value="daily">日频</TabsTrigger>
              <TabsTrigger value="weekly">周频</TabsTrigger>
            </TabsList>
          </Tabs>
        </CardContent>
      </Card>

      {/* Table */}
      <Card className="rounded-xl shadow-md overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow className="bg-muted/50">
              <TableHead className="w-[200px]">基金名称</TableHead>
              <TableHead>备案编号</TableHead>
              <TableHead>基金经理</TableHead>
              <TableHead>策略类型</TableHead>
              <TableHead className="text-right">最新净值</TableHead>
              <TableHead className="text-center">频率</TableHead>
              <TableHead>成立日期</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filteredFunds.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={7}
                  className="h-32 text-center text-muted-foreground"
                >
                  暂无符合条件的基金数据
                </TableCell>
              </TableRow>
            ) : (
              filteredFunds.map((fund) => (
                <TableRow
                  key={fund.id}
                  className="cursor-pointer transition-colors hover:bg-muted/40"
                >
                  <TableCell className="font-medium">
                    {fund.fund_name}
                  </TableCell>
                  <TableCell className="font-tabular text-muted-foreground">
                    {fund.filing_number}
                  </TableCell>
                  <TableCell>{fund.manager_name}</TableCell>
                  <TableCell>
                    <Badge variant="secondary" className="text-[11px]">
                      {fund.strategy_type}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right font-tabular font-semibold">
                    {fund.latest_nav?.toFixed(4) ?? "—"}
                  </TableCell>
                  <TableCell className="text-center">
                    {fund.nav_frequency === "daily" ? (
                      <Badge className="bg-blue-100 text-blue-700 hover:bg-blue-100 border-blue-200 text-[11px]">
                        日频
                      </Badge>
                    ) : (
                      <Badge className="bg-amber-100 text-amber-700 hover:bg-amber-100 border-amber-200 text-[11px]">
                        周频
                      </Badge>
                    )}
                  </TableCell>
                  <TableCell className="font-tabular text-muted-foreground">
                    {fund.inception_date}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Card>
    </div>
  );
}
