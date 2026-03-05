"use client";

import { PieChart } from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";

export default function PortfolioPage() {
  return (
    <div className="space-y-3">
      <PageHeader title="组合研究" description="组合构建、配置模型与回测分析" />
      <div className="bg-card border border-border rounded h-64 flex items-center justify-center text-muted-foreground">
        <div className="text-center space-y-2">
          <PieChart className="mx-auto h-8 w-8 opacity-25" />
          <p className="text-[13px] opacity-60">组合研究模块开发中</p>
          <p className="text-[11px] opacity-40">将支持自定义权重、等权、风险平价等配置模型</p>
        </div>
      </div>
    </div>
  );
}
