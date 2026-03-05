"use client";

import { useRouter } from "next/navigation";
import { Layers, GitCompareArrows, PieChart } from "lucide-react";

const modules = [
  {
    title: "基金池",
    desc: "管理基础池、观察池、投资池",
    icon: Layers,
    href: "/fund-research/pools",
  },
  {
    title: "基金比较",
    desc: "多基金归一化NAV对比与指标分析",
    icon: GitCompareArrows,
    href: "/fund-research/comparison",
  },
  {
    title: "组合研究",
    desc: "组合构建、配置模型、回测分析",
    icon: PieChart,
    href: "/fund-research/portfolio",
  },
];

export default function FundResearchPage() {
  const router = useRouter();
  return (
    <div className="space-y-3">
      <h1 className="text-[15px] font-semibold">基金研究</h1>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        {modules.map(m => (
          <button
            key={m.href}
            onClick={() => router.push(m.href)}
            className="bg-card border border-border rounded px-5 py-6 text-left hover:border-primary/30 hover:shadow-sm transition-all group"
          >
            <m.icon className="h-6 w-6 text-primary/60 mb-3 group-hover:text-primary transition-colors" />
            <div className="text-[13px] font-medium mb-1">{m.title}</div>
            <div className="text-[11px] text-muted-foreground">{m.desc}</div>
          </button>
        ))}
      </div>
    </div>
  );
}
