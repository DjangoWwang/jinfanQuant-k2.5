"use client";

import { useMemo } from "react";
import dynamic from "next/dynamic";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

interface AttributionData {
  category: string;
  allocation: number;
  selection: number;
  interaction: number;
}

interface AttributionChartProps {
  data: AttributionData[];
}

const escHtml = (s: string) =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

export function AttributionChart({ data }: AttributionChartProps) {
  const chartOption = useMemo(() => {
    const categories = data.map((d) => d.category);

    return {
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        formatter: (params: Array<{ seriesName: string; value: number; color: string }>) => {
          const cat = params[0] && "axisValue" in params[0] ? (params[0] as unknown as { axisValue: string }).axisValue : "";
          const lines = params.map(
            (p) =>
              `<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${escHtml(p.color)};margin-right:4px;"></span>${escHtml(p.seriesName)}: ${(p.value * 100).toFixed(2)}%`
          );
          return `${escHtml(cat)}<br/>${lines.join("<br/>")}`;
        },
      },
      legend: {
        bottom: 0,
        textStyle: { fontSize: 11 },
      },
      grid: { left: 100, right: 30, top: 10, bottom: 40 },
      xAxis: {
        type: "value",
        axisLabel: {
          fontSize: 10,
          color: "#999",
          formatter: (v: number) => `${(v * 100).toFixed(1)}%`,
        },
        splitLine: { lineStyle: { color: "#f3f4f6" } },
      },
      yAxis: {
        type: "category",
        data: categories,
        axisLabel: { fontSize: 11, color: "#666" },
        axisLine: { lineStyle: { color: "#e5e7eb" } },
      },
      series: [
        {
          name: "配置效应",
          type: "bar",
          stack: "attribution",
          data: data.map((d) => d.allocation),
          itemStyle: { color: "#4f46e5" },
          barMaxWidth: 24,
        },
        {
          name: "选择效应",
          type: "bar",
          stack: "attribution",
          data: data.map((d) => d.selection),
          itemStyle: { color: "#10b981" },
        },
        {
          name: "交互效应",
          type: "bar",
          stack: "attribution",
          data: data.map((d) => d.interaction),
          itemStyle: { color: "#f59e0b" },
        },
      ],
    };
  }, [data]);

  if (data.length === 0) {
    return (
      <div className="h-48 flex items-center justify-center text-muted-foreground text-[12px]">
        暂无归因数据
      </div>
    );
  }

  return <ReactECharts option={chartOption} style={{ height: Math.max(200, data.length * 40 + 80) }} notMerge />;
}
