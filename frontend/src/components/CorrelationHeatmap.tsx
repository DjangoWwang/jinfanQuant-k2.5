"use client";

import { useMemo } from "react";
import dynamic from "next/dynamic";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

interface CorrelationHeatmapProps {
  labels: string[];
  matrix: number[][];
}

const escHtml = (s: string) =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

export function CorrelationHeatmap({ labels, matrix }: CorrelationHeatmapProps) {
  const chartOption = useMemo(() => {
    // Build heatmap data: [x, y, value]
    const heatmapData: [number, number, number][] = [];
    for (let i = 0; i < labels.length; i++) {
      for (let j = 0; j < labels.length; j++) {
        const val = matrix[i]?.[j] ?? 0;
        heatmapData.push([j, i, parseFloat(val.toFixed(4))]);
      }
    }

    const min = Math.min(...heatmapData.map((d) => d[2]));
    const max = Math.max(...heatmapData.map((d) => d[2]));

    return {
      tooltip: {
        formatter: (params: { value: [number, number, number] }) => {
          const [x, y, v] = params.value;
          return `${escHtml(labels[y] || "")} vs ${escHtml(labels[x] || "")}<br/>相关系数: <b>${v.toFixed(4)}</b>`;
        },
      },
      grid: {
        left: 100,
        right: 40,
        top: 10,
        bottom: 80,
      },
      xAxis: {
        type: "category",
        data: labels,
        axisLabel: { fontSize: 10, color: "#666", rotate: 30 },
        splitArea: { show: true },
      },
      yAxis: {
        type: "category",
        data: labels,
        axisLabel: { fontSize: 10, color: "#666" },
        splitArea: { show: true },
      },
      visualMap: {
        min: min < -1 ? min : -1,
        max: max > 1 ? max : 1,
        calculable: true,
        orient: "horizontal",
        left: "center",
        bottom: 0,
        itemWidth: 14,
        itemHeight: 120,
        textStyle: { fontSize: 10 },
        inRange: {
          color: ["#3b82f6", "#93c5fd", "#ffffff", "#fca5a5", "#ef4444"],
        },
      },
      series: [
        {
          type: "heatmap",
          data: heatmapData,
          label: {
            show: labels.length <= 8,
            fontSize: 10,
            formatter: (params: { value: [number, number, number] }) => params.value[2].toFixed(2),
          },
          emphasis: {
            itemStyle: {
              shadowBlur: 6,
              shadowColor: "rgba(0, 0, 0, 0.2)",
            },
          },
        },
      ],
    };
  }, [labels, matrix]);

  if (labels.length === 0) {
    return (
      <div className="h-48 flex items-center justify-center text-muted-foreground text-[12px]">
        暂无相关性数据
      </div>
    );
  }

  const size = Math.max(300, labels.length * 50 + 120);

  return <ReactECharts option={chartOption} style={{ height: size }} notMerge />;
}
