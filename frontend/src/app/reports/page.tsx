"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import {
  FileText,
  Download,
  Loader2,
  BarChart3,
  GitCompareArrows,
  Play,
  CheckCircle2,
  XCircle,
  Clock,
  Shield,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { fetchApiAuth, fetchApiBlobAuth } from "@/lib/api";
import { AttributionChart } from "@/components/AttributionChart";
import { CorrelationHeatmap } from "@/components/CorrelationHeatmap";

/* --- Types --- */

interface ProductItem {
  id: number;
  product_name: string;
  product_code: string | null;
}

interface AsyncTask {
  task_id: string;
  product_id: number;
  product_name: string;
  format: string;
  status: string;
  created_at: string;
}

interface AttributionCategory {
  category: string;
  category_name: string;
  allocation_effect: number;
  selection_effect: number;
  interaction_effect: number;
  total_effect: number;
  benchmark_weight: number;
  actual_weight: number;
  benchmark_return: number;
  actual_return: number;
}

interface AttributionPeriod {
  period_start: string;
  period_end: string;
  benchmark_total_return: number;
  actual_total_return: number;
  excess_return: number;
  total_allocation: number;
  total_selection: number;
  total_interaction: number;
  categories: AttributionCategory[];
}

interface AttributionResponse {
  product_id: number;
  period_start: string;
  period_end: string;
  granularity: string;
  periods: AttributionPeriod[];
  cumulative_excess: number;
  cumulative_allocation: number;
  cumulative_selection: number;
  cumulative_interaction: number;
  aggregated_categories: AttributionCategory[];
}

interface FundContributionItem {
  fund_id: number | null;
  fund_name: string;
  weight: number;
  return: number | null;
  contribution: number | null;
}

interface FundContributionResponse {
  product_id: number;
  period_start: string;
  period_end: string;
  contributions: FundContributionItem[];
}

interface CorrelationMatrixResponse {
  labels: string[];
  matrix: number[][];
  period: { start: string; end: string };
}

/* --- Helpers --- */

function getDefaultPeriod(): { start: string; end: string } {
  const now = new Date();
  const end = now.toISOString().slice(0, 10);
  const start = new Date(now.getFullYear(), now.getMonth() - 3, now.getDate())
    .toISOString()
    .slice(0, 10);
  return { start, end };
}

function TaskStatusBadge({ status }: { status: string }) {
  switch (status) {
    case "SUCCESS":
      return (
        <Badge className="text-[10px] bg-green-500/10 text-green-600 border-green-200 hover:bg-green-500/10">
          <CheckCircle2 className="h-3 w-3 mr-0.5" />
          已完成
        </Badge>
      );
    case "FAILURE":
      return (
        <Badge className="text-[10px] bg-red-500/10 text-red-600 border-red-200 hover:bg-red-500/10">
          <XCircle className="h-3 w-3 mr-0.5" />
          失败
        </Badge>
      );
    case "PENDING":
    case "STARTED":
    case "RETRY":
      return (
        <Badge className="text-[10px] bg-blue-500/10 text-blue-600 border-blue-200 hover:bg-blue-500/10">
          <Clock className="h-3 w-3 mr-0.5 animate-pulse" />
          {status === "PENDING" ? "排队中" : status === "STARTED" ? "生成中" : "重试中"}
        </Badge>
      );
    default:
      return (
        <Badge className="text-[10px] bg-muted text-muted-foreground">
          {status}
        </Badge>
      );
  }
}

/* --- Page --- */

export default function ReportsPage() {
  const [activeTab, setActiveTab] = useState("generate");
  const [products, setProducts] = useState<ProductItem[]>([]);
  const [loadingProducts, setLoadingProducts] = useState(true);
  const [authError, setAuthError] = useState(false);

  // --- Tab 1: Report generation ---
  const [genProductId, setGenProductId] = useState("");
  const [genReportType, setGenReportType] = useState("monthly");
  const [genFormat, setGenFormat] = useState<"pdf" | "excel">("pdf");
  const [genPeriodStart, setGenPeriodStart] = useState(getDefaultPeriod().start);
  const [genPeriodEnd, setGenPeriodEnd] = useState(getDefaultPeriod().end);
  const [generating, setGenerating] = useState(false);
  const [genError, setGenError] = useState("");

  // Async tasks
  const [tasks, setTasks] = useState<AsyncTask[]>([]);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const tasksRef = useRef<AsyncTask[]>([]);
  const [pollError, setPollError] = useState("");
  const pollErrorCountRef = useRef(0);
  const pollingInProgressRef = useRef(false);

  // --- Tab 2: Attribution ---
  const [attrProductId, setAttrProductId] = useState("");
  const [attrPeriodStart, setAttrPeriodStart] = useState(getDefaultPeriod().start);
  const [attrPeriodEnd, setAttrPeriodEnd] = useState(getDefaultPeriod().end);
  const [attrLoading, setAttrLoading] = useState(false);
  const [attrData, setAttrData] = useState<AttributionResponse | null>(null);
  const [contribData, setContribData] = useState<FundContributionResponse | null>(null);
  const [attrError, setAttrError] = useState("");

  // --- Tab 3: Correlation ---
  const [corrFundIds, setCorrFundIds] = useState("");
  const [corrPeriodStart, setCorrPeriodStart] = useState(getDefaultPeriod().start);
  const [corrPeriodEnd, setCorrPeriodEnd] = useState(getDefaultPeriod().end);
  const [corrLoading, setCorrLoading] = useState(false);
  const [corrData, setCorrData] = useState<CorrelationMatrixResponse | null>(null);
  const [corrError, setCorrError] = useState("");

  // Load products
  const loadProducts = useCallback(async () => {
    setLoadingProducts(true);
    try {
      const res = await fetchApiAuth<{ items: ProductItem[]; total: number }>("/products/");
      setProducts(Array.isArray(res.items) ? res.items : []);
      setAuthError(false);
    } catch (e: unknown) {
      if (e instanceof Error && e.message === "AUTH_REQUIRED") {
        setAuthError(true);
      }
      setProducts([]);
    } finally {
      setLoadingProducts(false);
    }
  }, []);

  useEffect(() => {
    loadProducts();
  }, [loadProducts]);

  // Keep tasksRef in sync
  useEffect(() => {
    tasksRef.current = tasks;
  }, [tasks]);

  // Poll pending tasks — only poll pending, use ref to avoid closure staleness
  useEffect(() => {
    pollingRef.current = setInterval(async () => {
      if (pollingInProgressRef.current) return; // prevent concurrent polls
      const currentTasks = tasksRef.current;
      const pending = currentTasks.filter(
        (t) => t.status !== "SUCCESS" && t.status !== "FAILURE"
      );
      if (pending.length === 0) return;

      pollingInProgressRef.current = true;
      try {
        const results = await Promise.all(
          pending.map(async (task) => {
            try {
              const res = await fetchApiAuth<{ task_id: string; status: string }>(
                `/reports/tasks/${encodeURIComponent(task.task_id)}`
              );
              return { taskId: task.task_id, status: res.status, ok: true };
            } catch {
              return { taskId: task.task_id, status: null, ok: false };
            }
          })
        );

        const successCount = results.filter((r) => r.ok).length;
        if (successCount === 0) {
          pollErrorCountRef.current += 1;
          if (pollErrorCountRef.current >= 3) {
            setPollError("任务状态同步失败，请刷新页面重试");
            if (pollingRef.current) {
              clearInterval(pollingRef.current);
              pollingRef.current = null;
            }
          }
          return;
        }

        pollErrorCountRef.current = 0;
        if (pollError) setPollError("");

        const updates = new Map(
          results
            .filter((r): r is { taskId: string; status: string; ok: true } => r.ok && !!r.status)
            .map((r) => [r.taskId, r.status])
        );

        if (updates.size > 0) {
          setTasks((prev) =>
            prev.map((t) => {
              const newStatus = updates.get(t.task_id);
              return newStatus && newStatus !== t.status
                ? { ...t, status: newStatus }
                : t;
            })
          );
        }
      } finally {
        pollingInProgressRef.current = false;
      }
    }, 3000);

    return () => {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
      }
    };
  }, []);

  // Generate report (sync PDF/Excel or async)
  const handleGenerate = async () => {
    if (!genProductId) return;
    const productId = parseInt(genProductId);
    if (isNaN(productId) || productId <= 0) {
      setGenError("请选择有效的产品");
      return;
    }
    if (genPeriodStart && genPeriodEnd && genPeriodStart > genPeriodEnd) {
      setGenError("结束日期不能早于开始日期");
      return;
    }
    setGenerating(true);
    setGenError("");
    const product = products.find((p) => p.id === productId);

    try {
      const reqBody = JSON.stringify({
        product_id: productId,
        report_type: genReportType,
        period_start: genPeriodStart,
        period_end: genPeriodEnd,
      });

      if (genFormat === "pdf") {
        const blob = await fetchApiBlobAuth("/reports/generate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: reqBody,
          timeoutMs: 120000,
        });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `report_${productId}_${genPeriodEnd}.pdf`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
      } else {
        const blob = await fetchApiBlobAuth(`/reports/${productId}/excel`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: reqBody,
          timeoutMs: 120000,
        });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `report_${productId}_${genPeriodEnd}.xlsx`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "报告生成失败";
      setGenError(msg);

      // Fallback: try async generation
      if (msg.includes("timeout") || msg.includes("504") || msg.includes("502")) {
        try {
          const asyncRes = await fetchApiAuth<{
            task_id: string;
            product_id: number;
            format: string;
            status: string;
          }>(`/reports/${productId}/generate-async`, {
            method: "POST",
            body: JSON.stringify({
              report_type: genReportType,
              period_start: genPeriodStart,
              period_end: genPeriodEnd,
              format: genFormat,
            }),
          });
          setTasks((prev) => [
            {
              task_id: asyncRes.task_id,
              product_id: asyncRes.product_id,
              product_name: product?.product_name || `产品#${productId}`,
              format: asyncRes.format,
              status: asyncRes.status,
              created_at: new Date().toISOString(),
            },
            ...prev,
          ]);
          setGenError("同步生成超时，已转为异步任务，请在下方查看进度");
        } catch {
          setGenError("报告生成失败，请稍后重试");
        }
      }
    } finally {
      setGenerating(false);
    }
  };

  // Generate async directly
  const handleGenerateAsync = async () => {
    if (!genProductId) return;
    const productId = parseInt(genProductId);
    if (isNaN(productId) || productId <= 0) {
      setGenError("请选择有效的产品");
      return;
    }
    if (genPeriodStart && genPeriodEnd && genPeriodStart > genPeriodEnd) {
      setGenError("结束日期不能早于开始日期");
      return;
    }
    setGenerating(true);
    setGenError("");
    const product = products.find((p) => p.id === productId);

    try {
      const res = await fetchApiAuth<{
        task_id: string;
        product_id: number;
        format: string;
        status: string;
      }>(`/reports/${productId}/generate-async`, {
        method: "POST",
        body: JSON.stringify({
          report_type: genReportType,
          period_start: genPeriodStart,
          period_end: genPeriodEnd,
          format: genFormat,
        }),
      });
      setTasks((prev) => [
        {
          task_id: res.task_id,
          product_id: res.product_id,
          product_name: product?.product_name || `产品#${productId}`,
          format: res.format,
          status: res.status,
          created_at: new Date().toISOString(),
        },
        ...prev,
      ]);
    } catch (e: unknown) {
      setGenError(e instanceof Error ? e.message : "异步任务创建失败");
    } finally {
      setGenerating(false);
    }
  };

  // Download completed task
  const handleDownloadTask = async (task: AsyncTask) => {
    try {
      const blob = await fetchApiBlobAuth(
        `/reports/tasks/${encodeURIComponent(task.task_id)}/download`
      );
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `report_${task.product_id}.${task.format === "excel" ? "xlsx" : "pdf"}`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (e: unknown) {
      setGenError(e instanceof Error ? e.message : "文件下载失败");
    }
  };

  // Load attribution
  const loadAttribution = async () => {
    if (!attrProductId) return;
    const productId = parseInt(attrProductId);
    if (isNaN(productId) || productId <= 0) {
      setAttrError("请选择有效的产品");
      return;
    }
    if (attrPeriodStart && attrPeriodEnd && attrPeriodStart > attrPeriodEnd) {
      setAttrError("结束日期不能早于开始日期");
      return;
    }
    setAttrLoading(true);
    setAttrError("");
    setAttrData(null);
    setContribData(null);

    try {
      const qs = new URLSearchParams({
        period_start: attrPeriodStart,
        period_end: attrPeriodEnd,
      }).toString();
      const [attr, contrib] = await Promise.all([
        fetchApiAuth<AttributionResponse>(
          `/reports/${productId}/attribution?${qs}`
        ),
        fetchApiAuth<FundContributionResponse>(
          `/reports/${productId}/fund-contribution?${qs}`
        ),
      ]);
      setAttrData(attr);
      setContribData(contrib);
    } catch (e: unknown) {
      setAttrError(e instanceof Error ? e.message : "归因分析失败");
    } finally {
      setAttrLoading(false);
    }
  };

  // Load correlation
  const loadCorrelation = async () => {
    if (!corrFundIds.trim()) return;
    const parsedIds = Array.from(
      new Set(
        corrFundIds
          .split(/[,，\s]+/)
          .map((s) => parseInt(s.trim(), 10))
          .filter((n) => Number.isInteger(n) && n > 0)
      )
    );
    if (parsedIds.length < 2) {
      setCorrError("请至少输入2个有效基金ID");
      return;
    }
    if (parsedIds.length > 50) {
      setCorrError(`最多支持50个基金ID，当前${parsedIds.length}个`);
      return;
    }
    if (corrPeriodStart && corrPeriodEnd && corrPeriodStart > corrPeriodEnd) {
      setCorrError("结束日期不能早于开始日期");
      return;
    }
    setCorrLoading(true);
    setCorrError("");
    setCorrData(null);

    try {
      const qs = new URLSearchParams({
        fund_ids: parsedIds.join(","),
        period_start: corrPeriodStart,
        period_end: corrPeriodEnd,
      }).toString();
      const res = await fetchApiAuth<CorrelationMatrixResponse>(
        `/reports/correlation-matrix?${qs}`
      );
      setCorrData(res);
    } catch (e: unknown) {
      setCorrError(e instanceof Error ? e.message : "相关性分析失败");
    } finally {
      setCorrLoading(false);
    }
  };

  // Auth error
  if (authError) {
    return (
      <div className="space-y-3">
        <PageHeader title="报告中心" description="报告生成与归因分析" />
        <div className="bg-card border border-border rounded p-8 text-center">
          <Shield className="h-8 w-8 mx-auto mb-3 text-muted-foreground opacity-40" />
          <p className="text-[13px] text-muted-foreground">请登录后查看报告中心</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <PageHeader title="报告中心" description="报告生成、归因分析与相关性分析" />

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="generate" className="text-[12px]">
            <FileText className="h-3.5 w-3.5 mr-1" />
            报告生成
          </TabsTrigger>
          <TabsTrigger value="attribution" className="text-[12px]">
            <BarChart3 className="h-3.5 w-3.5 mr-1" />
            归因分析
          </TabsTrigger>
          <TabsTrigger value="correlation" className="text-[12px]">
            <GitCompareArrows className="h-3.5 w-3.5 mr-1" />
            相关性分析
          </TabsTrigger>
        </TabsList>

        {/* ===== Tab 1: Report Generation ===== */}
        <TabsContent value="generate">
          <div className="bg-card border border-border rounded">
            {/* Form */}
            <div className="px-4 py-3 border-b border-border space-y-2">
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2">
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    产品
                  </label>
                  <select
                    className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                    value={genProductId}
                    onChange={(e) => setGenProductId(e.target.value)}
                    disabled={loadingProducts}
                  >
                    <option value="">
                      {loadingProducts ? "加载中..." : "-- 选择产品 --"}
                    </option>
                    {products.map((p) => (
                      <option key={p.id} value={p.id}>
                        {p.product_name}
                        {p.product_code ? ` (${p.product_code})` : ""}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    报告类型
                  </label>
                  <select
                    className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                    value={genReportType}
                    onChange={(e) => setGenReportType(e.target.value)}
                  >
                    <option value="monthly">月报</option>
                    <option value="weekly">周报</option>
                  </select>
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    输出格式
                  </label>
                  <select
                    className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                    value={genFormat}
                    onChange={(e) =>
                      setGenFormat(e.target.value as "pdf" | "excel")
                    }
                  >
                    <option value="pdf">PDF</option>
                    <option value="excel">Excel</option>
                  </select>
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    开始日期
                  </label>
                  <input
                    type="date"
                    className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                    value={genPeriodStart}
                    onChange={(e) => setGenPeriodStart(e.target.value)}
                  />
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    结束日期
                  </label>
                  <input
                    type="date"
                    className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                    value={genPeriodEnd}
                    onChange={(e) => setGenPeriodEnd(e.target.value)}
                  />
                </div>
                <div className="flex items-end gap-1">
                  <Button
                    size="sm"
                    className="h-7 text-[11px]"
                    onClick={handleGenerate}
                    disabled={!genProductId || generating}
                  >
                    {generating ? (
                      <Loader2 className="h-3 w-3 animate-spin mr-1" />
                    ) : (
                      <Play className="h-3 w-3 mr-1" />
                    )}
                    生成
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-7 text-[11px]"
                    onClick={handleGenerateAsync}
                    disabled={!genProductId || generating}
                    title="后台异步生成"
                  >
                    <Clock className="h-3 w-3 mr-1" />
                    异步
                  </Button>
                </div>
              </div>

              {genError && (
                <p className="text-[11px] text-red-500">{genError}</p>
              )}
              {pollError && (
                <p className="text-[11px] text-amber-600">{pollError}</p>
              )}
            </div>

            {/* Async tasks table */}
            {tasks.length > 0 && (
              <>
                <div className="px-4 py-2 border-b border-border">
                  <span className="text-[12px] font-medium">异步任务</span>
                </div>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="h-7 text-[11px] font-normal">
                        任务ID
                      </TableHead>
                      <TableHead className="h-7 text-[11px] font-normal">
                        产品
                      </TableHead>
                      <TableHead className="h-7 text-[11px] font-normal text-center">
                        格式
                      </TableHead>
                      <TableHead className="h-7 text-[11px] font-normal text-center">
                        状态
                      </TableHead>
                      <TableHead className="h-7 text-[11px] font-normal text-center w-20">
                        操作
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {tasks.map((task) => (
                      <TableRow key={task.task_id} className="text-[12px]">
                        <TableCell className="py-1.5 font-mono text-muted-foreground">
                          {task.task_id.slice(0, 8)}...
                        </TableCell>
                        <TableCell className="py-1.5">
                          {task.product_name}
                        </TableCell>
                        <TableCell className="py-1.5 text-center">
                          <Badge
                            variant="outline"
                            className="text-[10px] uppercase"
                          >
                            {task.format}
                          </Badge>
                        </TableCell>
                        <TableCell className="py-1.5 text-center">
                          <TaskStatusBadge status={task.status} />
                        </TableCell>
                        <TableCell className="py-1.5 text-center">
                          {task.status === "SUCCESS" ? (
                            <Button
                              size="sm"
                              variant="ghost"
                              className="h-6 px-2 text-[11px]"
                              onClick={() => handleDownloadTask(task)}
                            >
                              <Download className="h-3 w-3 mr-0.5" />
                              下载
                            </Button>
                          ) : task.status === "FAILURE" ? (
                            <span className="text-[11px] text-muted-foreground">
                              --
                            </span>
                          ) : (
                            <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground mx-auto" />
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </>
            )}

            {/* Empty state */}
            {tasks.length === 0 && (
              <div className="h-36 flex items-center justify-center text-muted-foreground">
                <div className="text-center space-y-1">
                  <FileText className="mx-auto h-7 w-7 opacity-25" />
                  <p className="text-[12px] opacity-60">
                    选择产品和参数后点击生成报告
                  </p>
                </div>
              </div>
            )}
          </div>
        </TabsContent>

        {/* ===== Tab 2: Attribution ===== */}
        <TabsContent value="attribution">
          <div className="bg-card border border-border rounded">
            {/* Form */}
            <div className="px-4 py-3 border-b border-border">
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    产品
                  </label>
                  <select
                    className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                    value={attrProductId}
                    onChange={(e) => setAttrProductId(e.target.value)}
                    disabled={loadingProducts}
                  >
                    <option value="">
                      {loadingProducts ? "加载中..." : "-- 选择产品 --"}
                    </option>
                    {products.map((p) => (
                      <option key={p.id} value={p.id}>
                        {p.product_name}
                        {p.product_code ? ` (${p.product_code})` : ""}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    开始日期
                  </label>
                  <input
                    type="date"
                    className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                    value={attrPeriodStart}
                    onChange={(e) => setAttrPeriodStart(e.target.value)}
                  />
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    结束日期
                  </label>
                  <input
                    type="date"
                    className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                    value={attrPeriodEnd}
                    onChange={(e) => setAttrPeriodEnd(e.target.value)}
                  />
                </div>
                <div className="flex items-end">
                  <Button
                    size="sm"
                    className="h-7 text-[11px]"
                    onClick={loadAttribution}
                    disabled={!attrProductId || attrLoading}
                  >
                    {attrLoading ? (
                      <Loader2 className="h-3 w-3 animate-spin mr-1" />
                    ) : (
                      <BarChart3 className="h-3 w-3 mr-1" />
                    )}
                    分析
                  </Button>
                </div>
              </div>
              {attrError && (
                <p className="text-[11px] text-red-500 mt-2">{attrError}</p>
              )}
            </div>

            {/* Attribution results */}
            {attrLoading ? (
              <div className="h-48 flex items-center justify-center">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : attrData ? (
              <div className="space-y-0">
                {/* Summary */}
                <div className="px-4 py-3 border-b border-border">
                  <div className="grid grid-cols-4 gap-3">
                    <div>
                      <span className="text-[10px] text-muted-foreground block">
                        累计超额收益
                      </span>
                      <span
                        className={`text-[14px] font-semibold tabular-nums ${
                          attrData.cumulative_excess >= 0
                            ? "text-green-600"
                            : "text-red-500"
                        }`}
                      >
                        {(attrData.cumulative_excess * 100).toFixed(2)}%
                      </span>
                    </div>
                    <div>
                      <span className="text-[10px] text-muted-foreground block">
                        配置效应
                      </span>
                      <span className="text-[14px] font-semibold tabular-nums">
                        {(attrData.cumulative_allocation * 100).toFixed(2)}%
                      </span>
                    </div>
                    <div>
                      <span className="text-[10px] text-muted-foreground block">
                        选择效应
                      </span>
                      <span className="text-[14px] font-semibold tabular-nums">
                        {(attrData.cumulative_selection * 100).toFixed(2)}%
                      </span>
                    </div>
                    <div>
                      <span className="text-[10px] text-muted-foreground block">
                        交互效应
                      </span>
                      <span className="text-[14px] font-semibold tabular-nums">
                        {(attrData.cumulative_interaction * 100).toFixed(2)}%
                      </span>
                    </div>
                  </div>
                </div>

                {/* Brinson Chart */}
                {attrData.aggregated_categories.length > 0 && (
                  <div className="px-4 py-3 border-b border-border">
                    <h3 className="text-[12px] font-medium mb-2">
                      Brinson 归因分解
                    </h3>
                    <AttributionChart
                      data={attrData.aggregated_categories.map((c) => ({
                        category: c.category_name || c.category,
                        allocation: c.allocation_effect,
                        selection: c.selection_effect,
                        interaction: c.interaction_effect,
                      }))}
                    />
                  </div>
                )}

                {/* Fund contribution table */}
                {contribData && contribData.contributions.length > 0 && (
                  <div>
                    <div className="px-4 py-2 border-b border-border">
                      <span className="text-[12px] font-medium">
                        基金贡献度
                      </span>
                    </div>
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="h-7 text-[11px] font-normal">
                            基金名称
                          </TableHead>
                          <TableHead className="h-7 text-[11px] font-normal text-right">
                            权重
                          </TableHead>
                          <TableHead className="h-7 text-[11px] font-normal text-right">
                            收益率
                          </TableHead>
                          <TableHead className="h-7 text-[11px] font-normal text-right">
                            贡献度
                          </TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {contribData.contributions.map((c, i) => (
                          <TableRow key={i} className="text-[12px]">
                            <TableCell className="py-1.5 font-medium">
                              {c.fund_name}
                              {c.fund_id && (
                                <span className="text-[10px] text-muted-foreground ml-1">
                                  #{c.fund_id}
                                </span>
                              )}
                            </TableCell>
                            <TableCell className="py-1.5 text-right tabular-nums">
                              {(c.weight * 100).toFixed(2)}%
                            </TableCell>
                            <TableCell className="py-1.5 text-right tabular-nums">
                              {c.return != null ? (
                                <span
                                  className={
                                    c.return >= 0
                                      ? "text-green-600"
                                      : "text-red-500"
                                  }
                                >
                                  {(c.return * 100).toFixed(2)}%
                                </span>
                              ) : (
                                <span className="text-muted-foreground">--</span>
                              )}
                            </TableCell>
                            <TableCell className="py-1.5 text-right tabular-nums">
                              {c.contribution != null ? (
                                <span
                                  className={
                                    c.contribution >= 0
                                      ? "text-green-600"
                                      : "text-red-500"
                                  }
                                >
                                  {(c.contribution * 100).toFixed(2)}%
                                </span>
                              ) : (
                                <span className="text-muted-foreground">--</span>
                              )}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                )}
              </div>
            ) : (
              <div className="h-48 flex items-center justify-center text-muted-foreground">
                <div className="text-center space-y-1">
                  <BarChart3 className="mx-auto h-7 w-7 opacity-25" />
                  <p className="text-[12px] opacity-60">
                    选择产品和期间后点击分析
                  </p>
                </div>
              </div>
            )}
          </div>
        </TabsContent>

        {/* ===== Tab 3: Correlation ===== */}
        <TabsContent value="correlation">
          <div className="bg-card border border-border rounded">
            {/* Form */}
            <div className="px-4 py-3 border-b border-border">
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    基金ID列表
                  </label>
                  <Input
                    className="h-7 text-[12px]"
                    placeholder="如: 1,2,3,4"
                    value={corrFundIds}
                    onChange={(e) => setCorrFundIds(e.target.value)}
                  />
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    开始日期
                  </label>
                  <input
                    type="date"
                    className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                    value={corrPeriodStart}
                    onChange={(e) => setCorrPeriodStart(e.target.value)}
                  />
                </div>
                <div>
                  <label className="text-[10px] text-muted-foreground mb-0.5 block">
                    结束日期
                  </label>
                  <input
                    type="date"
                    className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                    value={corrPeriodEnd}
                    onChange={(e) => setCorrPeriodEnd(e.target.value)}
                  />
                </div>
                <div className="flex items-end">
                  <Button
                    size="sm"
                    className="h-7 text-[11px]"
                    onClick={loadCorrelation}
                    disabled={!corrFundIds.trim() || corrLoading}
                  >
                    {corrLoading ? (
                      <Loader2 className="h-3 w-3 animate-spin mr-1" />
                    ) : (
                      <GitCompareArrows className="h-3 w-3 mr-1" />
                    )}
                    计算
                  </Button>
                </div>
              </div>
              {corrError && (
                <p className="text-[11px] text-red-500 mt-2">{corrError}</p>
              )}
            </div>

            {/* Correlation results */}
            {corrLoading ? (
              <div className="h-48 flex items-center justify-center">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : corrData ? (
              <div className="px-4 py-3">
                <h3 className="text-[12px] font-medium mb-2">
                  相关系数矩阵
                  <span className="text-[10px] text-muted-foreground ml-2">
                    {corrData.period.start} ~ {corrData.period.end}
                  </span>
                </h3>
                <CorrelationHeatmap
                  labels={corrData.labels}
                  matrix={corrData.matrix}
                />
              </div>
            ) : (
              <div className="h-48 flex items-center justify-center text-muted-foreground">
                <div className="text-center space-y-1">
                  <GitCompareArrows className="mx-auto h-7 w-7 opacity-25" />
                  <p className="text-[12px] opacity-60">
                    输入基金ID列表和期间后点击计算
                  </p>
                </div>
              </div>
            )}
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
