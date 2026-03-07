"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import {
  Database,
  RefreshCw,
  RotateCcw,
  Calendar,
  Loader2,
  CheckCircle2,
  XCircle,
  Clock,
  Activity,
  Shield,
  AlertTriangle,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { fetchApiAuth } from "@/lib/api";

/* --- Types --- */

interface TaskResponse {
  task_id: string;
  status: string;
  message: string;
}

interface TaskStatusResponse {
  task_id: string;
  status: string;
  result: unknown;
  progress: {
    processed?: number;
    failed?: number;
    total?: number;
    phase?: string;
  } | null;
}

interface TrackedTask {
  task_id: string;
  type: "incremental" | "full" | "daily";
  status: string;
  message: string;
  created_at: string;
  progress: TaskStatusResponse["progress"];
  result: unknown;
}

/* --- Helpers --- */

const TASK_TYPE_LABELS: Record<string, string> = {
  incremental: "增量更新",
  full: "全量重建",
  daily: "每日刷新",
};

const TERMINAL_STATES = new Set(["SUCCESS", "FAILURE", "REVOKED"]);

function statusBadge(status: string) {
  switch (status) {
    case "SUCCESS":
      return (
        <Badge className="bg-emerald-500/10 text-emerald-600 border-emerald-200 hover:bg-emerald-500/10 text-[10px]">
          <CheckCircle2 className="h-3 w-3 mr-0.5" />
          成功
        </Badge>
      );
    case "FAILURE":
    case "REVOKED":
      return (
        <Badge className="bg-red-500/10 text-red-600 border-red-200 hover:bg-red-500/10 text-[10px]">
          <XCircle className="h-3 w-3 mr-0.5" />
          失败
        </Badge>
      );
    case "PROGRESS":
      return (
        <Badge className="bg-blue-500/10 text-blue-600 border-blue-200 hover:bg-blue-500/10 text-[10px]">
          <Activity className="h-3 w-3 mr-0.5" />
          执行中
        </Badge>
      );
    case "STARTED":
      return (
        <Badge className="bg-blue-500/10 text-blue-600 border-blue-200 hover:bg-blue-500/10 text-[10px]">
          <Loader2 className="h-3 w-3 mr-0.5 animate-spin" />
          已启动
        </Badge>
      );
    default:
      return (
        <Badge className="bg-amber-500/10 text-amber-600 border-amber-200 hover:bg-amber-500/10 text-[10px]">
          <Clock className="h-3 w-3 mr-0.5" />
          排队中
        </Badge>
      );
  }
}

function formatTime(iso: string): string {
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return d.toLocaleTimeString("zh-CN", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return iso;
  }
}

/* --- Page --- */

export default function DataManagementPage() {
  const [authError, setAuthError] = useState(false);
  const [fundTotal, setFundTotal] = useState<number | null>(null);
  const [tasks, setTasks] = useState<TrackedTask[]>([]);
  const [submitting, setSubmitting] = useState<string | null>(null);

  // Dialog state
  const [dialogType, setDialogType] = useState<
    "incremental" | "full" | "daily" | null
  >(null);
  const [fundIdsInput, setFundIdsInput] = useState("");
  const [updateAll, setUpdateAll] = useState(true);

  // Polling ref
  const pollTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const tasksRef = useRef<TrackedTask[]>([]);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [pollError, setPollError] = useState<string | null>(null);
  const pollErrorCountRef = useRef(0);
  const pollingInProgressRef = useRef(false);

  // Load fund count
  useEffect(() => {
    (async () => {
      try {
        const data = await fetchApiAuth<{ total: number }>(
          "/funds/?limit=1"
        );
        setFundTotal(data.total ?? null);
        setAuthError(false);
      } catch (e: unknown) {
        if (e instanceof Error && e.message === "AUTH_REQUIRED") {
          setAuthError(true);
        }
      }
    })();
  }, []);

  // Keep tasksRef in sync
  useEffect(() => {
    tasksRef.current = tasks;
  }, [tasks]);

  // Poll task statuses — use ref to avoid closure/race issues
  useEffect(() => {
    pollTimerRef.current = setInterval(async () => {
      if (pollingInProgressRef.current) return; // prevent concurrent polls
      const currentTasks = tasksRef.current;
      const pending = currentTasks.filter((t) => !TERMINAL_STATES.has(t.status));
      if (pending.length === 0) return;

      pollingInProgressRef.current = true;
      try {
        const results = await Promise.all(
          pending.map(async (task) => {
            try {
              const qs = new URLSearchParams({ task_id: task.task_id }).toString();
              const res = await fetchApiAuth<TaskStatusResponse>(
                `/etl/status?${qs}`
              );
              return { taskId: task.task_id, data: res };
            } catch {
              return { taskId: task.task_id, data: null };
            }
          })
        );

        // Reset error count on successful poll
        if (results.some((r) => r.data !== null)) {
          pollErrorCountRef.current = 0;
          setPollError(null);
        }

        setTasks((prev) => {
          const updates = new Map(
            results
              .filter((r): r is typeof r & { data: TaskStatusResponse } => r.data !== null)
              .map((r) => [r.taskId, r.data])
          );
          if (updates.size === 0) return prev;

          const hasChanges = prev.some((t) => {
            const u = updates.get(t.task_id);
            return u && (u.status !== t.status);
          });
          if (!hasChanges) return prev;

          return prev.map((t) => {
            const u = updates.get(t.task_id);
            if (!u) return t;
            return { ...t, status: u.status, progress: u.progress, result: u.result };
          });
        });
      } catch {
        pollErrorCountRef.current++;
        if (pollErrorCountRef.current >= 3) {
          setPollError("任务状态同步失败，请刷新页面重试");
          if (pollTimerRef.current) {
            clearInterval(pollTimerRef.current);
            pollTimerRef.current = null;
          }
        }
      } finally {
        pollingInProgressRef.current = false;
      }
    }, 5000);

    return () => {
      if (pollTimerRef.current) clearInterval(pollTimerRef.current);
    };
  }, []);

  // Parse fund IDs from input (deduped)
  const parseFundIds = (input: string): number[] => {
    return Array.from(
      new Set(
        input
          .split(/[,，\s]+/)
          .map((s) => s.trim())
          .filter((s) => s.length > 0)
          .map((s) => parseInt(s, 10))
          .filter((n) => Number.isInteger(n) && n > 0)
      )
    );
  };

  // Submit task
  const submitTask = async () => {
    if (!dialogType) return;
    setSubmitError(null);

    if (dialogType === "incremental") {
      setSubmitting("incremental");
      try {
        const body: { fund_ids?: number[] | null } = {};
        if (updateAll) {
          body.fund_ids = null;
        } else {
          const ids = parseFundIds(fundIdsInput);
          if (ids.length === 0) return;
          body.fund_ids = ids;
        }
        const res = await fetchApiAuth<TaskResponse>("/etl/refresh-nav", {
          method: "POST",
          body: JSON.stringify(body),
        });
        setTasks((prev) => [
          {
            task_id: res.task_id,
            type: "incremental",
            status: res.status === "queued" ? "PENDING" : res.status,
            message: res.message,
            created_at: new Date().toISOString(),
            progress: null,
            result: null,
          },
          ...prev,
        ]);
        setDialogType(null);
        setFundIdsInput("");
        setUpdateAll(true);
      } catch (e: unknown) {
        setSubmitError(e instanceof Error ? e.message : "操作失败，请稍后重试");
      } finally {
        setSubmitting(null);
      }
    } else if (dialogType === "full") {
      const ids = parseFundIds(fundIdsInput);
      if (ids.length === 0) return;
      setSubmitting("full");
      try {
        const res = await fetchApiAuth<TaskResponse>("/etl/refresh-nav-full", {
          method: "POST",
          body: JSON.stringify({ fund_ids: ids }),
        });
        setTasks((prev) => [
          {
            task_id: res.task_id,
            type: "full",
            status: res.status === "queued" ? "PENDING" : res.status,
            message: res.message,
            created_at: new Date().toISOString(),
            progress: null,
            result: null,
          },
          ...prev,
        ]);
        setDialogType(null);
        setFundIdsInput("");
      } catch (e: unknown) {
        setSubmitError(e instanceof Error ? e.message : "操作失败，请稍后重试");
      } finally {
        setSubmitting(null);
      }
    } else if (dialogType === "daily") {
      setSubmitting("daily");
      try {
        const res = await fetchApiAuth<TaskResponse>("/etl/daily-refresh", {
          method: "POST",
        });
        setTasks((prev) => [
          {
            task_id: res.task_id,
            type: "daily",
            status: res.status === "queued" ? "PENDING" : res.status,
            message: res.message,
            created_at: new Date().toISOString(),
            progress: null,
            result: null,
          },
          ...prev,
        ]);
        setDialogType(null);
      } catch (e: unknown) {
        setSubmitError(e instanceof Error ? e.message : "操作失败，请稍后重试");
      } finally {
        setSubmitting(null);
      }
    }
  };

  // Open dialog with confirmation for daily
  const openDialog = (type: "incremental" | "full" | "daily") => {
    setFundIdsInput("");
    setUpdateAll(true);
    setDialogType(type);
  };

  // Count active tasks
  const activeTasks = tasks.filter(
    (t) => !TERMINAL_STATES.has(t.status)
  ).length;

  if (authError) {
    return (
      <div className="space-y-3">
        <PageHeader title="数据管理" description="ETL任务管理与数据维护" />
        <div className="bg-card border border-border rounded p-8 text-center">
          <Shield className="h-8 w-8 mx-auto mb-3 text-muted-foreground opacity-40" />
          <p className="text-[13px] text-muted-foreground">
            请登录管理员账户后查看
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <PageHeader
        title="数据管理"
        description="ETL任务管理与数据维护"
        actions={
          activeTasks > 0 ? (
            <Badge className="bg-blue-500/10 text-blue-600 border-blue-200 hover:bg-blue-500/10">
              <Loader2 className="h-3 w-3 mr-1 animate-spin" />
              {activeTasks} 个任务进行中
            </Badge>
          ) : undefined
        }
      />

      {/* Statistics Cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="bg-card border border-border rounded p-3">
          <div className="flex items-center gap-2 mb-1">
            <Database className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-[11px] text-muted-foreground">基金总数</span>
          </div>
          <p className="text-[18px] font-semibold tabular-nums">
            {fundTotal !== null ? fundTotal.toLocaleString() : "--"}
          </p>
        </div>
        <div className="bg-card border border-border rounded p-3">
          <div className="flex items-center gap-2 mb-1">
            <Activity className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-[11px] text-muted-foreground">
              已提交任务
            </span>
          </div>
          <p className="text-[18px] font-semibold tabular-nums">
            {tasks.length}
          </p>
        </div>
        <div className="bg-card border border-border rounded p-3">
          <div className="flex items-center gap-2 mb-1">
            <Loader2 className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-[11px] text-muted-foreground">
              进行中
            </span>
          </div>
          <p className="text-[18px] font-semibold tabular-nums text-blue-600">
            {activeTasks}
          </p>
        </div>
        <div className="bg-card border border-border rounded p-3">
          <div className="flex items-center gap-2 mb-1">
            <CheckCircle2 className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-[11px] text-muted-foreground">
              已完成
            </span>
          </div>
          <p className="text-[18px] font-semibold tabular-nums text-emerald-600">
            {tasks.filter((t) => t.status === "SUCCESS").length}
          </p>
        </div>
      </div>

      {/* Operations */}
      <div className="bg-card border border-border rounded">
        <div className="px-4 py-2 border-b border-border">
          <span className="text-[13px] font-medium">数据操作</span>
        </div>
        {(submitError || pollError) && (
          <div className="px-4 pt-2 space-y-1">
            {submitError && <p className="text-[11px] text-red-500">{submitError}</p>}
            {pollError && <p className="text-[11px] text-amber-600">{pollError}</p>}
          </div>
        )}
        <div className="px-4 py-3 flex flex-wrap gap-2">
          <Button
            size="sm"
            className="h-8 text-[12px]"
            onClick={() => openDialog("incremental")}
            disabled={submitting !== null}
          >
            <RefreshCw className="h-3.5 w-3.5 mr-1.5" />
            增量更新
          </Button>
          <Button
            size="sm"
            variant="outline"
            className="h-8 text-[12px]"
            onClick={() => openDialog("full")}
            disabled={submitting !== null}
          >
            <RotateCcw className="h-3.5 w-3.5 mr-1.5" />
            全量重建
          </Button>
          <Button
            size="sm"
            variant="outline"
            className="h-8 text-[12px]"
            onClick={() => openDialog("daily")}
            disabled={submitting !== null}
          >
            <Calendar className="h-3.5 w-3.5 mr-1.5" />
            每日刷新
          </Button>
        </div>
      </div>

      {/* Task List */}
      <div className="bg-card border border-border rounded">
        <div className="px-4 py-2 border-b border-border flex items-center justify-between">
          <span className="text-[13px] font-medium">任务列表</span>
          {tasks.length > 0 && (
            <Button
              variant="ghost"
              size="sm"
              className="h-6 px-2 text-[11px] text-muted-foreground"
              onClick={() =>
                setTasks((prev) =>
                  prev.filter((t) => !TERMINAL_STATES.has(t.status))
                )
              }
            >
              清除已完成
            </Button>
          )}
        </div>

        {tasks.length === 0 ? (
          <div className="h-40 flex items-center justify-center text-muted-foreground">
            <div className="text-center space-y-1">
              <Database className="mx-auto h-7 w-7 opacity-25" />
              <p className="text-[12px] opacity-60">暂无任务记录</p>
              <p className="text-[11px] opacity-40">
                使用上方按钮提交ETL任务
              </p>
            </div>
          </div>
        ) : (
          <div className="divide-y divide-border">
            {tasks.map((task) => (
              <div key={task.task_id} className="px-4 py-2.5">
                <div className="flex items-center justify-between mb-1">
                  <div className="flex items-center gap-2">
                    {statusBadge(task.status)}
                    <span className="text-[12px] font-medium">
                      {TASK_TYPE_LABELS[task.type] || task.type}
                    </span>
                    <span className="text-[10px] text-muted-foreground font-mono">
                      {task.task_id.slice(0, 8)}...
                    </span>
                  </div>
                  <span className="text-[11px] text-muted-foreground tabular-nums">
                    {formatTime(task.created_at)}
                  </span>
                </div>
                <p className="text-[11px] text-muted-foreground mb-1">
                  {task.message}
                </p>

                {/* Progress bar */}
                {(() => {
                  const p = task.progress;
                  if (!p || p.total == null || p.total <= 0) return null;
                  const pct = Math.min(100, Math.round(((p.processed ?? 0) / p.total) * 100));
                  return (
                    <div className="mt-1.5">
                      <div className="flex items-center justify-between mb-0.5">
                        <span className="text-[10px] text-muted-foreground">
                          {p.phase ? `${p.phase} - ` : ""}
                          {p.processed ?? 0}/{p.total}
                          {p.failed ? `（失败 ${p.failed}）` : ""}
                        </span>
                        <span className="text-[10px] text-muted-foreground tabular-nums">
                          {pct}%
                        </span>
                      </div>
                      <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full transition-all duration-500 ${
                            p.failed && p.failed > 0 ? "bg-amber-500" : "bg-blue-500"
                          }`}
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                    </div>
                  );
                })()}

                {/* Result display for completed tasks */}
                {task.status === "SUCCESS" && !!task.result && (
                  <div className="mt-1.5 p-1.5 bg-emerald-500/5 rounded text-[10px] text-emerald-700">
                    {typeof task.result === "string"
                      ? task.result
                      : JSON.stringify(task.result)}
                  </div>
                )}
                {task.status === "FAILURE" && !!task.result && (
                  <div className="mt-1.5 p-1.5 bg-red-500/5 rounded text-[10px] text-red-600">
                    {typeof task.result === "object" &&
                    task.result !== null &&
                    "error" in (task.result as Record<string, unknown>)
                      ? String(
                          (task.result as Record<string, unknown>).error
                        )
                      : JSON.stringify(task.result)}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Incremental Refresh Dialog */}
      <Dialog
        open={dialogType === "incremental"}
        onOpenChange={(open) => !open && setDialogType(null)}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="text-[14px]">增量净值更新</DialogTitle>
            <DialogDescription className="text-[12px]">
              增量更新仅获取最新的净值数据，不会删除已有记录。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <div className="flex items-center gap-3">
              <label className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="radio"
                  name="scope"
                  checked={updateAll}
                  onChange={() => setUpdateAll(true)}
                  className="accent-primary"
                />
                <span className="text-[12px]">更新所有基金</span>
              </label>
              <label className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="radio"
                  name="scope"
                  checked={!updateAll}
                  onChange={() => setUpdateAll(false)}
                  className="accent-primary"
                />
                <span className="text-[12px]">指定基金ID</span>
              </label>
            </div>
            {!updateAll && (
              <div>
                <label className="text-[11px] text-muted-foreground mb-1 block">
                  基金ID列表（逗号或空格分隔）
                </label>
                <Input
                  className="h-8 text-[12px]"
                  placeholder="例如: 1, 2, 3"
                  value={fundIdsInput}
                  onChange={(e) => setFundIdsInput(e.target.value)}
                />
              </div>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-[12px]"
              onClick={() => setDialogType(null)}
            >
              取消
            </Button>
            <Button
              size="sm"
              className="h-7 text-[12px]"
              onClick={submitTask}
              disabled={
                submitting !== null ||
                (!updateAll && parseFundIds(fundIdsInput).length === 0)
              }
            >
              {submitting === "incremental" && (
                <Loader2 className="h-3 w-3 animate-spin mr-1" />
              )}
              确认提交
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Full Rebuild Dialog */}
      <Dialog
        open={dialogType === "full"}
        onOpenChange={(open) => !open && setDialogType(null)}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="text-[14px] flex items-center gap-1.5">
              <AlertTriangle className="h-4 w-4 text-amber-500" />
              全量净值重建
            </DialogTitle>
            <DialogDescription className="text-[12px]">
              <span className="text-red-500 font-medium">
                警告：此操作将删除指定基金的所有已有净值数据并重新爬取。
              </span>
              <br />
              必须指定基金ID列表，不支持全量操作。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">
                基金ID列表（逗号或空格分隔，必填）
              </label>
              <Input
                className="h-8 text-[12px]"
                placeholder="例如: 1, 2, 3"
                value={fundIdsInput}
                onChange={(e) => setFundIdsInput(e.target.value)}
              />
              {fundIdsInput && parseFundIds(fundIdsInput).length > 0 && (
                <p className="text-[10px] text-muted-foreground mt-1">
                  已识别 {parseFundIds(fundIdsInput).length} 个基金ID:{" "}
                  {parseFundIds(fundIdsInput).join(", ")}
                </p>
              )}
            </div>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-[12px]"
              onClick={() => setDialogType(null)}
            >
              取消
            </Button>
            <Button
              size="sm"
              variant="destructive"
              className="h-7 text-[12px]"
              onClick={submitTask}
              disabled={
                submitting !== null ||
                parseFundIds(fundIdsInput).length === 0
              }
            >
              {submitting === "full" && (
                <Loader2 className="h-3 w-3 animate-spin mr-1" />
              )}
              确认重建
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Daily Refresh Dialog */}
      <Dialog
        open={dialogType === "daily"}
        onOpenChange={(open) => !open && setDialogType(null)}
      >
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle className="text-[14px]">手动每日刷新</DialogTitle>
            <DialogDescription className="text-[12px]">
              将触发完整的每日数据刷新流程，包括增量净值更新和数据质量检查。确认要执行吗？
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-[12px]"
              onClick={() => setDialogType(null)}
            >
              取消
            </Button>
            <Button
              size="sm"
              className="h-7 text-[12px]"
              onClick={submitTask}
              disabled={submitting !== null}
            >
              {submitting === "daily" && (
                <Loader2 className="h-3 w-3 animate-spin mr-1" />
              )}
              确认执行
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
