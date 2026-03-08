"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import {
  Activity,
  Database,
  HardDrive,
  Cpu,
  Clock,
  RefreshCw,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Shield,
  Loader2,
  CalendarClock,
  Server,
  WifiOff,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { fetchApiAuth } from "@/lib/api";

/* ---------- Types ---------- */

type HealthStatus = "ok" | "healthy" | "warning" | "degraded" | "error" | "unavailable" | "unknown" | (string & {});

interface ComponentHealth {
  status: HealthStatus;
  latency_ms?: number;
  detail?: string | null;
  table_rows?: Record<string, number>;
  db_size?: string | null;
  used_memory_human?: string;
  used_memory_peak_human?: string;
  connected_clients?: number;
  workers?: { name: string; active: number; scheduled: number; reserved: number }[];
  worker_count?: number;
  active_tasks?: number;
  scheduled_tasks?: number;
  reserved_tasks?: number;
}

interface FreshnessData {
  status: string;
  check_date?: string;
  stale_threshold_days?: number;
  funds?: {
    total: number;
    active: number;
    has_data: number;
    latest_nav_date: string | null;
    stale_count: number;
  };
  products?: {
    total: number;
    latest_nav_date: string | null;
    stale_count: number;
  };
  valuation?: {
    latest_snapshot_date: string | null;
  };
}

interface OverviewData {
  status: string;
  components: {
    database: ComponentHealth;
    redis: ComponentHealth;
    celery: ComponentHealth;
  };
  data_freshness: FreshnessData;
}

interface ScheduleData {
  beat_enabled: boolean;
  schedules: Record<string, { time: string; days: string; task: string }>;
}

/* ---------- Helpers ---------- */

const AUTO_REFRESH_MS = 30_000;

function isAuthError(e: unknown): boolean {
  if (e instanceof Error) {
    const msg = e.message;
    return msg === "AUTH_REQUIRED" || msg.includes("401") || msg.includes("403");
  }
  return false;
}

function statusIcon(status: string) {
  switch (status) {
    case "ok":
    case "healthy":
      return <CheckCircle2 className="h-5 w-5 text-emerald-500" />;
    case "warning":
    case "degraded":
      return <AlertTriangle className="h-5 w-5 text-amber-500" />;
    case "error":
    case "unavailable":
      return <XCircle className="h-5 w-5 text-red-500" />;
    default:
      return <Activity className="h-5 w-5 text-muted-foreground" />;
  }
}

const STATUS_BADGE_MAP: Record<string, { label: string; cls: string }> = {
  ok: { label: "正常", cls: "bg-emerald-500/10 text-emerald-600 border-emerald-200" },
  healthy: { label: "健康", cls: "bg-emerald-500/10 text-emerald-600 border-emerald-200" },
  warning: { label: "告警", cls: "bg-amber-500/10 text-amber-600 border-amber-200" },
  degraded: { label: "降级", cls: "bg-amber-500/10 text-amber-600 border-amber-200" },
  error: { label: "异常", cls: "bg-red-500/10 text-red-600 border-red-200" },
  unavailable: { label: "不可用", cls: "bg-red-500/10 text-red-600 border-red-200" },
};

function statusBadge(status: string) {
  const info = STATUS_BADGE_MAP[status] ?? { label: status, cls: "bg-muted text-muted-foreground" };
  return <Badge className={`${info.cls} text-[10px]`}>{info.label}</Badge>;
}

const SCHEDULE_LABELS: Record<string, string> = {
  daily_data_refresh: "每日数据刷新",
  risk_check: "风险规则检查",
  nav_calc: "产品净值计算",
};

/* ---------- Component ---------- */

export default function SystemMonitorPage() {
  const [overview, setOverview] = useState<OverviewData | null>(null);
  const [schedule, setSchedule] = useState<ScheduleData | null>(null);
  const [loading, setLoading] = useState(true);
  const [authError, setAuthError] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const [lastAttempt, setLastAttempt] = useState<Date | null>(null);

  // Request counter to prevent stale responses from overwriting newer data
  const requestId = useRef(0);
  const mountedRef = useRef(true);

  const fetchData = useCallback(async (showLoading = false) => {
    const thisRequest = ++requestId.current;
    if (showLoading) setLoading(true);

    try {
      // Use allSettled so partial failures don't block the other
      const [ovResult, schResult] = await Promise.allSettled([
        fetchApiAuth<OverviewData>("/monitor/overview"),
        fetchApiAuth<ScheduleData>("/etl/schedule"),
      ]);

      // Stale response guard
      if (!mountedRef.current || thisRequest !== requestId.current) return;

      let hasError = false;

      if (ovResult.status === "fulfilled") {
        setOverview(ovResult.value);
      } else {
        if (isAuthError(ovResult.reason)) { setAuthError(true); return; }
        hasError = true;
      }

      if (schResult.status === "fulfilled") {
        setSchedule(schResult.value);
      } else {
        if (isAuthError(schResult.reason)) { setAuthError(true); return; }
        hasError = true;
      }

      setAuthError(false);
      setLastAttempt(new Date());
      if (hasError) {
        setFetchError("部分数据获取失败，显示的可能是旧数据");
      } else {
        setFetchError(null);
        setLastRefresh(new Date());
      }
    } catch (e) {
      if (!mountedRef.current || thisRequest !== requestId.current) return;
      if (isAuthError(e)) {
        setAuthError(true);
      } else {
        setLastAttempt(new Date());
        setFetchError("数据获取失败，请检查网络连接");
      }
    } finally {
      if (mountedRef.current && thisRequest === requestId.current) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    fetchData(true);
    const timer = setInterval(() => fetchData(false), AUTO_REFRESH_MS);
    return () => {
      mountedRef.current = false;
      clearInterval(timer);
    };
  }, [fetchData]);

  /* --- Auth gate --- */
  if (authError) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground gap-3 p-10">
        <Shield className="h-10 w-10 opacity-30" />
        <p className="text-sm">请登录管理员账户后查看系统监控</p>
      </div>
    );
  }

  /* --- Loading --- */
  if (loading && !overview) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const db = overview?.components?.database;
  const redis = overview?.components?.redis;
  const celery = overview?.components?.celery;
  const freshness = overview?.data_freshness;

  return (
    <div className="flex-1 overflow-y-auto">
      <PageHeader
        title="系统监控"
        description="基础设施健康状况、数据新鲜度、定时任务调度"
        actions={
          <div className="flex items-center gap-2">
            {(lastRefresh || lastAttempt) && (
              <span className="text-[10px] text-muted-foreground">
                {lastRefresh
                  ? `${lastRefresh.toLocaleTimeString("zh-CN")} 成功`
                  : `${lastAttempt!.toLocaleTimeString("zh-CN")} 尝试`}
              </span>
            )}
            <Button
              variant="outline"
              size="sm"
              onClick={() => fetchData(true)}
              disabled={loading}
            >
              {loading ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" />
              ) : (
                <RefreshCw className="h-3.5 w-3.5 mr-1" />
              )}
              刷新
            </Button>
          </div>
        }
      />

      <div className="p-4 space-y-4">
        {/* ===== Fetch Error Banner ===== */}
        {fetchError && (
          <div className="flex items-center gap-2 px-4 py-2 rounded-lg border border-amber-200 bg-amber-50 text-amber-700 text-[11px]">
            <WifiOff className="h-4 w-4 shrink-0" />
            <span>{fetchError}</span>
            <Button
              variant="ghost"
              size="xs"
              className="ml-auto text-amber-700 hover:text-amber-900"
              onClick={() => fetchData(true)}
            >
              重试
            </Button>
          </div>
        )}

        {/* ===== Overall Status ===== */}
        <div className="flex items-center gap-3 px-4 py-3 rounded-lg border bg-card">
          {statusIcon(overview?.status ?? "unknown")}
          <div>
            <p className="text-sm font-medium">系统整体状态</p>
            <p className="text-[11px] text-muted-foreground">
              {overview?.status === "healthy"
                ? "所有组件运行正常"
                : overview?.status === "degraded"
                ? "部分组件存在告警"
                : overview?.status === "error"
                ? "存在组件异常"
                : "状态未知"}
            </p>
          </div>
          <div className="ml-auto">{statusBadge(overview?.status ?? "unknown")}</div>
        </div>

        {/* ===== Component Health Cards ===== */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          {/* Database */}
          <div className="rounded-lg border bg-card p-4 space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Database className="h-4 w-4 text-blue-500" />
                <span className="text-[13px] font-medium">PostgreSQL</span>
              </div>
              {statusBadge(db?.status ?? "unknown")}
            </div>
            <div className="space-y-1.5 text-[11px] text-muted-foreground">
              {db?.latency_ms != null && (
                <div className="flex justify-between">
                  <span>延迟</span>
                  <span className="font-mono">{db.latency_ms}ms</span>
                </div>
              )}
              {db?.db_size && (
                <div className="flex justify-between">
                  <span>数据库大小</span>
                  <span className="font-mono">{db.db_size}</span>
                </div>
              )}
              {db?.table_rows && (
                <div className="flex justify-between">
                  <span>表数量</span>
                  <span className="font-mono">{Object.keys(db.table_rows).length}</span>
                </div>
              )}
              {db?.detail && <p className="text-red-500 text-[10px]">{db.detail}</p>}
            </div>
            {db?.table_rows && (
              <details className="text-[10px]">
                <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
                  表行数明细
                </summary>
                <div className="mt-1 max-h-32 overflow-y-auto space-y-0.5">
                  {Object.entries(db.table_rows)
                    .sort(([, a], [, b]) => Number(b) - Number(a))
                    .map(([table, rows]) => (
                      <div key={table} className="flex justify-between font-mono text-muted-foreground">
                        <span>{table}</span>
                        <span>{rows.toLocaleString()}</span>
                      </div>
                    ))}
                </div>
              </details>
            )}
          </div>

          {/* Redis */}
          <div className="rounded-lg border bg-card p-4 space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <HardDrive className="h-4 w-4 text-red-500" />
                <span className="text-[13px] font-medium">Redis</span>
              </div>
              {statusBadge(redis?.status ?? "unknown")}
            </div>
            <div className="space-y-1.5 text-[11px] text-muted-foreground">
              {redis?.latency_ms != null && (
                <div className="flex justify-between">
                  <span>延迟</span>
                  <span className="font-mono">{redis.latency_ms}ms</span>
                </div>
              )}
              {redis?.used_memory_human && (
                <div className="flex justify-between">
                  <span>已用内存</span>
                  <span className="font-mono">{redis.used_memory_human}</span>
                </div>
              )}
              {redis?.used_memory_peak_human && (
                <div className="flex justify-between">
                  <span>峰值内存</span>
                  <span className="font-mono">{redis.used_memory_peak_human}</span>
                </div>
              )}
              {redis?.connected_clients != null && (
                <div className="flex justify-between">
                  <span>连接数</span>
                  <span className="font-mono">{redis.connected_clients}</span>
                </div>
              )}
              {redis?.detail && <p className="text-red-500 text-[10px]">{redis.detail}</p>}
            </div>
          </div>

          {/* Celery */}
          <div className="rounded-lg border bg-card p-4 space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Cpu className="h-4 w-4 text-purple-500" />
                <span className="text-[13px] font-medium">Celery</span>
              </div>
              {statusBadge(celery?.status ?? "unknown")}
            </div>
            <div className="space-y-1.5 text-[11px] text-muted-foreground">
              {celery?.worker_count != null && (
                <div className="flex justify-between">
                  <span>Worker 数</span>
                  <span className="font-mono">{celery.worker_count}</span>
                </div>
              )}
              {celery?.active_tasks != null && (
                <div className="flex justify-between">
                  <span>活跃任务</span>
                  <span className="font-mono">{celery.active_tasks}</span>
                </div>
              )}
              {celery?.scheduled_tasks != null && (
                <div className="flex justify-between">
                  <span>已调度</span>
                  <span className="font-mono">{celery.scheduled_tasks}</span>
                </div>
              )}
              {celery?.reserved_tasks != null && (
                <div className="flex justify-between">
                  <span>队列等待</span>
                  <span className="font-mono">{celery.reserved_tasks}</span>
                </div>
              )}
              {celery?.detail && <p className="text-red-500 text-[10px]">{celery.detail}</p>}
            </div>
            {celery?.workers && celery.workers.length > 0 && (
              <details className="text-[10px]">
                <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
                  Worker 明细
                </summary>
                <div className="mt-1 space-y-1">
                  {celery.workers.map((w) => (
                    <div key={w.name} className="flex items-center gap-2 font-mono text-muted-foreground">
                      <Server className="h-3 w-3 shrink-0" />
                      <span className="truncate flex-1">{w.name.split("@").pop() ?? w.name}</span>
                      <span>活跃:{w.active}</span>
                    </div>
                  ))}
                </div>
              </details>
            )}
          </div>
        </div>

        {/* ===== Data Freshness ===== */}
        <div className="rounded-lg border bg-card p-4 space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Clock className="h-4 w-4 text-amber-500" />
              <span className="text-[13px] font-medium">数据新鲜度</span>
            </div>
            {freshness && statusBadge(freshness.status)}
          </div>

          {freshness && (
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              {/* Fund freshness */}
              <div className="rounded border p-3 space-y-1.5">
                <p className="text-[12px] font-medium">基金净值</p>
                <div className="space-y-1 text-[11px] text-muted-foreground">
                  <div className="flex justify-between">
                    <span>总数 / 活跃 / 有数据</span>
                    <span className="font-mono">
                      {freshness.funds?.total ?? "-"} / {freshness.funds?.active ?? "-"} / {freshness.funds?.has_data ?? "-"}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span>最新净值日期</span>
                    <span className="font-mono">{freshness.funds?.latest_nav_date ?? "无"}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>陈旧基金数</span>
                    <span className={`font-mono ${(freshness.funds?.stale_count ?? 0) > 0 ? "text-amber-500" : ""}`}>
                      {freshness.funds?.stale_count ?? 0}
                    </span>
                  </div>
                </div>
              </div>

              {/* Product freshness */}
              <div className="rounded border p-3 space-y-1.5">
                <p className="text-[12px] font-medium">产品净值</p>
                <div className="space-y-1 text-[11px] text-muted-foreground">
                  <div className="flex justify-between">
                    <span>活跃产品数</span>
                    <span className="font-mono">{freshness.products?.total ?? "-"}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>最新净值日期</span>
                    <span className="font-mono">{freshness.products?.latest_nav_date ?? "无"}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>陈旧产品数</span>
                    <span className={`font-mono ${(freshness.products?.stale_count ?? 0) > 0 ? "text-amber-500" : ""}`}>
                      {freshness.products?.stale_count ?? 0}
                    </span>
                  </div>
                </div>
              </div>

              {/* Valuation */}
              <div className="rounded border p-3 space-y-1.5">
                <p className="text-[12px] font-medium">估值快照</p>
                <div className="space-y-1 text-[11px] text-muted-foreground">
                  <div className="flex justify-between">
                    <span>最新估值日期</span>
                    <span className="font-mono">{freshness.valuation?.latest_snapshot_date ?? "无"}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>陈旧阈值</span>
                    <span className="font-mono">{freshness.stale_threshold_days ?? 7} 天</span>
                  </div>
                  <div className="flex justify-between">
                    <span>检查日期</span>
                    <span className="font-mono">{freshness.check_date ?? "-"}</span>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* ===== Celery Beat Schedule ===== */}
        <div className="rounded-lg border bg-card p-4 space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <CalendarClock className="h-4 w-4 text-indigo-500" />
              <span className="text-[13px] font-medium">定时任务调度</span>
            </div>
            {schedule && (
              <Badge
                className={`text-[10px] ${
                  schedule.beat_enabled
                    ? "bg-emerald-500/10 text-emerald-600 border-emerald-200"
                    : "bg-muted text-muted-foreground"
                }`}
              >
                {schedule.beat_enabled ? "已启用" : "未启用"}
              </Badge>
            )}
          </div>

          {schedule?.schedules && Object.keys(schedule.schedules).length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="border-b text-muted-foreground">
                    <th className="text-left py-1.5 pr-4 font-medium">任务</th>
                    <th className="text-left py-1.5 pr-4 font-medium">执行时间</th>
                    <th className="text-left py-1.5 pr-4 font-medium">执行日</th>
                    <th className="text-left py-1.5 font-medium">任务标识</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(schedule.schedules).map(([key, val]) => (
                    <tr key={key} className="border-b border-dashed last:border-0">
                      <td className="py-1.5 pr-4 font-medium">{SCHEDULE_LABELS[key] ?? key}</td>
                      <td className="py-1.5 pr-4 font-mono">{val.time}</td>
                      <td className="py-1.5 pr-4">{val.days}</td>
                      <td className="py-1.5 text-muted-foreground font-mono text-[10px]">{val.task}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            !loading && <p className="text-[11px] text-muted-foreground text-center py-3">暂无定时任务配置</p>
          )}
        </div>
      </div>
    </div>
  );
}
