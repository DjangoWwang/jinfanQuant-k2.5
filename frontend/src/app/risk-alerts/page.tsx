"use client";

import { useEffect, useState, useCallback } from "react";
import {
  AlertTriangle,
  AlertCircle,
  Bell,
  CheckCircle2,
  Eye,
  EyeOff,
  Loader2,
  Plus,
  X,
  Shield,
  ShieldAlert,
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
import { fetchApiAuth, type AlertEvent, type RiskRule } from "@/lib/api";

/* --- Helpers --- */

function SeverityIcon({ severity }: { severity: string }) {
  if (severity === "critical") {
    return <AlertCircle className="h-4 w-4 text-red-500 shrink-0" />;
  }
  return <AlertTriangle className="h-4 w-4 text-amber-500 shrink-0" />;
}

function formatTime(iso: string): string {
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    const now = new Date();
    const diff = now.getTime() - d.getTime();
    if (diff < 60000) return "刚刚";
    if (diff < 3600000) return `${Math.floor(diff / 60000)}分钟前`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}小时前`;
    return d.toLocaleDateString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
  } catch {
    return iso;
  }
}

/* --- Page --- */

export default function RiskAlertsPage() {
  const [activeTab, setActiveTab] = useState("events");
  const [events, setEvents] = useState<AlertEvent[]>([]);
  const [rules, setRules] = useState<RiskRule[]>([]);
  const [loadingEvents, setLoadingEvents] = useState(true);
  const [loadingRules, setLoadingRules] = useState(false);
  const [authError, setAuthError] = useState(false);

  // Filters
  const [severityFilter, setSeverityFilter] = useState<"all" | "warning" | "critical">("all");
  const [readFilter, setReadFilter] = useState<"all" | "unread" | "read">("all");

  // Create rule form
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newRule, setNewRule] = useState({
    name: "",
    rule_type: "drawdown",
    target_type: "product",
    threshold: "",
    comparison: "gt",
    severity: "warning",
  });
  const [creating, setCreating] = useState(false);

  // Load events
  const loadEvents = useCallback(async () => {
    setLoadingEvents(true);
    try {
      const data = await fetchApiAuth<AlertEvent[]>("/alerts/events");
      setEvents(Array.isArray(data) ? data : []);
      setAuthError(false);
    } catch (e: unknown) {
      if (e instanceof Error && e.message === "AUTH_REQUIRED") {
        setAuthError(true);
      }
      setEvents([]);
    } finally {
      setLoadingEvents(false);
    }
  }, []);

  // Load rules
  const loadRules = useCallback(async () => {
    setLoadingRules(true);
    try {
      const data = await fetchApiAuth<RiskRule[]>("/alerts/rules");
      setRules(Array.isArray(data) ? data : []);
      setAuthError(false);
    } catch (e: unknown) {
      if (e instanceof Error && e.message === "AUTH_REQUIRED") {
        setAuthError(true);
      }
      setRules([]);
    } finally {
      setLoadingRules(false);
    }
  }, []);

  useEffect(() => {
    loadEvents();
  }, [loadEvents]);

  useEffect(() => {
    if (activeTab === "rules") {
      loadRules();
    }
  }, [activeTab, loadRules]);

  // Toggle read status
  const toggleRead = async (event: AlertEvent) => {
    try {
      await fetchApiAuth(`/alerts/events/${event.id}/read`, {
        method: "PUT",
      });
      setEvents((prev) =>
        prev.map((e) => (e.id === event.id ? { ...e, is_read: true } : e))
      );
    } catch {
      /* ignore */
    }
  };

  // Mark all read
  const markAllRead = async () => {
    try {
      await fetchApiAuth("/alerts/events/read-all", { method: "PUT" });
      setEvents((prev) => prev.map((e) => ({ ...e, is_read: true })));
    } catch {
      /* ignore */
    }
  };

  // Toggle rule active
  const toggleRuleActive = async (rule: RiskRule) => {
    try {
      await fetchApiAuth(`/alerts/rules/${rule.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_active: !rule.is_active }),
      });
      setRules((prev) =>
        prev.map((r) => (r.id === rule.id ? { ...r, is_active: !r.is_active } : r))
      );
    } catch {
      /* ignore */
    }
  };

  // Create rule
  const createRule = async () => {
    if (!newRule.name.trim() || !newRule.threshold) return;
    setCreating(true);
    try {
      const created = await fetchApiAuth<RiskRule>("/alerts/rules", {
        method: "POST",
        body: JSON.stringify({
          ...newRule,
          threshold: parseFloat(newRule.threshold),
          target_id: null,
        }),
      });
      setRules((prev) => [created, ...prev]);
      setShowCreateForm(false);
      setNewRule({ name: "", rule_type: "drawdown", target_type: "product", threshold: "", comparison: "gt", severity: "warning" });
    } catch {
      /* ignore */
    } finally {
      setCreating(false);
    }
  };

  // Filter events
  const filteredEvents = events.filter((e) => {
    if (severityFilter !== "all" && e.severity !== severityFilter) return false;
    if (readFilter === "unread" && e.is_read) return false;
    if (readFilter === "read" && !e.is_read) return false;
    return true;
  });

  const unreadCount = events.filter((e) => !e.is_read).length;

  if (authError) {
    return (
      <div className="space-y-3">
        <PageHeader title="风险预警" description="监控规则与预警事件" />
        <div className="bg-card border border-border rounded p-8 text-center">
          <Shield className="h-8 w-8 mx-auto mb-3 text-muted-foreground opacity-40" />
          <p className="text-[13px] text-muted-foreground">请登录后查看风险预警</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <PageHeader
        title="风险预警"
        description="监控规则与预警事件"
        actions={
          unreadCount > 0 ? (
            <Badge className="bg-red-500/10 text-red-600 border-red-200 hover:bg-red-500/10">
              {unreadCount} 条未读
            </Badge>
          ) : undefined
        }
      />

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="events" className="text-[12px]">
            <Bell className="h-3.5 w-3.5 mr-1" />
            预警事件
            {unreadCount > 0 && (
              <span className="ml-1.5 px-1.5 py-0.5 rounded-full bg-red-500 text-white text-[10px] leading-none">
                {unreadCount}
              </span>
            )}
          </TabsTrigger>
          <TabsTrigger value="rules" className="text-[12px]">
            <ShieldAlert className="h-3.5 w-3.5 mr-1" />
            监控规则
          </TabsTrigger>
        </TabsList>

        {/* Events Tab */}
        <TabsContent value="events">
          <div className="bg-card border border-border rounded">
            {/* Filters */}
            <div className="flex items-center justify-between px-4 py-2 border-b border-border">
              <div className="flex items-center gap-2">
                <span className="text-[11px] text-muted-foreground">严重程度:</span>
                {(["all", "critical", "warning"] as const).map((s) => (
                  <Button
                    key={s}
                    variant={severityFilter === s ? "default" : "ghost"}
                    size="sm"
                    className="h-6 px-2 text-[11px]"
                    onClick={() => setSeverityFilter(s)}
                  >
                    {s === "all" ? "全部" : s === "critical" ? "严重" : "警告"}
                  </Button>
                ))}
                <span className="text-border mx-1">|</span>
                <span className="text-[11px] text-muted-foreground">状态:</span>
                {(["all", "unread", "read"] as const).map((s) => (
                  <Button
                    key={s}
                    variant={readFilter === s ? "default" : "ghost"}
                    size="sm"
                    className="h-6 px-2 text-[11px]"
                    onClick={() => setReadFilter(s)}
                  >
                    {s === "all" ? "全部" : s === "unread" ? "未读" : "已读"}
                  </Button>
                ))}
              </div>
              <Button
                variant="ghost"
                size="sm"
                className="h-7 text-[11px]"
                onClick={markAllRead}
                disabled={unreadCount === 0}
              >
                <CheckCircle2 className="h-3.5 w-3.5 mr-1" />
                标记全部已读
              </Button>
            </div>

            {/* Events table */}
            {loadingEvents ? (
              <div className="h-48 flex items-center justify-center">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : filteredEvents.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="h-7 text-[11px] font-normal w-10"></TableHead>
                    <TableHead className="h-7 text-[11px] font-normal">目标</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal">预警信息</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-right">时间</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center w-16">状态</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredEvents.map((event) => (
                    <TableRow
                      key={event.id}
                      className={`text-[12px] ${!event.is_read ? "bg-primary/[0.02]" : ""}`}
                    >
                      <TableCell className="py-1.5">
                        <SeverityIcon severity={event.severity} />
                      </TableCell>
                      <TableCell className="py-1.5 font-medium">
                        {event.target_name}
                        <span className="text-[10px] text-muted-foreground ml-1">
                          ({event.target_type})
                        </span>
                      </TableCell>
                      <TableCell className="py-1.5 text-muted-foreground">
                        {event.message}
                      </TableCell>
                      <TableCell className="py-1.5 text-right text-muted-foreground tabular-nums">
                        {formatTime(event.created_at)}
                      </TableCell>
                      <TableCell className="py-1.5 text-center">
                        <button
                          onClick={() => toggleRead(event)}
                          className="hover:opacity-70 transition-opacity"
                          title={event.is_read ? "标记为未读" : "标记为已读"}
                        >
                          {event.is_read ? (
                            <Eye className="h-3.5 w-3.5 text-muted-foreground" />
                          ) : (
                            <EyeOff className="h-3.5 w-3.5 text-primary" />
                          )}
                        </button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <div className="h-48 flex items-center justify-center text-muted-foreground">
                <div className="text-center space-y-1">
                  <Bell className="mx-auto h-7 w-7 opacity-25" />
                  <p className="text-[12px] opacity-60">暂无预警事件</p>
                </div>
              </div>
            )}
          </div>
        </TabsContent>

        {/* Rules Tab */}
        <TabsContent value="rules">
          <div className="bg-card border border-border rounded">
            <div className="flex items-center justify-between px-4 py-2 border-b border-border">
              <span className="text-[13px] font-medium">监控规则列表</span>
              <Button
                size="sm"
                className="h-7 text-[11px]"
                onClick={() => setShowCreateForm(!showCreateForm)}
              >
                {showCreateForm ? (
                  <>
                    <X className="h-3.5 w-3.5 mr-1" />
                    取消
                  </>
                ) : (
                  <>
                    <Plus className="h-3.5 w-3.5 mr-1" />
                    创建规则
                  </>
                )}
              </Button>
            </div>

            {/* Create rule form */}
            {showCreateForm && (
              <div className="px-4 py-3 border-b border-border bg-muted/30 space-y-2">
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2">
                  <div>
                    <label className="text-[10px] text-muted-foreground mb-0.5 block">规则名称</label>
                    <Input
                      className="h-7 text-[12px]"
                      placeholder="如: 回撤超限"
                      value={newRule.name}
                      onChange={(e) => setNewRule({ ...newRule, name: e.target.value })}
                    />
                  </div>
                  <div>
                    <label className="text-[10px] text-muted-foreground mb-0.5 block">规则类型</label>
                    <select
                      className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                      value={newRule.rule_type}
                      onChange={(e) => setNewRule({ ...newRule, rule_type: e.target.value })}
                    >
                      <option value="drawdown">最大回撤</option>
                      <option value="volatility">波动率</option>
                      <option value="nav_anomaly">净值异动</option>
                      <option value="concentration">集中度</option>
                    </select>
                  </div>
                  <div>
                    <label className="text-[10px] text-muted-foreground mb-0.5 block">目标类型</label>
                    <select
                      className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                      value={newRule.target_type}
                      onChange={(e) => setNewRule({ ...newRule, target_type: e.target.value })}
                    >
                      <option value="product">产品</option>
                      <option value="fund">基金</option>
                    </select>
                  </div>
                  <div>
                    <label className="text-[10px] text-muted-foreground mb-0.5 block">阈值</label>
                    <Input
                      className="h-7 text-[12px]"
                      type="number"
                      step="0.01"
                      placeholder="如: 0.1"
                      value={newRule.threshold}
                      onChange={(e) => setNewRule({ ...newRule, threshold: e.target.value })}
                    />
                  </div>
                  <div>
                    <label className="text-[10px] text-muted-foreground mb-0.5 block">比较方式</label>
                    <select
                      className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                      value={newRule.comparison}
                      onChange={(e) => setNewRule({ ...newRule, comparison: e.target.value })}
                    >
                      <option value="gt">大于</option>
                      <option value="lt">小于</option>
                      <option value="gte">大于等于</option>
                      <option value="lte">小于等于</option>
                    </select>
                  </div>
                  <div>
                    <label className="text-[10px] text-muted-foreground mb-0.5 block">严重程度</label>
                    <select
                      className="h-7 w-full rounded border border-border bg-background px-2 text-[12px]"
                      value={newRule.severity}
                      onChange={(e) => setNewRule({ ...newRule, severity: e.target.value })}
                    >
                      <option value="warning">警告</option>
                      <option value="critical">严重</option>
                    </select>
                  </div>
                </div>
                <div className="flex justify-end">
                  <Button
                    size="sm"
                    className="h-7 text-[11px]"
                    onClick={createRule}
                    disabled={!newRule.name.trim() || !newRule.threshold || creating}
                  >
                    {creating && <Loader2 className="h-3 w-3 animate-spin mr-1" />}
                    保存规则
                  </Button>
                </div>
              </div>
            )}

            {/* Rules table */}
            {loadingRules ? (
              <div className="h-48 flex items-center justify-center">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : rules.length > 0 ? (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="h-7 text-[11px] font-normal">规则名称</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal">类型</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-right">阈值</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center">严重程度</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center">状态</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rules.map((rule) => (
                    <TableRow key={rule.id} className="text-[12px]">
                      <TableCell className="py-1.5 font-medium">{rule.name}</TableCell>
                      <TableCell className="py-1.5 text-muted-foreground">
                        {rule.rule_type === "drawdown"
                          ? "最大回撤"
                          : rule.rule_type === "volatility"
                          ? "波动率"
                          : rule.rule_type === "nav_anomaly"
                          ? "净值异动"
                          : rule.rule_type === "concentration"
                          ? "集中度"
                          : rule.rule_type}
                      </TableCell>
                      <TableCell className="py-1.5 text-right tabular-nums">
                        {rule.comparison === "gt"
                          ? ">"
                          : rule.comparison === "lt"
                          ? "<"
                          : rule.comparison === "gte"
                          ? ">="
                          : "<="}{" "}
                        {rule.threshold}
                      </TableCell>
                      <TableCell className="py-1.5 text-center">
                        <Badge
                          className={`text-[10px] ${
                            rule.severity === "critical"
                              ? "bg-red-500/10 text-red-600 border-red-200 hover:bg-red-500/10"
                              : "bg-amber-500/10 text-amber-600 border-amber-200 hover:bg-amber-500/10"
                          }`}
                        >
                          {rule.severity === "critical" ? "严重" : "警告"}
                        </Badge>
                      </TableCell>
                      <TableCell className="py-1.5 text-center">
                        <button
                          onClick={() => toggleRuleActive(rule)}
                          className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                            rule.is_active ? "bg-primary" : "bg-muted-foreground/20"
                          }`}
                        >
                          <span
                            className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white transition-transform shadow-sm ${
                              rule.is_active ? "translate-x-[18px]" : "translate-x-[3px]"
                            }`}
                          />
                        </button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : (
              <div className="h-48 flex items-center justify-center text-muted-foreground">
                <div className="text-center space-y-1">
                  <ShieldAlert className="mx-auto h-7 w-7 opacity-25" />
                  <p className="text-[12px] opacity-60">暂无监控规则，点击上方按钮创建</p>
                </div>
              </div>
            )}
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
