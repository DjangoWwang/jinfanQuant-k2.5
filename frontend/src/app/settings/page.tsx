"use client";

import { useEffect, useState, useCallback } from "react";
import {
  Users,
  UserPlus,
  Shield,
  ShieldCheck,
  ShieldOff,
  KeyRound,
  Pencil,
  Loader2,
  ScrollText,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  RefreshCw,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { fetchApiAuth } from "@/lib/api";

/* --- Types --- */

interface UserInfo {
  id: number;
  username: string;
  email: string | null;
  display_name: string | null;
  role: string;
  is_active: boolean;
  last_login_at: string | null;
  created_at: string | null;
}

interface AuditLogItem {
  id: number;
  user_id: number;
  username: string;
  action: string;
  target_type: string | null;
  target_id: number | null;
  detail: string | null;
  ip_address: string | null;
  created_at: string | null;
}

/* --- Helpers --- */

const ROLE_LABELS: Record<string, string> = {
  admin: "管理员",
  analyst: "分析师",
  viewer: "观察者",
};

const ACTION_LABELS: Record<string, string> = {
  "user.create": "创建用户",
  "user.update": "修改用户",
  "user.enable": "启用用户",
  "user.disable": "禁用用户",
  "user.reset_password": "重置密码",
};

function RoleBadge({ role }: { role: string }) {
  const colorMap: Record<string, string> = {
    admin: "bg-purple-500/10 text-purple-600 border-purple-200 hover:bg-purple-500/10",
    analyst: "bg-blue-500/10 text-blue-600 border-blue-200 hover:bg-blue-500/10",
    viewer: "bg-gray-500/10 text-gray-600 border-gray-200 hover:bg-gray-500/10",
  };
  return (
    <Badge className={`text-[10px] ${colorMap[role] || colorMap.viewer}`}>
      {ROLE_LABELS[role] || role}
    </Badge>
  );
}

function isPasswordValid(value: string): boolean {
  return value.length >= 8 && value.length <= 128 && /[a-zA-Z]/.test(value) && /\d/.test(value);
}

function formatDateTime(iso: string | null): string {
  if (!iso) return "--";
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return d.toLocaleString("zh-CN", {
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

/* --- Page --- */

export default function SettingsPage() {
  const [activeTab, setActiveTab] = useState("users");
  const [authError, setAuthError] = useState(false);
  const [currentUser, setCurrentUser] = useState<UserInfo | null>(null);

  // --- Users ---
  const [users, setUsers] = useState<UserInfo[]>([]);
  const [loadingUsers, setLoadingUsers] = useState(true);
  const [userError, setUserError] = useState("");

  // --- Create user dialog ---
  const [showCreate, setShowCreate] = useState(false);
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newRole, setNewRole] = useState("analyst");
  const [newEmail, setNewEmail] = useState("");
  const [newDisplayName, setNewDisplayName] = useState("");
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState("");

  // --- Edit user dialog ---
  const [editUser, setEditUser] = useState<UserInfo | null>(null);
  const [editRole, setEditRole] = useState("");
  const [editDisplayName, setEditDisplayName] = useState("");
  const [editEmail, setEditEmail] = useState("");
  const [saving, setSaving] = useState(false);
  const [editError, setEditError] = useState("");

  // --- Reset password dialog ---
  const [resetUser, setResetUser] = useState<UserInfo | null>(null);
  const [resetPassword, setResetPassword] = useState("");
  const [resetting, setResetting] = useState(false);
  const [resetError, setResetError] = useState("");

  // --- Confirm disable dialog ---
  const [confirmDisableUser, setConfirmDisableUser] = useState<UserInfo | null>(null);

  // --- Audit logs ---
  const [logs, setLogs] = useState<AuditLogItem[]>([]);
  const [logTotal, setLogTotal] = useState(0);
  const [logOffset, setLogOffset] = useState(0);
  const [loadingLogs, setLoadingLogs] = useState(false);
  const [logError, setLogError] = useState("");

  // Load current user
  useEffect(() => {
    (async () => {
      try {
        const me = await fetchApiAuth<UserInfo>("/auth/me");
        setCurrentUser(me);
      } catch {
        // ignore — currentUser stays null
      }
    })();
  }, []);

  // Load users
  const loadUsers = useCallback(async () => {
    setLoadingUsers(true);
    setUserError("");
    try {
      const data = await fetchApiAuth<UserInfo[]>("/auth/users");
      setUsers(Array.isArray(data) ? data : []);
    } catch (e: unknown) {
      if (e instanceof Error && e.message === "AUTH_REQUIRED") {
        setAuthError(true);
      } else {
        setUserError(e instanceof Error ? e.message : "加载用户列表失败");
      }
    } finally {
      setLoadingUsers(false);
    }
  }, []);

  // Load audit logs
  const loadLogs = useCallback(async (offset = 0) => {
    setLoadingLogs(true);
    setLogError("");
    try {
      const qs = new URLSearchParams({
        limit: "50",
        offset: String(offset),
      }).toString();
      const data = await fetchApiAuth<{ items: AuditLogItem[]; total: number }>(
        `/auth/audit-logs?${qs}`
      );
      setLogs(data.items || []);
      setLogTotal(data.total || 0);
      setLogOffset(offset);
    } catch (e: unknown) {
      if (e instanceof Error && e.message === "AUTH_REQUIRED") {
        setLogError("请登录管理员账户后查看");
      } else {
        setLogError(e instanceof Error ? e.message : "加载操作日志失败");
      }
    } finally {
      setLoadingLogs(false);
    }
  }, []);

  useEffect(() => {
    loadUsers();
  }, [loadUsers]);

  useEffect(() => {
    if (activeTab === "audit") {
      loadLogs(0);
    }
  }, [activeTab, loadLogs]);

  // Create user
  const handleCreate = async () => {
    if (!newUsername || !newPassword) return;
    setCreating(true);
    setCreateError("");
    try {
      await fetchApiAuth<UserInfo>("/auth/register", {
        method: "POST",
        body: JSON.stringify({
          username: newUsername,
          password: newPassword,
          role: newRole,
          email: newEmail || null,
          display_name: newDisplayName || null,
        }),
      });
      setShowCreate(false);
      setNewUsername("");
      setNewPassword("");
      setNewRole("analyst");
      setNewEmail("");
      setNewDisplayName("");
      await loadUsers();
    } catch (e: unknown) {
      setCreateError(e instanceof Error ? e.message : "创建用户失败");
    } finally {
      setCreating(false);
    }
  };

  // Edit user
  const openEdit = (user: UserInfo) => {
    setEditUser(user);
    setEditRole(user.role);
    setEditDisplayName(user.display_name || "");
    setEditEmail(user.email || "");
    setEditError("");
  };

  const handleEdit = async () => {
    if (!editUser) return;
    setSaving(true);
    setEditError("");
    try {
      await fetchApiAuth<UserInfo>(`/auth/users/${editUser.id}`, {
        method: "PATCH",
        body: JSON.stringify({
          role: editRole !== editUser.role ? editRole : undefined,
          display_name: editDisplayName !== (editUser.display_name || "") ? editDisplayName : undefined,
          email: editEmail !== (editUser.email || "") ? editEmail : undefined,
        }),
      });
      setEditUser(null);
      await loadUsers();
    } catch (e: unknown) {
      setEditError(e instanceof Error ? e.message : "更新失败");
    } finally {
      setSaving(false);
    }
  };

  // Toggle active — with confirmation dialog for disabling
  const handleToggleActiveClick = (user: UserInfo) => {
    if (user.is_active) {
      // Disabling: show confirmation
      setConfirmDisableUser(user);
    } else {
      // Enabling: proceed directly
      doToggleActive(user);
    }
  };

  const doToggleActive = async (user: UserInfo) => {
    setConfirmDisableUser(null);
    try {
      await fetchApiAuth<UserInfo>(`/auth/users/${user.id}/toggle-active`, {
        method: "PUT",
        body: JSON.stringify({ is_active: !user.is_active }),
      });
      await loadUsers();
    } catch (e: unknown) {
      setUserError(e instanceof Error ? e.message : "操作失败");
    }
  };

  // Reset password
  const handleResetPassword = async () => {
    if (!resetUser || !resetPassword) return;
    setResetting(true);
    setResetError("");
    try {
      await fetchApiAuth(`/auth/users/${resetUser.id}/reset-password`, {
        method: "POST",
        body: JSON.stringify({ new_password: resetPassword }),
      });
      setResetUser(null);
      setResetPassword("");
    } catch (e: unknown) {
      setResetError(e instanceof Error ? e.message : "重置密码失败");
    } finally {
      setResetting(false);
    }
  };

  if (authError) {
    return (
      <div className="space-y-3">
        <PageHeader title="系统设置" description="用户管理与系统配置" />
        <div className="bg-card border border-border rounded p-8 text-center">
          <Shield className="h-8 w-8 mx-auto mb-3 text-muted-foreground opacity-40" />
          <p className="text-[13px] text-muted-foreground">请登录管理员账户后查看</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <PageHeader title="系统设置" description="用户管理与操作日志" />

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="users" className="text-[12px]">
            <Users className="h-3.5 w-3.5 mr-1" />
            用户管理
          </TabsTrigger>
          <TabsTrigger value="audit" className="text-[12px]">
            <ScrollText className="h-3.5 w-3.5 mr-1" />
            操作日志
          </TabsTrigger>
        </TabsList>

        {/* ===== Tab 1: Users ===== */}
        <TabsContent value="users">
          <div className="bg-card border border-border rounded">
            <div className="px-4 py-2 border-b border-border flex items-center justify-between">
              <span className="text-[13px] font-medium">用户列表</span>
              <div className="flex items-center gap-1">
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 px-2 text-[11px]"
                  onClick={() => loadUsers()}
                  disabled={loadingUsers}
                >
                  <RefreshCw className={`h-3 w-3 mr-1 ${loadingUsers ? "animate-spin" : ""}`} />
                  刷新
                </Button>
                <Button
                  size="sm"
                  className="h-7 text-[11px]"
                  onClick={() => {
                    setShowCreate(true);
                    setCreateError("");
                  }}
                >
                  <UserPlus className="h-3 w-3 mr-1" />
                  创建用户
                </Button>
              </div>
            </div>

            {userError && (
              <div className="px-4 pt-2">
                <p className="text-[11px] text-red-500">{userError}</p>
              </div>
            )}

            {loadingUsers ? (
              <div className="h-40 flex items-center justify-center">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="h-7 text-[11px] font-normal">用户名</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal">显示名</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center">角色</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center">状态</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center">最后登录</TableHead>
                    <TableHead className="h-7 text-[11px] font-normal text-center w-36">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {users.map((user) => {
                    const isSelf = currentUser?.id === user.id;
                    return (
                      <TableRow key={user.id} className="text-[12px]">
                        <TableCell className="py-1.5 font-medium">
                          {user.username}
                          {user.email && (
                            <span className="text-[10px] text-muted-foreground ml-1">
                              ({user.email})
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="py-1.5">
                          {user.display_name || "--"}
                        </TableCell>
                        <TableCell className="py-1.5 text-center">
                          <RoleBadge role={user.role} />
                        </TableCell>
                        <TableCell className="py-1.5 text-center">
                          {user.is_active ? (
                            <Badge className="text-[10px] bg-green-500/10 text-green-600 border-green-200 hover:bg-green-500/10">
                              <CheckCircle2 className="h-3 w-3 mr-0.5" />
                              正常
                            </Badge>
                          ) : (
                            <Badge className="text-[10px] bg-red-500/10 text-red-600 border-red-200 hover:bg-red-500/10">
                              <XCircle className="h-3 w-3 mr-0.5" />
                              已禁用
                            </Badge>
                          )}
                        </TableCell>
                        <TableCell className="py-1.5 text-center text-muted-foreground">
                          {formatDateTime(user.last_login_at)}
                        </TableCell>
                        <TableCell className="py-1.5 text-center">
                          <div className="flex items-center justify-center gap-1">
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-6 px-1.5 text-[11px]"
                              onClick={() => openEdit(user)}
                              title="编辑"
                            >
                              <Pencil className="h-3 w-3" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-6 px-1.5 text-[11px]"
                              onClick={() => {
                                setResetUser(user);
                                setResetPassword("");
                                setResetError("");
                              }}
                              title="重置密码"
                            >
                              <KeyRound className="h-3 w-3" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              className={`h-6 px-1.5 text-[11px] ${
                                user.is_active
                                  ? "text-red-500 hover:text-red-600"
                                  : "text-green-500 hover:text-green-600"
                              }`}
                              disabled={isSelf}
                              onClick={() => handleToggleActiveClick(user)}
                              title={isSelf ? "不能操作自己" : user.is_active ? "禁用" : "启用"}
                            >
                              {user.is_active ? (
                                <ShieldOff className="h-3 w-3" />
                              ) : (
                                <ShieldCheck className="h-3 w-3" />
                              )}
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </div>
        </TabsContent>

        {/* ===== Tab 2: Audit Logs ===== */}
        <TabsContent value="audit">
          <div className="bg-card border border-border rounded">
            <div className="px-4 py-2 border-b border-border flex items-center justify-between">
              <span className="text-[13px] font-medium">
                操作日志
                {logTotal > 0 && (
                  <span className="text-[11px] text-muted-foreground ml-2">
                    共 {logTotal} 条
                  </span>
                )}
              </span>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 px-2 text-[11px]"
                onClick={() => loadLogs(0)}
                disabled={loadingLogs}
              >
                <RefreshCw className={`h-3 w-3 mr-1 ${loadingLogs ? "animate-spin" : ""}`} />
                刷新
              </Button>
            </div>

            {logError ? (
              <div className="h-40 flex items-center justify-center">
                <div className="text-center space-y-2">
                  <AlertTriangle className="mx-auto h-6 w-6 text-amber-500 opacity-60" />
                  <p className="text-[12px] text-red-500">{logError}</p>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-6 px-3 text-[11px]"
                    onClick={() => loadLogs(0)}
                  >
                    重试
                  </Button>
                </div>
              </div>
            ) : loadingLogs ? (
              <div className="h-40 flex items-center justify-center">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : logs.length === 0 ? (
              <div className="h-40 flex items-center justify-center text-muted-foreground">
                <div className="text-center space-y-1">
                  <ScrollText className="mx-auto h-7 w-7 opacity-25" />
                  <p className="text-[12px] opacity-60">暂无操作日志</p>
                </div>
              </div>
            ) : (
              <>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="h-7 text-[11px] font-normal">时间</TableHead>
                      <TableHead className="h-7 text-[11px] font-normal">操作人</TableHead>
                      <TableHead className="h-7 text-[11px] font-normal">操作</TableHead>
                      <TableHead className="h-7 text-[11px] font-normal">详情</TableHead>
                      <TableHead className="h-7 text-[11px] font-normal text-right">IP</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {logs.map((log) => (
                      <TableRow key={log.id} className="text-[12px]">
                        <TableCell className="py-1.5 text-muted-foreground tabular-nums">
                          {formatDateTime(log.created_at)}
                        </TableCell>
                        <TableCell className="py-1.5 font-medium">
                          {log.username}
                        </TableCell>
                        <TableCell className="py-1.5">
                          <Badge variant="outline" className="text-[10px]">
                            {ACTION_LABELS[log.action] || log.action}
                          </Badge>
                        </TableCell>
                        <TableCell className="py-1.5 text-muted-foreground max-w-[300px] truncate">
                          {log.detail || "--"}
                        </TableCell>
                        <TableCell className="py-1.5 text-right text-muted-foreground font-mono text-[10px]">
                          {log.ip_address || "--"}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>

                {/* Pagination */}
                {logTotal > 50 && (
                  <div className="px-4 py-2 border-t border-border flex items-center justify-between">
                    <span className="text-[11px] text-muted-foreground">
                      {logOffset + 1}-{Math.min(logOffset + 50, logTotal)} / {logTotal}
                    </span>
                    <div className="flex gap-1">
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-6 px-2 text-[11px]"
                        disabled={logOffset === 0}
                        onClick={() => loadLogs(Math.max(0, logOffset - 50))}
                      >
                        上一页
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-6 px-2 text-[11px]"
                        disabled={logOffset + 50 >= logTotal}
                        onClick={() => loadLogs(logOffset + 50)}
                      >
                        下一页
                      </Button>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        </TabsContent>
      </Tabs>

      {/* ===== Confirm Disable Dialog ===== */}
      <Dialog open={!!confirmDisableUser} onOpenChange={(open) => !open && setConfirmDisableUser(null)}>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle className="text-[14px]">确认禁用用户</DialogTitle>
            <DialogDescription className="text-[12px]">
              确定要禁用用户 <strong>{confirmDisableUser?.username}</strong> 吗？禁用后该用户将无法登录。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-[12px]"
              onClick={() => setConfirmDisableUser(null)}
            >
              取消
            </Button>
            <Button
              variant="destructive"
              size="sm"
              className="h-7 text-[12px]"
              onClick={() => confirmDisableUser && doToggleActive(confirmDisableUser)}
            >
              确认禁用
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* ===== Create User Dialog ===== */}
      <Dialog open={showCreate} onOpenChange={(open) => !open && setShowCreate(false)}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="text-[14px]">创建新用户</DialogTitle>
            <DialogDescription className="text-[12px]">
              填写用户信息后点击创建。密码至少8位，需包含字母和数字。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">用户名 *</label>
              <Input
                className="h-8 text-[12px]"
                placeholder="2-50个字符"
                value={newUsername}
                onChange={(e) => setNewUsername(e.target.value)}
              />
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">密码 *</label>
              <Input
                type="password"
                className="h-8 text-[12px]"
                placeholder="至少8个字符，需含字母和数字"
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
              />
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">角色</label>
              <select
                className="h-8 w-full rounded border border-border bg-background px-2 text-[12px]"
                value={newRole}
                onChange={(e) => setNewRole(e.target.value)}
              >
                <option value="admin">管理员</option>
                <option value="analyst">分析师</option>
                <option value="viewer">观察者</option>
              </select>
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">显示名</label>
              <Input
                className="h-8 text-[12px]"
                placeholder="可选"
                value={newDisplayName}
                onChange={(e) => setNewDisplayName(e.target.value)}
              />
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">邮箱</label>
              <Input
                className="h-8 text-[12px]"
                placeholder="可选"
                value={newEmail}
                onChange={(e) => setNewEmail(e.target.value)}
              />
            </div>
            {newPassword.length > 0 && !isPasswordValid(newPassword) && (
              <p className="text-[10px] text-amber-500">密码需至少8位，且包含字母和数字</p>
            )}
            {createError && (
              <p className="text-[11px] text-red-500">{createError}</p>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-[12px]"
              onClick={() => setShowCreate(false)}
            >
              取消
            </Button>
            <Button
              size="sm"
              className="h-7 text-[12px]"
              onClick={handleCreate}
              disabled={creating || !newUsername || !isPasswordValid(newPassword)}
            >
              {creating && <Loader2 className="h-3 w-3 animate-spin mr-1" />}
              创建
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* ===== Edit User Dialog ===== */}
      <Dialog open={!!editUser} onOpenChange={(open) => !open && setEditUser(null)}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="text-[14px]">
              编辑用户 - {editUser?.username}
            </DialogTitle>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">角色</label>
              <select
                className="h-8 w-full rounded border border-border bg-background px-2 text-[12px]"
                value={editRole}
                onChange={(e) => setEditRole(e.target.value)}
              >
                <option value="admin">管理员</option>
                <option value="analyst">分析师</option>
                <option value="viewer">观察者</option>
              </select>
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">显示名</label>
              <Input
                className="h-8 text-[12px]"
                value={editDisplayName}
                onChange={(e) => setEditDisplayName(e.target.value)}
              />
            </div>
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">邮箱</label>
              <Input
                className="h-8 text-[12px]"
                value={editEmail}
                onChange={(e) => setEditEmail(e.target.value)}
              />
            </div>
            {editError && (
              <p className="text-[11px] text-red-500">{editError}</p>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-[12px]"
              onClick={() => setEditUser(null)}
            >
              取消
            </Button>
            <Button
              size="sm"
              className="h-7 text-[12px]"
              onClick={handleEdit}
              disabled={saving}
            >
              {saving && <Loader2 className="h-3 w-3 animate-spin mr-1" />}
              保存
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* ===== Reset Password Dialog ===== */}
      <Dialog open={!!resetUser} onOpenChange={(open) => !open && setResetUser(null)}>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle className="text-[14px]">
              重置密码 - {resetUser?.username}
            </DialogTitle>
            <DialogDescription className="text-[12px]">
              为该用户设置新密码，至少8个字符，需包含字母和数字。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">新密码</label>
              <Input
                type="password"
                className="h-8 text-[12px]"
                placeholder="至少8个字符，需含字母和数字"
                value={resetPassword}
                onChange={(e) => setResetPassword(e.target.value)}
              />
            </div>
            {resetPassword.length > 0 && !isPasswordValid(resetPassword) && (
              <p className="text-[10px] text-amber-500">密码需至少8位，且包含字母和数字</p>
            )}
            {resetError && (
              <p className="text-[11px] text-red-500">{resetError}</p>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              className="h-7 text-[12px]"
              onClick={() => setResetUser(null)}
            >
              取消
            </Button>
            <Button
              variant="destructive"
              size="sm"
              className="h-7 text-[12px]"
              onClick={handleResetPassword}
              disabled={resetting || !isPasswordValid(resetPassword)}
            >
              {resetting && <Loader2 className="h-3 w-3 animate-spin mr-1" />}
              确认重置
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
