const API_BASE =
  process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

const DEFAULT_TIMEOUT_MS = 30000;

export async function fetchApi<T>(
  path: string,
  options?: RequestInit & { timeoutMs?: number }
): Promise<T> {
  const { timeoutMs = DEFAULT_TIMEOUT_MS, ...fetchOptions } = options ?? {};
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(`${API_BASE}${path}`, {
      headers: {
        ...(fetchOptions?.body instanceof FormData ? {} : { "Content-Type": "application/json" }),
        ...fetchOptions?.headers,
      },
      signal: controller.signal,
      ...fetchOptions,
    });
    if (!res.ok) {
      let msg = `API error: ${res.status}`;
      try {
        const body = await res.json();
        if (body.detail) msg = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
      } catch { /* ignore parse error */ }
      throw new Error(msg);
    }
    if (res.status === 204) return undefined as T;
    return res.json();
  } finally {
    clearTimeout(timer);
  }
}

/** Single source of truth for auth token */
function getAuthToken(): string {
  if (typeof window === "undefined") throw new Error("AUTH_REQUIRED");
  const token = localStorage.getItem("auth_token");
  if (!token) throw new Error("AUTH_REQUIRED");
  return token;
}

function withAuthHeaders(headers?: HeadersInit): HeadersInit {
  return { ...headers, Authorization: `Bearer ${getAuthToken()}` };
}

/** Fetch with auth token from localStorage */
export async function fetchApiAuth<T>(
  path: string,
  options?: RequestInit & { timeoutMs?: number }
): Promise<T> {
  const { headers, ...rest } = options ?? {};
  return fetchApi<T>(path, {
    ...rest,
    headers: withAuthHeaders(headers),
  });
}

/** Fetch with auth, returning a Blob (for file downloads) */
export async function fetchApiBlobAuth(
  path: string,
  options?: RequestInit & { timeoutMs?: number }
): Promise<Blob> {
  const { timeoutMs = DEFAULT_TIMEOUT_MS, headers, ...rest } = options ?? {};
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      ...rest,
      headers: withAuthHeaders(headers),
      signal: controller.signal,
    });
    if (!res.ok) {
      let msg = `API error: ${res.status}`;
      try {
        const body = await res.json();
        if (body.detail) msg = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
      } catch { /* ignore parse error */ }
      throw new Error(msg);
    }
    return res.blob();
  } finally {
    clearTimeout(timer);
  }
}

/* --- Alert types --- */

export interface AlertEvent {
  id: number;
  rule_id: number;
  target_type: string;
  target_id: number;
  target_name: string;
  metric_value: number;
  threshold_value: number;
  severity: string;
  message: string;
  is_read: boolean;
  created_at: string;
}

export interface AlertDashboard {
  unread_total: number;
  alerts_by_severity: Record<string, number>;
  active_rules: number;
  top_risks: AlertEvent[];
  recent_events: AlertEvent[];
}

export interface RiskRule {
  id: number;
  name: string;
  rule_type: string;
  target_type: string;
  target_id: number | null;
  threshold: number;
  comparison: string;
  severity: string;
  is_active: boolean;
}
