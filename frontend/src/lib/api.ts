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

/** Fetch with auth token from localStorage */
export async function fetchApiAuth<T>(
  path: string,
  options?: RequestInit & { timeoutMs?: number }
): Promise<T> {
  const token = typeof window !== "undefined" ? localStorage.getItem("auth_token") : null;
  if (!token) throw new Error("AUTH_REQUIRED");
  const { headers, ...rest } = options ?? {};
  return fetchApi<T>(path, {
    ...rest,
    headers: {
      ...headers,
      Authorization: `Bearer ${token}`,
    },
  });
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
