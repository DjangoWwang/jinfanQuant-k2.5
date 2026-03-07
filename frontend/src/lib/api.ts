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
