const DEFAULT_API_ROOT = 'https://d2vrkasldxh3jt.cloudfront.net';

export const API_ROOT = (
  import.meta.env.VITE_API_ROOT_URL ||
  import.meta.env.VITE_API_BASE_URL ||
  DEFAULT_API_ROOT
).replace(/\/$/, '');

export const READ_API_BASE = API_ROOT;
export const WRITE_API_BASE = `${API_ROOT}/v2`;
export const API_BASE = READ_API_BASE;

export function formatApiError(payload, fallback = 'Request failed.') {
  const detail = payload?.detail;

  if (typeof detail === 'string' && detail.trim()) {
    return detail;
  }

  if (Array.isArray(detail) && detail.length > 0) {
    return detail.map((item) => item?.msg || JSON.stringify(item)).join('; ');
  }

  if (typeof payload?.message === 'string' && payload.message.trim()) {
    return payload.message;
  }

  if (typeof payload?.error_message === 'string' && payload.error_message.trim()) {
    return payload.error_message;
  }

  return fallback;
}

export async function readJson(path, options = {}) {
  const response = await fetch(`${READ_API_BASE}${path}`, options);
  const payload = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(formatApiError(payload, `HTTP ${response.status}`));
  }

  return payload;
}

export async function postJson(path, body, options = {}) {
  const response = await fetch(`${READ_API_BASE}${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
    body: JSON.stringify(body),
  });

  const payload = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(formatApiError(payload, `HTTP ${response.status}`));
  }

  return payload;
}

export async function putJson(path, body, options = {}) {
  const response = await fetch(`${READ_API_BASE}${path}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
    body: JSON.stringify(body),
  });

  const payload = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(formatApiError(payload, `HTTP ${response.status}`));
  }

  return payload;
}

export async function submitV2Operation(path, body, options = {}) {
  const response = await fetch(`${WRITE_API_BASE}${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
    body: JSON.stringify(body),
  });

  const payload = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(formatApiError(payload, `HTTP ${response.status}`));
  }

  return payload;
}

export async function pollOperation(operationId, options = {}) {
  const {
    intervalMs = 1200,
    timeoutMs = 15000,
    headers,
  } = options;
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const payload = await readJson(`/v2/operations/${operationId}`, {
      headers,
    });

    if (payload.status === 'succeeded') {
      return payload;
    }

    if (payload.status === 'failed' || payload.status === 'dead') {
      throw new Error(formatApiError(payload, 'The operation failed.'));
    }

    await new Promise((resolve) => window.setTimeout(resolve, intervalMs));
  }

  throw new Error('The operation is still processing. Please try again in a moment.');
}
