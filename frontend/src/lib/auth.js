const STORAGE_KEY = 'polaris-auth';

export function loadAuth() {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

export function saveAuth(auth) {
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(auth));
}

export function clearAuth() {
  window.localStorage.removeItem(STORAGE_KEY);
}

export function getStoredUserId() {
  const auth = loadAuth();
  const n = Number(auth?.userId);
  return Number.isNaN(n) ? null : n;
}
