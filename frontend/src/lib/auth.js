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

export function getStoredSessionToken() {
  const auth = loadAuth();
  return auth?.sessionToken || null;
}

export function getStoredAccessToken() {
  const auth = loadAuth();
  return auth?.accessToken || auth?.sessionToken || null;
}

export function getStoredFirstName() {
  const auth = loadAuth();
  return auth?.firstName || null;
}
