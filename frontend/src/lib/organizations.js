export function normalizeOrganizationMembership(raw) {
  if (!raw || typeof raw !== 'object') {
    return null;
  }

  const organizationId = raw.organization_id ?? raw.org_id ?? null;
  const isLeader =
    raw.membership === 'leader' ||
    raw.is_leader === true ||
    raw.is_leader === 1;

  return {
    ...raw,
    organization_id: organizationId,
    org_id: organizationId,
    is_leader: isLeader,
    membership: isLeader ? 'leader' : 'member',
    role_id: raw.role_id ?? null,
  };
}

export function normalizeOrganizationMembershipList(rows) {
  if (!Array.isArray(rows)) {
    return [];
  }

  return rows
    .map(normalizeOrganizationMembership)
    .filter(Boolean);
}
