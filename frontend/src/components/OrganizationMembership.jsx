import { useEffect, useState } from 'react';
import { readJson } from '../lib/api';
import { normalizeOrganizationMembershipList } from '../lib/organizations';

/**
 * Shows whether the current user is an organization leader or a member (from dashboard API).
 */
export default function OrganizationMembership({ organizationId, userId }) {
  const [phase, setPhase] = useState('loading'); // loading | leader | member | not_in_org | error
  const [roleId, setRoleId] = useState(null);

  useEffect(() => {
    if (!organizationId || !userId) return;

    let cancelled = false;

    const run = async () => {
      setPhase('loading');
      setRoleId(null);
      try {
        const orgs = normalizeOrganizationMembershipList(
          await readJson(`/dashboard/users/${userId}/organizations`)
        );
        if (!Array.isArray(orgs)) {
          if (!cancelled) setPhase('error');
          return;
        }
        const current = orgs.find((o) => String(o.organization_id) === String(organizationId));
        if (cancelled) return;
        if (!current) {
          setPhase('not_in_org');
          return;
        }
        if (current.membership === 'leader') {
          setPhase('leader');
        } else {
          setPhase('member');
          setRoleId(current.role_id ?? null);
        }
      } catch {
        if (!cancelled) setPhase('error');
      }
    };

    run();
    return () => {
      cancelled = true;
    };
  }, [organizationId, userId]);

  if (phase === 'loading') {
    return (
      <div className="organization-membership organization-membership--loading" role="status">
        <span className="organization-membership__label">Your role</span>
        <p className="organization-membership__value">Loading…</p>
      </div>
    );
  }

  if (phase === 'error') {
    return (
      <div className="organization-membership organization-membership--error" role="alert">
        <span className="organization-membership__label">Your role</span>
        <p className="organization-membership__value">Could not load your membership.</p>
      </div>
    );
  }

  if (phase === 'not_in_org') {
    return (
      <div className="organization-membership organization-membership--guest" role="status">
        <span className="organization-membership__label">Your role</span>
        <p className="organization-membership__value">
          You are not listed as a leader or member of this organization.
        </p>
      </div>
    );
  }

  if (phase === 'leader') {
    return (
      <div className="organization-membership organization-membership--leader">
        <span className="organization-membership__label">Your role</span>
        <p className="organization-membership__value">
          <strong>Organization leader</strong>
          <span className="organization-membership__hint">You manage this organization.</span>
        </p>
      </div>
    );
  }

  return (
    <div className="organization-membership organization-membership--member">
      <span className="organization-membership__label">Your role</span>
      <p className="organization-membership__value">
        <strong>Member</strong>
        {roleId ? (
          <span className="organization-membership__hint">Role id: {roleId}</span>
        ) : (
          <span className="organization-membership__hint">Member of this organization.</span>
        )}
      </p>
    </div>
  );
}
