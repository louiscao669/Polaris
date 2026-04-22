import { useCallback, useEffect, useState } from 'react';
import { Link, useLocation, useNavigate, useParams, useSearchParams } from 'react-router-dom';
import './UserDashboard.css';
import { API_BASE, pollOperation, readJson, submitV2Operation } from '../lib/api';
import { getStoredUserId } from '../lib/auth';
import { normalizeOrganizationMembershipList } from '../lib/organizations';

function formatApiError(err) {
  const d = err?.detail;
  if (d == null) return null;
  if (typeof d === 'string') return d;
  if (Array.isArray(d)) {
    return d.map((x) => (typeof x === 'object' && x?.msg ? x.msg : String(x))).join('; ');
  }
  return String(d);
}

export default function UserDashboard() {
  const navigate = useNavigate();
  const location = useLocation();
  const { organizationId: orgIdFromPath } = useParams();
  const [searchParams] = useSearchParams();
  const paramId = searchParams.get('userId');

  const [userId, setUserId] = useState(() => {
    if (paramId) {
      const n = Number(paramId);
      return Number.isNaN(n) ? null : n;
    }
    const fromLogin = location.state?.userId;
    if (fromLogin != null) {
      const n = Number(fromLogin);
      return Number.isNaN(n) ? null : n;
    }
    return getStoredUserId();
  });

  const [orgs, setOrgs] = useState([]);
  const [orgsLoading, setOrgsLoading] = useState(false);
  const [orgsError, setOrgsError] = useState(null);

  const [selectedOrgId, setSelectedOrgId] = useState(null);
  const [events, setEvents] = useState([]);
  const [eventsLoading, setEventsLoading] = useState(false);
  const [eventsError, setEventsError] = useState(null);

  useEffect(() => {
    if (paramId) {
      const n = Number(paramId);
      if (!Number.isNaN(n)) setUserId(n);
    }
  }, [paramId]);

  useEffect(() => {
    const id = location.state?.userId;
    if (id != null) {
      const n = Number(id);
      if (!Number.isNaN(n)) setUserId(n);
    }
  }, [location.state]);

  const loadOrgs = useCallback(async () => {
    if (userId == null || Number.isNaN(userId)) return;
    setOrgsLoading(true);
    setOrgsError(null);
    try {
      const data = await readJson(`/dashboard/users/${userId}/organizations`);
      setOrgs(normalizeOrganizationMembershipList(data));
    } catch (e) {
      setOrgsError(e.message || 'Failed to load organizations');
      setOrgs([]);
    } finally {
      setOrgsLoading(false);
    }
  }, [userId]);

  useEffect(() => {
    loadOrgs();
  }, [loadOrgs]);

  const loadEvents = useCallback(
    async (orgId) => {
      if (userId == null || Number.isNaN(userId)) return;
      setSelectedOrgId(orgId);
      setEventsLoading(true);
      setEventsError(null);
      try {
        const q = new URLSearchParams({ user_id: String(userId) });
        const data = await readJson(`/organizations/${orgId}/events?${q.toString()}`);
        setEvents(Array.isArray(data) ? data : []);
      } catch (e) {
        setEventsError(e.message || 'Failed to load events');
        setEvents([]);
      } finally {
        setEventsLoading(false);
      }
    },
    [userId]
  );

  // Deep link: /dashboard/:organizationId — load that org's events once we know the user
  useEffect(() => {
    if (userId == null || Number.isNaN(userId) || orgIdFromPath == null || orgIdFromPath === '') {
      return;
    }
    const oid = Number(orgIdFromPath);
    if (Number.isNaN(oid)) return;
    loadEvents(oid);
  }, [orgIdFromPath, userId, loadEvents]);

  const selectOrganization = (orgId) => {
    navigate(`/organization/${orgId}${userId ? `?userId=${userId}` : ''}`);
  };

  const createOrganization = async () => {
    if (userId == null || Number.isNaN(userId)) {
      navigate('/signin');
      return;
    }

    const name = prompt('Enter the organization name');
    if (!name) return;

    const description = prompt('Enter a short organization description') || '';

    try {
      const op = await submitV2Operation('/org/management', {
        action: 'CREATE_ORGANIZATION',
        user_id: Number(userId),
        name,
        description,
      });
      await pollOperation(op.operation_id, {
        headers: { 'X-Force-Leader': 'true' },
      });
      await loadOrgs();
    } catch (e) {
      setOrgsError(e.message || 'Failed to create organization');
    }
  };

  if (userId == null || Number.isNaN(userId)) {
    return (
      <section className="user-dashboard" aria-label="User dashboard">
        <div className="user-dashboard-shell">
          <h1>Your dashboard</h1>
          <p className="user-dashboard-hint">
            Sign in to load your dashboard, or open <code>/dashboard?userId=1</code> for testing.
          </p>
          <p>
            <Link to="/signin">Sign in</Link>
            {' · '}
            <Link to="/signup">Sign up</Link>
          </p>
        </div>
      </section>
    );
  }

  return (
    <section className="user-dashboard" aria-label="User dashboard">
      <div className="user-dashboard-shell">
        <header className="user-dashboard-header">
          <p className="user-dashboard-kicker">Member</p>
          <h1>Your dashboard</h1>
          <p>
            Open an organization to see events you can bet on (open events that match your access).
          </p>
        </header>

        <div className="user-dashboard-grid">
          <article className="user-dashboard-card">
            <h2>Your organizations</h2>
            <p className="user-dashboard-muted">
              <button type="button" className="user-dashboard-org-btn" onClick={createOrganization}>
                Create organization
              </button>
            </p>
            {orgsLoading && <p className="user-dashboard-muted">Loading…</p>}
            {orgsError && <p className="user-dashboard-error">{orgsError}</p>}
            {!orgsLoading && !orgsError && orgs.length === 0 && (
              <p className="user-dashboard-muted">
                You are not listed as a leader or member of any organization yet.
              </p>
            )}
            <ul className="user-dashboard-org-list">
              {orgs.map((o) => (
                <li key={o.organization_id}>
                  <button
                    type="button"
                    className={
                      selectedOrgId === o.organization_id
                        ? 'user-dashboard-org-btn is-active'
                        : 'user-dashboard-org-btn'
                    }
                    onClick={() => selectOrganization(o.organization_id)}
                  >
                    <span className="user-dashboard-org-name">{o.name}</span>
                    <span className="user-dashboard-org-meta">
                      {o.membership === 'leader' ? 'Leader' : `Role: ${o.role_id || 'member'}`}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          </article>

          <article className="user-dashboard-card user-dashboard-card--wide">
            <h2>Events you can bet on</h2>
            {!selectedOrgId && (
              <p className="user-dashboard-muted">Select an organization to list events.</p>
            )}
            {selectedOrgId && eventsLoading && <p className="user-dashboard-muted">Loading…</p>}
            {selectedOrgId && eventsError && (
              <p className="user-dashboard-error">{eventsError}</p>
            )}
            {selectedOrgId && !eventsLoading && !eventsError && events.length === 0 && (
              <p className="user-dashboard-muted">No open events available for you in this org.</p>
            )}
            <ul className="user-dashboard-event-list">
              {events.map((ev) => (
                <li key={ev.event_id}>
                  <div>
                    <strong>{ev.caption}</strong>
                    <span className="user-dashboard-muted">Event #{ev.event_id}</span>
                  </div>
                  <span className="user-dashboard-badge">Open</span>
                </li>
              ))}
            </ul>
          </article>
        </div>
      </div>
    </section>
  );
}
