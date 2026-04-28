import { useCallback, useEffect, useState } from 'react';
import { Link, useLocation, useNavigate, useParams, useSearchParams } from 'react-router-dom';
import './UserDashboard.css';
import { readJson, submitAndAwaitV2Operation } from '../lib/api';
import { getStoredFirstName, getStoredUserId } from '../lib/auth';
import { normalizeOrganizationMembershipList } from '../lib/organizations';
import InlineActionPanel from './InlineActionPanel';
import { formatRoleOption } from '../lib/policyOptions';

function formatTokenUnits(value) {
  const n = Number(value || 0);
  return `${(n / 100).toFixed(2)} tokens`;
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
  const [portfolio, setPortfolio] = useState({ token_balances: [], open_tickets: [] });
  const [portfolioLoading, setPortfolioLoading] = useState(false);
  const [portfolioError, setPortfolioError] = useState(null);
  const [showCreateOrgPanel, setShowCreateOrgPanel] = useState(false);
  const [createOrgSubmitting, setCreateOrgSubmitting] = useState(false);
  const [createOrgForm, setCreateOrgForm] = useState({ name: '', description: '' });
  const [showJoinOrgPanel, setShowJoinOrgPanel] = useState(false);
  const [joinOrgSubmitting, setJoinOrgSubmitting] = useState(false);
  const [joinOrgLookupLoading, setJoinOrgLookupLoading] = useState(false);
  const [joinOrgLookupError, setJoinOrgLookupError] = useState(null);
  const [joinOrgOptions, setJoinOrgOptions] = useState(null);
  const [joinOrgForm, setJoinOrgForm] = useState({ organizationId: '', roleId: '' });
  const firstName = getStoredFirstName();
  const dashboardTitle = firstName ? `${firstName}'s dashboard` : 'Your dashboard';

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

  const loadPortfolio = useCallback(async () => {
    if (userId == null || Number.isNaN(userId)) return;
    setPortfolioLoading(true);
    setPortfolioError(null);
    try {
      const data = await readJson(`/dashboard/users/${userId}/portfolio`);
      setPortfolio({
        token_balances: Array.isArray(data?.token_balances) ? data.token_balances : [],
        open_tickets: Array.isArray(data?.open_tickets) ? data.open_tickets : [],
      });
    } catch (e) {
      setPortfolioError(e.message || 'Failed to load balances and tickets');
      setPortfolio({ token_balances: [], open_tickets: [] });
    } finally {
      setPortfolioLoading(false);
    }
  }, [userId]);

  useEffect(() => {
    loadPortfolio();
  }, [loadPortfolio]);

  useEffect(() => {
    const organizationId = Number(joinOrgForm.organizationId);
    if (!joinOrgForm.organizationId.trim() || Number.isNaN(organizationId)) {
      setJoinOrgOptions(null);
      setJoinOrgLookupError(null);
      return;
    }

    let cancelled = false;
    const loadJoinOptions = async () => {
      setJoinOrgLookupLoading(true);
      setJoinOrgLookupError(null);
      try {
        const data = await readJson(`/organizations/${organizationId}/join-options`);
        if (cancelled) return;
        setJoinOrgOptions(data);
        setJoinOrgForm((current) => ({
          ...current,
          roleId: current.roleId || String(data?.roles?.[0]?.role_id || ''),
        }));
      } catch (error) {
        if (cancelled) return;
        setJoinOrgOptions(null);
        setJoinOrgLookupError(error.message || 'Could not load organization roles');
      } finally {
        if (!cancelled) {
          setJoinOrgLookupLoading(false);
        }
      }
    };

    loadJoinOptions();
    return () => {
      cancelled = true;
    };
  }, [joinOrgForm.organizationId]);

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
    if (!createOrgForm.name.trim()) return;

    setCreateOrgSubmitting(true);
    try {
      await submitAndAwaitV2Operation('/org/management', {
        action: 'CREATE_ORGANIZATION',
        user_id: Number(userId),
        name: createOrgForm.name.trim(),
        description: createOrgForm.description.trim(),
      });
      setCreateOrgForm({ name: '', description: '' });
      setShowCreateOrgPanel(false);
      await loadOrgs();
    } catch (e) {
      setOrgsError(e.message || 'Failed to create organization');
    } finally {
      setCreateOrgSubmitting(false);
    }
  };

  const joinOrganization = async () => {
    if (userId == null || Number.isNaN(userId)) {
      navigate('/signin');
      return;
    }

    const organizationId = Number(joinOrgForm.organizationId);
    if (Number.isNaN(organizationId) || !joinOrgForm.roleId) return;

    setJoinOrgSubmitting(true);
    setOrgsError(null);
    try {
      await submitAndAwaitV2Operation('/org/management', {
        action: 'JOIN_ORGANIZATION',
        user_id: Number(userId),
        organization_id: organizationId,
        role_id: joinOrgForm.roleId,
      });
      setJoinOrgForm({ organizationId: '', roleId: '' });
      setJoinOrgOptions(null);
      setShowJoinOrgPanel(false);
      await loadOrgs();
      selectOrganization(organizationId);
    } catch (error) {
      setOrgsError(error.message || 'Failed to join organization');
    } finally {
      setJoinOrgSubmitting(false);
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
          <h1>{dashboardTitle}</h1>
          <p>
            Open an organization to see events you can bet on (open events that match your access).
          </p>
        </header>

        <div className="user-dashboard-grid">
          <article className="user-dashboard-card">
            <h2>Your organizations</h2>
            <div className="user-dashboard-actions">
              <button
                type="button"
                className="ui-action-button ui-action-button--primary"
                onClick={() => {
                  setOrgsError(null);
                  setShowJoinOrgPanel(false);
                  setShowCreateOrgPanel((current) => !current);
                }}
              >
                Create organization
              </button>
              {' '}
              <button
                type="button"
                className="ui-action-button ui-action-button--secondary"
                onClick={() => {
                  setOrgsError(null);
                  setShowCreateOrgPanel(false);
                  setJoinOrgLookupError(null);
                  setShowJoinOrgPanel((current) => !current);
                }}
              >
                Join organization
              </button>
            </div>
            {showCreateOrgPanel && (
              <InlineActionPanel
                title="Create organization"
                description="Set up a new org without leaving your dashboard."
                onSubmit={(event) => {
                  event.preventDefault();
                  createOrganization();
                }}
                onCancel={() => {
                  setShowCreateOrgPanel(false);
                  setCreateOrgForm({ name: '', description: '' });
                }}
                submitLabel={createOrgSubmitting ? 'Creating...' : 'Create organization'}
                submitDisabled={createOrgSubmitting || !createOrgForm.name.trim()}
              >
                <label>
                  Organization name
                  <input
                    type="text"
                    value={createOrgForm.name}
                    onChange={(event) =>
                      setCreateOrgForm((current) => ({ ...current, name: event.target.value }))
                    }
                    placeholder="North Dakota Forecasting Club"
                  />
                </label>
                <label data-span="full">
                  Description
                  <textarea
                    value={createOrgForm.description}
                    onChange={(event) =>
                      setCreateOrgForm((current) => ({ ...current, description: event.target.value }))
                    }
                    placeholder="What this organization is for and how members should use it."
                  />
                </label>
              </InlineActionPanel>
            )}
            {showJoinOrgPanel && (
              <InlineActionPanel
                title="Join organization"
                description="Enter an organization id, review the available roles, and join directly."
                onSubmit={(event) => {
                  event.preventDefault();
                  joinOrganization();
                }}
                onCancel={() => {
                  setShowJoinOrgPanel(false);
                  setJoinOrgForm({ organizationId: '', roleId: '' });
                  setJoinOrgOptions(null);
                  setJoinOrgLookupError(null);
                }}
                submitLabel={joinOrgSubmitting ? 'Joining...' : 'Join organization'}
                submitDisabled={
                  joinOrgSubmitting ||
                  !joinOrgForm.organizationId.trim() ||
                  !joinOrgForm.roleId
                }
              >
                <label>
                  Organization id
                  <input
                    type="number"
                    min="1"
                    step="1"
                    value={joinOrgForm.organizationId}
                    onChange={(event) =>
                      setJoinOrgForm((current) => ({
                        ...current,
                        organizationId: event.target.value,
                        roleId: '',
                      }))
                    }
                    placeholder="12"
                  />
                </label>
                <label>
                  Role
                  <select
                    value={joinOrgForm.roleId}
                    onChange={(event) =>
                      setJoinOrgForm((current) => ({ ...current, roleId: event.target.value }))
                    }
                    disabled={!joinOrgOptions || joinOrgLookupLoading}
                  >
                    <option value="" disabled>
                      {joinOrgLookupLoading ? 'Loading roles...' : 'Select a role'}
                    </option>
                    {(joinOrgOptions?.roles || []).map((role) => (
                      <option key={role.role_id} value={role.role_id}>
                        {formatRoleOption(role)}
                      </option>
                    ))}
                  </select>
                </label>
                {joinOrgOptions && (
                  <label data-span="full">
                    Organization
                    <input
                      type="text"
                      value={`${joinOrgOptions.name}${joinOrgOptions.description ? ` - ${joinOrgOptions.description}` : ''}`}
                      readOnly
                    />
                  </label>
                )}
                {joinOrgLookupError && (
                  <label data-span="full">
                    Lookup
                    <input type="text" value={joinOrgLookupError} readOnly />
                  </label>
                )}
              </InlineActionPanel>
            )}
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

          <article className="user-dashboard-card">
            <h2>Your token balances</h2>
            {portfolioLoading && <p className="user-dashboard-muted">Loading…</p>}
            {portfolioError && <p className="user-dashboard-error">{portfolioError}</p>}
            {!portfolioLoading && !portfolioError && portfolio.token_balances.length === 0 && (
              <p className="user-dashboard-muted">No token balances yet.</p>
            )}
            <ul className="user-dashboard-event-list">
              {portfolio.token_balances.map((balance) => (
                <li key={`${balance.organization_id}-${balance.token_id}`}>
                  <div>
                    <strong>{balance.token_name}</strong>
                    <span className="user-dashboard-muted">{balance.organization_name}</span>
                  </div>
                  <span className="user-dashboard-badge user-dashboard-badge--neutral">
                    {formatTokenUnits(balance.qty)}
                  </span>
                </li>
              ))}
            </ul>
          </article>

          <article className="user-dashboard-card user-dashboard-card--wide">
            <h2>Your open tickets</h2>
            {portfolioLoading && <p className="user-dashboard-muted">Loading…</p>}
            {portfolioError && <p className="user-dashboard-error">{portfolioError}</p>}
            {!portfolioLoading && !portfolioError && portfolio.open_tickets.length === 0 && (
              <p className="user-dashboard-muted">You do not have any open tickets yet.</p>
            )}
            <ul className="user-dashboard-event-list">
              {portfolio.open_tickets.map((ticket) => (
                <li key={`${ticket.market_id}-${ticket.side ? 'yes' : 'no'}`}>
                  <div className="user-dashboard-ticket">
                    <strong>{ticket.question}</strong>
                    <span className="user-dashboard-ticket__group">
                      {ticket.organization_name} · {ticket.event_caption}
                    </span>
                    <span className="user-dashboard-ticket__meta">
                      <span
                        className={
                          ticket.side
                            ? 'user-dashboard-ticket__pill user-dashboard-ticket__pill--yes'
                            : 'user-dashboard-ticket__pill user-dashboard-ticket__pill--no'
                        }
                      >
                        {ticket.side ? 'YES' : 'NO'} side
                      </span>
                      <span className="user-dashboard-ticket__pill user-dashboard-ticket__pill--status">
                        {ticket.is_open ? 'Open market' : 'Resolved market'}
                      </span>
                    </span>
                  </div>
                  <span className="user-dashboard-badge">
                    {ticket.qty} ticket{ticket.qty === 1 ? '' : 's'}
                  </span>
                </li>
              ))}
            </ul>
          </article>
        </div>
      </div>
    </section>
  );
}
