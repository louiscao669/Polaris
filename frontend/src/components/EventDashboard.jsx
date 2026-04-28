import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { useEffect, useState } from 'react';
import './EventDashboard.css';
import InlineActionPanel from './InlineActionPanel';
import { readJson, submitAndAwaitV2Operation } from '../lib/api';
import { getStoredUserId } from '../lib/auth';
import { normalizeOrganizationMembershipList } from '../lib/organizations';
import { formatConstraintOption, formatRoleOption, readPolicyOptions } from '../lib/policyOptions';

const bettorView = [
  'Active positions and unrealized P/L',
  'Open markets you can trade right now',
  'Recent executions and pending orders',
];

const viewerView = [
  'Live odds movement and volume trend',
  'Organization consensus by market',
  'Read-only event timeline and outcomes',
];

const analyzerView = [
  'Compare market forecast vs actual outcome',
  'Detect whale concentration and sentiment shifts',
  'Export event and trading metrics for reports',
];

function formatMemberLabel(member) {
  const fullName = [member?.first, member?.last].filter(Boolean).join(' ').trim();
  if (fullName && member?.username) return `${fullName} (@${member.username})`;
  if (fullName) return fullName;
  if (member?.username) return `@${member.username}`;
  return `User #${member?.user_id ?? ''}`;
}

export default function EventDashboard() {
  const navigate = useNavigate();
  const { organizationId, eventId } = useParams();
  const [searchParams] = useSearchParams();
  const userId = searchParams.get('userId') || getStoredUserId();
  const [eventData, setEventData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [markets, setMarkets] = useState([]);
  const [marketsLoading, setMarketsLoading] = useState(false);
  const [roleView, setRoleView] = useState('viewer');
  const [showAnalytics, setShowAnalytics] = useState(false);
  const [analyticsLoading, setAnalyticsLoading] = useState(false);
  const [analyticsError, setAnalyticsError] = useState(null);
  const [marketAnalytics, setMarketAnalytics] = useState([]);
  const [marketActionError, setMarketActionError] = useState(null);
  const [adminError, setAdminError] = useState(null);
  const [organizationData, setOrganizationData] = useState(null);
  const [activeAdminPanel, setActiveAdminPanel] = useState(null);
  const [createMarketForm, setCreateMarketForm] = useState({ question: '', description: '' });
  const [editEventForm, setEditEventForm] = useState({ caption: '' });
  const [allowRoleForm, setAllowRoleForm] = useState({ roleId: '' });
  const [eventTokenId, setEventTokenId] = useState('');
  const [marketCreatorId, setMarketCreatorId] = useState('');
  const [eventRuleForm, setEventRuleForm] = useState({ constraintId: '', value: '' });
  const [policyOptions, setPolicyOptions] = useState({ constraints: [], market_access: [] });
  const numericUserId = Number(userId);

  const loadEvent = async () => {
    if (!eventId || !userId) return;
    setLoading(true);
    try {
      const data = await readJson(`/events/${eventId}?user_id=${encodeURIComponent(userId)}`);
      setEventData(data);
    } catch (e) {
      console.error(e);
      setEventData(null);
    } finally {
      setLoading(false);
    }
  };

  const loadMarkets = async () => {
    if (!eventId || !userId) return;
    setMarketsLoading(true);
    try {
      const rows = await readJson(`/events/${eventId}/markets?user_id=${encodeURIComponent(userId)}`);
      setMarkets(Array.isArray(rows) ? rows : []);
    } catch (e) {
      console.error(e);
      setMarkets([]);
    } finally {
      setMarketsLoading(false);
    }
  };

  useEffect(() => {
    loadEvent();
  }, [eventId, userId]);

  useEffect(() => {
    if (!organizationId || !userId) {
      setOrganizationData(null);
      return;
    }
    let cancelled = false;
    const loadOrganization = async () => {
      try {
        const data = await readJson(
          `/organizations/${organizationId}?user_id=${encodeURIComponent(userId)}`
        );
        if (!cancelled) {
          setOrganizationData(data);
        }
      } catch (error) {
        console.error(error);
        if (!cancelled) {
          setOrganizationData(null);
        }
      }
    };
    loadOrganization();
    return () => {
      cancelled = true;
    };
  }, [organizationId, userId]);

  useEffect(() => {
    loadMarkets();
  }, [eventId, userId]);

  useEffect(() => {
    let cancelled = false;
    const loadPolicyOptions = async () => {
      try {
        const data = await readPolicyOptions();
        if (!cancelled) {
          setPolicyOptions({
            constraints: Array.isArray(data?.constraints) ? data.constraints : [],
            market_access: Array.isArray(data?.market_access) ? data.market_access : [],
          });
        }
      } catch (error) {
        console.error(error);
        if (!cancelled) {
          setPolicyOptions({ constraints: [], market_access: [] });
        }
      }
    };
    loadPolicyOptions();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const loadAnalytics = async () => {
      if (!showAnalytics || roleView !== 'analyzer' || !Number.isFinite(numericUserId) || markets.length === 0) {
        setMarketAnalytics([]);
        setAnalyticsError(null);
        return;
      }

      setAnalyticsLoading(true);
      setAnalyticsError(null);
      try {
        const validMarkets = markets.filter((market) => Number.isFinite(Number(market?.market_id)));
        const analyticsRows = await Promise.all(
          validMarkets.map(async (market) => {
            const marketId = Number(market.market_id);
            const q = `user_id=${encodeURIComponent(String(numericUserId))}&market_id=${encodeURIComponent(
              String(marketId)
            )}`;
            const [liquidity, whales, tradeDistribution, windowComparison] = await Promise.all([
              readJson(`/markets/stats/liquidity?${q}`),
              readJson(`/markets/stats/whales?${q}`),
              readJson(`/markets/stats/trade-distribution?${q}`),
              readJson(`/markets/stats/window-comparison?${q}&hours=24`),
            ]);

            return {
              marketId,
              question: market.question,
              liquidity,
              whales,
              tradeDistribution,
              windowComparison,
            };
          })
        );
        setMarketAnalytics(analyticsRows);
      } catch (e) {
        console.error(e);
        setMarketAnalytics([]);
        setAnalyticsError(e.message || 'Failed to load market analytics');
      } finally {
        setAnalyticsLoading(false);
      }
    };
    loadAnalytics();
  }, [showAnalytics, roleView, numericUserId, markets]);

  useEffect(() => {
    const loadPermissionView = async () => {
      if (!organizationId || !userId) {
        setRoleView('viewer');
        return;
      }
      try {
        const orgs = normalizeOrganizationMembershipList(
          await readJson(`/dashboard/users/${userId}/organizations`)
        );
        if (!Array.isArray(orgs)) {
          setRoleView('viewer');
          return;
        }
        const current = orgs.find((o) => String(o.organization_id) === String(organizationId));
        if (!current) {
          setRoleView('viewer');
          return;
        }
        if (current.membership === 'leader') {
          // Leaders get analyzer-style controls by default.
          setRoleView('analyzer');
          return;
        }
        const normalizedRole = String(current.role_id || '').toLowerCase();
        if (
          normalizedRole.includes('stat') ||
          normalizedRole.includes('analyst') ||
          normalizedRole.includes('analytics')
        ) {
          setRoleView('analyzer');
          return;
        }
        if (current.membership === 'member') {
          setRoleView('bettor');
          return;
        }
        setRoleView('viewer');
      } catch (e) {
        console.error(e);
        setRoleView('viewer');
      }
    };
    loadPermissionView();
  }, [organizationId, userId]);

  const roleSections = {
    bettor: {
      title: 'Bettor View',
      items: bettorView,
    },
    viewer: {
      title: 'Viewer View',
      items: viewerView,
    },
    analyzer: {
      title: 'Analyzer',
      items: analyzerView,
    },
  };
  const activeRoleSection = roleSections[roleView] || roleSections.viewer;
  const tokensAllowed = Array.isArray(eventData?.tokens_allowed) ? eventData.tokens_allowed : [];
  const canCreateMarket = roleView === 'analyzer' && !!eventId && !!userId;
  const canManageEvent = !!eventData?.is_leader && !!eventId && !!userId;
  const organizationTokens = Array.isArray(organizationData?.tokens) ? organizationData.tokens : [];
  const organizationMembers = Array.isArray(organizationData?.members) ? organizationData.members : [];
  const organizationRoles = Array.isArray(organizationData?.roles) ? organizationData.roles : [];
  const availableConstraints = Array.isArray(policyOptions?.constraints) ? policyOptions.constraints : [];
  const tokenNameById = Object.fromEntries(
    organizationTokens.map((token) => [String(token.token_id), token.name])
  );

  const openAdminPanel = (panel) => {
    setAdminError(null);
    setActiveAdminPanel(panel);
  };

  const closeAdminPanel = () => {
    setActiveAdminPanel(null);
  };

  const handleCreateMarket = async () => {
    const question = createMarketForm.question.trim();
    if (!question || !eventId || !userId) return;

    const description = createMarketForm.description.trim() || `${question} market for event ${eventId}`;

    setMarketActionError(null);

    try {
      await submitAndAwaitV2Operation('/markets/lifecycle', {
        action: 'CREATE_MARKET',
        user_id: Number(userId),
        event_id: Number(eventId),
        question,
        description,
      });
      setCreateMarketForm({ question: '', description: '' });
      closeAdminPanel();
      await loadMarkets();
    } catch (e) {
      console.error(e);
      setMarketActionError(e.message || 'Failed to create market');
    }
  };

  const refreshEventData = async () => {
    await Promise.all([loadEvent(), loadMarkets()]);
  };

  const handleRenameEvent = async () => {
    const caption = editEventForm.caption.trim();
    if (!caption || !canManageEvent) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/events/lifecycle', {
        action: 'UPDATE_EVENT',
        user_id: Number(userId),
        event_id: Number(eventId),
        caption,
      });
      closeAdminPanel();
      await loadEvent();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to update event');
    }
  };

  const handleAllowRole = async () => {
    const roleId = allowRoleForm.roleId;
    if (!roleId || !canManageEvent) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/events/lifecycle', {
        action: 'DESIGNATE_EVENT_OPEN_TO',
        user_id: Number(userId),
        event_id: Number(eventId),
        role_id: roleId,
      });
      closeAdminPanel();
      await loadEvent();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to add event visibility role');
    }
  };

  const handleAddEventToken = async () => {
    if (!canManageEvent) return;
    setEventTokenId(String(organizationTokens[0]?.token_id || ''));
    openAdminPanel('add-token');
  };

  const submitAddEventToken = async () => {
    if (!eventTokenId || !canManageEvent) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/events/lifecycle', {
        action: 'DESIGNATE_EVENT_TOKEN',
        user_id: Number(userId),
        event_id: Number(eventId),
        token_id: Number(eventTokenId),
      });
      closeAdminPanel();
      await loadEvent();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to add event token');
    }
  };

  const handleAddMarketCreator = async () => {
    if (!canManageEvent) return;
    setMarketCreatorId(String(organizationMembers[0]?.user_id || ''));
    openAdminPanel('add-creator');
  };

  const submitAddMarketCreator = async () => {
    if (!marketCreatorId || !canManageEvent) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/events/lifecycle', {
        action: 'DESIGNATE_EVENT_MARKET_CREATOR',
        user_id: Number(userId),
        event_id: Number(eventId),
        market_creator_id: Number(marketCreatorId),
      });
      closeAdminPanel();
      await loadEvent();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to add market creator');
    }
  };

  const handleAddEventRule = async () => {
    const constraintId = eventRuleForm.constraintId;
    const value = eventRuleForm.value;
    if (!constraintId || !value || !canManageEvent) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/events/lifecycle', {
        action: 'DESIGNATE_EVENT_CONSTRAINT',
        user_id: Number(userId),
        event_id: Number(eventId),
        constraint_id: Number(constraintId),
        value: Number(value),
      });
      setEventRuleForm({ constraintId: '', value: '' });
      closeAdminPanel();
      await loadEvent();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to add event rule');
    }
  };

  const submitDeleteEvent = async () => {
    if (!canManageEvent) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/events/lifecycle', {
        action: 'DELETE_EVENT',
        user_id: Number(userId),
        event_id: Number(eventId),
      });
      closeAdminPanel();
      navigate(`/organization/${organizationId}${userId ? `?userId=${userId}` : ''}`);
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to delete event');
    }
  };

  return (
    <section className="event-page" aria-label="Event dashboard">
      <div className="event-shell">
        <p className="event-kicker">Event Dashboard</p>
        <h1>{loading ? 'Loading event...' : eventData?.caption || 'Event not found'}</h1>
        <p className="event-subtitle">
          {eventData
            ? `Organization #${eventData.organization_id} · Status: ${
                eventData.is_open ? 'Open' : 'Closed'
              }`
            : 'Open an event from the organization dashboard to see details here.'}
        </p>

        <div className="event-nav">
          <Link
            className="page-back-link"
            to={`/organization/${organizationId}${userId ? `?userId=${userId}` : ''}`}
            aria-label="Back to organization"
          >
            <span className="page-back-link__arrow" aria-hidden="true">
              {'<'}
            </span>
            <span className="page-back-link__label">Organization</span>
          </Link>
        </div>
        <div className="event-action-groups">
          <section className="event-action-group">
            <div className="event-action-group__header">
              <span>Event actions</span>
              <p>Core actions for creating and reviewing markets in this event.</p>
            </div>
            <div className="event-actions ui-action-bar">
              {canCreateMarket && (
                <button
                  type="button"
                  className="ui-action-button ui-action-button--primary"
                  onClick={() => {
                    setCreateMarketForm({ question: '', description: '' });
                    openAdminPanel('create-market');
                  }}
                >
                  Create market
                </button>
              )}
            </div>
          </section>
          {canManageEvent && (
            <section className="event-action-group event-action-group--owner">
              <div className="event-action-group__header">
                <span>Owner actions</span>
                <p>Manage access, permissions, and event-level configuration.</p>
              </div>
              <div className="event-actions ui-action-bar">
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={() => {
                    setEditEventForm({ caption: eventData?.caption || '' });
                    openAdminPanel('edit-event');
                  }}
                >
                  Edit event
                </button>
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={() => {
                    setAllowRoleForm({ roleId: organizationRoles[0]?.role_id || '' });
                    openAdminPanel('allow-role');
                  }}
                >
                  Allow role
                </button>
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={handleAddEventToken}
                >
                  Add token
                </button>
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={handleAddMarketCreator}
                >
                  Add market creator
                </button>
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={() => {
                    setEventRuleForm({
                      constraintId: String(availableConstraints[0]?.constraint_id || ''),
                      value: '',
                    });
                    openAdminPanel('add-rule');
                  }}
                >
                  Add rule
                </button>
                <button
                  type="button"
                  className="ui-action-button ui-action-button--ghost"
                  onClick={() => openAdminPanel('delete-event')}
                >
                  Delete event
                </button>
              </div>
            </section>
          )}
        </div>
        {activeAdminPanel === 'create-market' && (
          <InlineActionPanel
            title="Create market"
            description="Define a new market directly inside the event dashboard."
            onSubmit={(event) => {
              event.preventDefault();
              handleCreateMarket();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Create market"
            submitDisabled={!createMarketForm.question.trim()}
          >
            <label>
              Market question
              <input
                type="text"
                value={createMarketForm.question}
                onChange={(event) =>
                  setCreateMarketForm((current) => ({ ...current, question: event.target.value }))
                }
                placeholder="Will Polaris daily active users exceed 1,000 by June 1?"
              />
            </label>
            <label data-span="full">
              Description
              <textarea
                value={createMarketForm.description}
                onChange={(event) =>
                  setCreateMarketForm((current) => ({ ...current, description: event.target.value }))
                }
                placeholder="Add context for traders and reviewers."
              />
            </label>
          </InlineActionPanel>
        )}
        {activeAdminPanel === 'edit-event' && (
          <InlineActionPanel
            title="Edit event"
            description="Rename the event without breaking flow."
            onSubmit={(event) => {
              event.preventDefault();
              handleRenameEvent();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Save event"
            submitDisabled={!editEventForm.caption.trim()}
          >
            <label data-span="full">
              Event caption
              <input
                type="text"
                value={editEventForm.caption}
                onChange={(event) => setEditEventForm({ caption: event.target.value })}
              />
            </label>
          </InlineActionPanel>
        )}
        {activeAdminPanel === 'allow-role' && (
          <InlineActionPanel
            title="Allow role"
            description="Grant event visibility to one of the organization roles."
            onSubmit={(event) => {
              event.preventDefault();
              handleAllowRole();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Allow role"
            submitDisabled={!allowRoleForm.roleId}
          >
            <label data-span="full">
              Role
              <select
                value={allowRoleForm.roleId}
                onChange={(event) => setAllowRoleForm({ roleId: event.target.value })}
              >
                <option value="" disabled>
                  Select a role
                </option>
                {organizationRoles.map((role) => (
                  <option key={role.role_id} value={role.role_id}>
                    {formatRoleOption(role)}
                  </option>
                ))}
              </select>
            </label>
          </InlineActionPanel>
        )}
        {activeAdminPanel === 'add-token' && (
          <InlineActionPanel
            title="Add event token"
            description="Choose an organization token to enable for this event."
            onSubmit={(event) => {
              event.preventDefault();
              submitAddEventToken();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Add token"
            submitDisabled={!eventTokenId}
          >
            <label data-span="full">
              Token
              <select value={eventTokenId} onChange={(event) => setEventTokenId(event.target.value)}>
                <option value="" disabled>
                  Select a token
                </option>
                {organizationTokens.map((token) => (
                  <option key={token.token_id} value={String(token.token_id)}>
                    {token.name}
                  </option>
                ))}
              </select>
            </label>
          </InlineActionPanel>
        )}
        {activeAdminPanel === 'add-creator' && (
          <InlineActionPanel
            title="Add market creator"
            description="Authorize one of the organization members to create markets for this event."
            onSubmit={(event) => {
              event.preventDefault();
              submitAddMarketCreator();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Add creator"
            submitDisabled={!marketCreatorId}
          >
            <label data-span="full">
              Member
              <select value={marketCreatorId} onChange={(event) => setMarketCreatorId(event.target.value)}>
                <option value="" disabled>
                  Select a member
                </option>
                {organizationMembers.map((member) => (
                  <option key={`${member.user_id}-${member.role_id || 'member'}`} value={String(member.user_id)}>
                    {formatMemberLabel(member)}
                  </option>
                ))}
              </select>
            </label>
          </InlineActionPanel>
        )}
        {activeAdminPanel === 'add-rule' && (
          <InlineActionPanel
            title="Add event rule"
            description="Attach a constraint id and value without a popup."
            onSubmit={(event) => {
              event.preventDefault();
              handleAddEventRule();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Add rule"
            submitDisabled={!eventRuleForm.constraintId || !eventRuleForm.value}
          >
            <label>
              Constraint
              <select
                value={eventRuleForm.constraintId}
                onChange={(event) =>
                  setEventRuleForm((current) => ({ ...current, constraintId: event.target.value }))
                }
              >
                <option value="" disabled>
                  Select a constraint
                </option>
                {availableConstraints.map((constraint) => (
                  <option key={constraint.constraint_id} value={String(constraint.constraint_id)}>
                    {formatConstraintOption(constraint)}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Value
              <input
                type="number"
                step="1"
                value={eventRuleForm.value}
                onChange={(event) =>
                  setEventRuleForm((current) => ({ ...current, value: event.target.value }))
                }
              />
            </label>
          </InlineActionPanel>
        )}
        {activeAdminPanel === 'delete-event' && (
          <InlineActionPanel
            title="Delete event"
            description="This permanently removes the event and its related markets, permissions, and rules."
            onSubmit={(event) => {
              event.preventDefault();
              submitDeleteEvent();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Delete event"
          >
            <label data-span="full">
              Event
              <input type="text" value={eventData?.caption || 'Current event'} readOnly />
            </label>
          </InlineActionPanel>
        )}
        {marketActionError && <p className="event-muted">{marketActionError}</p>}
        {adminError && <p className="event-muted">{adminError}</p>}

        <section className="event-dashboard-content">
          <header className="event-dashboard-content__header">
            <h2>Binary Markets</h2>
            <p>Track yes/no probabilities, volume, and role-specific views for this event.</p>
          </header>

          <div className="binary-market-grid" aria-label="Binary market cards">
            {marketsLoading && <p className="event-muted">Loading markets...</p>}
            {!marketsLoading && markets.length === 0 && (
              <p className="event-muted">No markets available for this event yet.</p>
            )}
            {markets.map((market) => (
              <article key={market.question} className="binary-market-card">
                <h3>
                  <Link
                    to={`/organization/${organizationId}/events/${eventId}/markets/${market.market_id}${
                      userId ? `?userId=${userId}` : ''
                    }`}
                  >
                    {market.question}
                  </Link>
                </h3>
                <p className="binary-market-card__meta">
                  Market #{market.market_id} · {market.is_open ? 'Open' : 'Closed'}
                </p>
                <div className="binary-market-card__odds">
                  <span className="odds-pill odds-pill--yes">Created by {market.created_by}</span>
                  <span className="odds-pill odds-pill--no">
                    Closes {market.close_at ? new Date(market.close_at).toLocaleString() : 'TBD'}
                  </span>
                </div>
              </article>
            ))}
          </div>

          <div className="role-dashboard-grid">
            <article className="event-card">
              <h3>{activeRoleSection.title}</h3>
              <ul className="event-checklist">
                {activeRoleSection.items.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
              {roleView === 'analyzer' && (
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={() => setShowAnalytics((value) => !value)}
                >
                  {showAnalytics ? 'Hide Market Analytics' : 'Analyze Event Performance'}
                </button>
              )}
            </article>
          </div>

          <article className="event-card event-card--tokens">
            <h2>Allowed Tokens</h2>
            {tokensAllowed.length === 0 && (
              <p className="event-muted">No allowed tokens found for this event.</p>
            )}
            {tokensAllowed.length > 0 && (
              <ul className="token-list">
                {tokensAllowed.map((tokenId) => (
                  <li key={tokenId}>
                    <div>
                      <strong>{tokenNameById[String(tokenId)] || `Token #${tokenId}`}</strong>
                      <p>Allowed for this event.</p>
                    </div>
                    <span>Enabled</span>
                  </li>
                ))}
              </ul>
            )}
          </article>

          {roleView === 'analyzer' && showAnalytics && (
            <article className="event-card event-card--tokens">
              <h2>Market Analytics</h2>
              {analyticsLoading && <p className="event-muted">Loading market analytics...</p>}
              {analyticsError && <p className="event-muted">{analyticsError}</p>}
              {!analyticsLoading && !analyticsError && marketAnalytics.length === 0 && (
                <p className="event-muted">No analytics available until this event has markets.</p>
              )}
              {marketAnalytics.length > 0 && (
                <ul className="token-list">
                  {marketAnalytics.map((entry) => (
                    <li key={entry.marketId}>
                      <div>
                        <strong>{entry.question}</strong>
                        <p>
                          Pool {entry.liquidity.total_pool} · Trades {entry.liquidity.trade_count} ·
                          Gross volume {entry.liquidity.gross_volume}
                        </p>
                        <p>
                          Window trades {entry.windowComparison.current_window.trade_count} vs previous{' '}
                          {entry.windowComparison.previous_window.trade_count}
                        </p>
                        <p>
                          Whale holders {(entry.whales.whales || []).length} · Distribution buckets{' '}
                          {Object.keys(entry.tradeDistribution || {}).join(', ') || 'none'}
                        </p>
                      </div>
                      <span>
                        Yes {entry.liquidity.yes_price}% / No {entry.liquidity.no_price}%
                      </span>
                    </li>
                  ))}
                </ul>
              )}
            </article>
          )}
        </section>
      </div>
    </section>
  );
}
