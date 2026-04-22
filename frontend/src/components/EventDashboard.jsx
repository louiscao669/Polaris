import { Link, useParams, useSearchParams } from 'react-router-dom';
import { useEffect, useState } from 'react';
import './EventDashboard.css';
import { readJson } from '../lib/api';
import { getStoredUserId } from '../lib/auth';
import { normalizeOrganizationMembershipList } from '../lib/organizations';

const binaryMarkets = [
  { question: 'Will Q2 launch ship before June 30?', yes: 62, no: 38, volume: '2,140' },
  { question: 'Will API latency stay under 200ms this month?', yes: 47, no: 53, volume: '1,275' },
  { question: 'Will support tickets drop 20% this sprint?', yes: 58, no: 42, volume: '964' },
];

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

export default function EventDashboard() {
  const { organizationId, eventId } = useParams();
  const [searchParams] = useSearchParams();
  const userId = searchParams.get('userId') || getStoredUserId();
  const [eventData, setEventData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [tokens, setTokens] = useState([]);
  const [tokensLoading, setTokensLoading] = useState(false);
  const [roleView, setRoleView] = useState('viewer');

  useEffect(() => {
    const loadEvent = async () => {
      if (!eventId) return;
      setLoading(true);
      try {
        const data = await readJson(`/events/${eventId}`);
        setEventData(data);
      } catch (e) {
        console.error(e);
        setEventData(null);
      } finally {
        setLoading(false);
      }
    };
    loadEvent();
  }, [eventId]);

  useEffect(() => {
    const loadTokens = async () => {
      if (!organizationId) return;
      setTokensLoading(true);
      try {
        const tokenIds = await readJson(`/dashboard/organizations/${organizationId}/tokens-allowed`);
        if (!Array.isArray(tokenIds) || tokenIds.length === 0) {
          setTokens([]);
          return;
        }
        const tokenRows = await Promise.all(
          tokenIds.map(async (tokenId) => {
            const [nameRes, descRes, qtyRes] = await Promise.all([
              readJson(`/dashboard/organizations/${organizationId}/${tokenId}/token-name`),
              readJson(`/dashboard/organizations/${organizationId}/${tokenId}/token-description`),
              readJson(
                `/dashboard/organizations/${organizationId}/${tokenId}/token-quantity${
                  userId ? `?user_id=${userId}` : ''
                }`
              ),
            ]);
            return { tokenId, name: nameRes, description: descRes, quantity: qtyRes };
          })
        );
        setTokens(tokenRows);
      } catch (e) {
        console.error(e);
        setTokens([]);
      } finally {
        setTokensLoading(false);
      }
    };
    loadTokens();
  }, [organizationId, userId]);

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

  return (
    <section className="event-page" aria-label="Event dashboard">
      <div className="event-shell">
        <p className="event-kicker">Event Dashboard</p>
        <h1>{loading ? 'Loading event...' : eventData?.caption || 'Event not found'}</h1>
        <p className="event-subtitle">
          {eventData
            ? `Organization: ${eventData.organization_name} · Status: ${
                eventData.is_open ? 'Open' : 'Closed'
              }`
            : 'Open an event from the organization dashboard to see details here.'}
        </p>

        <div className="event-actions">
          <Link to={`/organization/${organizationId}${userId ? `?userId=${userId}` : ''}`}>Back to organization</Link>
        </div>

        <section className="event-dashboard-content">
          <header className="event-dashboard-content__header">
            <h2>Binary Markets</h2>
            <p>Track yes/no probabilities, volume, and role-specific views for this event.</p>
          </header>

          <div className="binary-market-grid" aria-label="Binary market cards">
            {binaryMarkets.map((market) => (
              <article key={market.question} className="binary-market-card">
                <h3>{market.question}</h3>
                <p className="binary-market-card__meta">Volume: {market.volume} tokens</p>
                <div className="binary-market-card__odds">
                  <span className="odds-pill odds-pill--yes">Yes {market.yes}%</span>
                  <span className="odds-pill odds-pill--no">No {market.no}%</span>
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
                <button type="button" className="analyze-btn">Analyze Event Performance</button>
              )}
            </article>
          </div>

          <article className="event-card event-card--tokens">
            <h2>Allowed Tokens {userId ? '(Current User)' : ''}</h2>
            {tokensLoading && <p className="event-muted">Loading tokens...</p>}
            {!tokensLoading && tokens.length === 0 && (
              <p className="event-muted">No allowed tokens found for this event.</p>
            )}
            {tokens.length > 0 && (
              <ul className="token-list">
                {tokens.map((token) => (
                  <li key={token.tokenId}>
                    <div>
                      <strong>{token.name}</strong>
                      <p>{token.description}</p>
                    </div>
                    <span>Qty: {token.quantity}</span>
                  </li>
                ))}
              </ul>
            )}
          </article>
        </section>
      </div>
    </section>
  );
}
