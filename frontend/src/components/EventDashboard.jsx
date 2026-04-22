import { Link, useParams, useSearchParams } from 'react-router-dom';
import { useEffect, useState } from 'react';
import './EventDashboard.css';
import { readJson } from '../lib/api';
import { getStoredUserId } from '../lib/auth';
import { normalizeOrganizationMembershipList } from '../lib/organizations';

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
  const [markets, setMarkets] = useState([]);
  const [marketsLoading, setMarketsLoading] = useState(false);
  const [roleView, setRoleView] = useState('viewer');
  const [showAnalytics, setShowAnalytics] = useState(false);
  const [analyticsLoading, setAnalyticsLoading] = useState(false);
  const [analyticsError, setAnalyticsError] = useState(null);
  const [marketAnalytics, setMarketAnalytics] = useState([]);

  useEffect(() => {
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
    loadEvent();
  }, [eventId, userId]);

  useEffect(() => {
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
    loadMarkets();
  }, [eventId, userId]);

  useEffect(() => {
    const loadAnalytics = async () => {
      if (!showAnalytics || roleView !== 'analyzer' || !userId || markets.length === 0) {
        setMarketAnalytics([]);
        setAnalyticsError(null);
        return;
      }

      setAnalyticsLoading(true);
      setAnalyticsError(null);
      try {
        const analyticsRows = await Promise.all(
          markets.map(async (market) => {
            const marketId = market.market_id;
            const q = `user_id=${encodeURIComponent(userId)}&market_id=${encodeURIComponent(marketId)}`;
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
  }, [showAnalytics, roleView, userId, markets]);

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

        <div className="event-actions">
          <Link to={`/organization/${organizationId}${userId ? `?userId=${userId}` : ''}`}>Back to organization</Link>
        </div>

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
                <h3>{market.question}</h3>
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
                  className="analyze-btn"
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
                      <strong>Token #{tokenId}</strong>
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
