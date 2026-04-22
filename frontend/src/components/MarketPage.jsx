import { Link, useParams, useSearchParams } from 'react-router-dom';
import { useEffect, useMemo, useState } from 'react';
import './MarketPage.css';
import ActionDialog from './ActionDialog';
import { pollOperation, postJson, putJson, readJson, submitV2Operation } from '../lib/api';
import { getStoredUserId } from '../lib/auth';
import { normalizeOrganizationMembershipList } from '../lib/organizations';

function buildRoleView(membership) {
  if (!membership) return 'viewer';
  if (membership.membership === 'leader') return 'analyzer';
  const normalizedRole = String(membership.role_id || '').toLowerCase();
  if (
    normalizedRole.includes('stat') ||
    normalizedRole.includes('analyst') ||
    normalizedRole.includes('analytics')
  ) {
    return 'analyzer';
  }
  return membership.membership === 'member' ? 'bettor' : 'viewer';
}

export default function MarketPage() {
  const { organizationId, eventId, marketId } = useParams();
  const [searchParams] = useSearchParams();
  const userId = searchParams.get('userId') || getStoredUserId();

  const [market, setMarket] = useState(null);
  const [membership, setMembership] = useState(null);
  const [marketLoading, setMarketLoading] = useState(false);
  const [marketError, setMarketError] = useState(null);
  const [tradeError, setTradeError] = useState(null);
  const [tradeSubmitting, setTradeSubmitting] = useState(false);
  const [showAnalytics, setShowAnalytics] = useState(false);
  const [analyticsLoading, setAnalyticsLoading] = useState(false);
  const [analyticsError, setAnalyticsError] = useState(null);
  const [analytics, setAnalytics] = useState(null);
  const [adminError, setAdminError] = useState(null);
  const [organizationData, setOrganizationData] = useState(null);
  const [showAddTokenDialog, setShowAddTokenDialog] = useState(false);
  const [marketTokenId, setMarketTokenId] = useState('');
  const [tradeForm, setTradeForm] = useState({
    transactionType: 'BUY',
    side: 'YES',
    qty: '1',
    tokenId: '',
  });

  const roleView = buildRoleView(membership);
  const canBet = roleView === 'bettor' || roleView === 'analyzer';
  const canViewAnalytics = roleView === 'analyzer';
  const canManageMarket = !!userId && (market?.is_leader || Number(market?.created_by) === Number(userId));

  const allowedTokenIds = useMemo(
    () => (Array.isArray(market?.tokens_allowed) ? market.tokens_allowed : []),
    [market]
  );
  const organizationTokens = Array.isArray(organizationData?.tokens) ? organizationData.tokens : [];
  const organizationMembers = Array.isArray(organizationData?.members) ? organizationData.members : [];
  const tokenNameById = useMemo(
    () => Object.fromEntries(organizationTokens.map((token) => [String(token.token_id), token.name])),
    [organizationTokens]
  );
  const memberNameById = useMemo(
    () =>
      Object.fromEntries(
        organizationMembers.map((member) => {
          const fullName = [member?.first, member?.last].filter(Boolean).join(' ').trim();
          const label = fullName || member?.username || `User #${member?.user_id ?? ''}`;
          return [String(member.user_id), label];
        })
      ),
    [organizationMembers]
  );

  useEffect(() => {
    if (!userId) return;
    let cancelled = false;

    const loadMembership = async () => {
      try {
        const orgs = normalizeOrganizationMembershipList(
          await readJson(`/dashboard/users/${userId}/organizations`)
        );
        if (cancelled) return;
        const current = orgs.find((row) => String(row.organization_id) === String(organizationId));
        setMembership(current || null);
      } catch (error) {
        console.error(error);
        if (!cancelled) {
          setMembership(null);
        }
      }
    };

    loadMembership();
    return () => {
      cancelled = true;
    };
  }, [organizationId, userId]);

  useEffect(() => {
    if (!marketId || !userId) return;
    let cancelled = false;

    const loadMarket = async () => {
      setMarketLoading(true);
      setMarketError(null);
      try {
        const data = await readJson(`/markets/${marketId}?user_id=${encodeURIComponent(userId)}`);
        if (cancelled) return;
        setMarket(data);
        setTradeForm((current) => ({
          ...current,
          tokenId:
            current.tokenId ||
            String((Array.isArray(data?.tokens_allowed) && data.tokens_allowed[0]) || ''),
        }));
      } catch (error) {
        console.error(error);
        if (!cancelled) {
          setMarket(null);
          setMarketError(error.message || 'Failed to load market');
        }
      } finally {
        if (!cancelled) {
          setMarketLoading(false);
        }
      }
    };

    loadMarket();
    return () => {
      cancelled = true;
    };
  }, [marketId, userId]);

  useEffect(() => {
    if (!market?.organization_id || !userId) {
      setOrganizationData(null);
      return;
    }
    let cancelled = false;
    const loadOrganization = async () => {
      try {
        const data = await readJson(
          `/organizations/${market.organization_id}?user_id=${encodeURIComponent(userId)}`
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
  }, [market?.organization_id, userId]);

  useEffect(() => {
    if (!showAnalytics || !canViewAnalytics || !userId || !marketId) {
      setAnalytics(null);
      setAnalyticsError(null);
      return;
    }

    let cancelled = false;

    const loadAnalytics = async () => {
      setAnalyticsLoading(true);
      setAnalyticsError(null);
      try {
        const q = `user_id=${encodeURIComponent(userId)}&market_id=${encodeURIComponent(marketId)}`;
        const [liquidity, timeFocus, whales, tradeDistribution, windowComparison, points] =
          await Promise.all([
            readJson(`/markets/stats/liquidity?${q}`),
            readJson(`/markets/stats/time-focus?${q}`),
            readJson(`/markets/stats/whales?${q}`),
            readJson(`/markets/stats/trade-distribution?${q}`),
            readJson(`/markets/stats/window-comparison?${q}&hours=24`),
            readJson(`/markets/points?${q}&span=25`),
          ]);
        if (cancelled) return;
        setAnalytics({
          liquidity,
          timeFocus,
          whales,
          tradeDistribution,
          windowComparison,
          points,
        });
      } catch (error) {
        console.error(error);
        if (!cancelled) {
          setAnalytics(null);
          setAnalyticsError(error.message || 'Failed to load analytics');
        }
      } finally {
        if (!cancelled) {
          setAnalyticsLoading(false);
        }
      }
    };

    loadAnalytics();
    return () => {
      cancelled = true;
    };
  }, [showAnalytics, canViewAnalytics, userId, marketId]);

  const handleTradeChange = (field) => (event) => {
    setTradeForm((current) => ({ ...current, [field]: event.target.value }));
  };

  const refreshAfterTrade = async () => {
    if (!userId || !marketId) return;
    const data = await readJson(`/markets/${marketId}?user_id=${encodeURIComponent(userId)}`);
    setMarket(data);
    if (showAnalytics && canViewAnalytics) {
      const q = `user_id=${encodeURIComponent(userId)}&market_id=${encodeURIComponent(marketId)}`;
      const [liquidity, timeFocus, whales, tradeDistribution, windowComparison, points] =
        await Promise.all([
          readJson(`/markets/stats/liquidity?${q}`),
          readJson(`/markets/stats/time-focus?${q}`),
          readJson(`/markets/stats/whales?${q}`),
          readJson(`/markets/stats/trade-distribution?${q}`),
          readJson(`/markets/stats/window-comparison?${q}&hours=24`),
          readJson(`/markets/points?${q}&span=25`),
        ]);
      setAnalytics({
        liquidity,
        timeFocus,
        whales,
        tradeDistribution,
        windowComparison,
        points,
      });
    }
  };

  const handleSubmitTrade = async (event) => {
    event.preventDefault();
    if (!userId || !marketId || !tradeForm.tokenId) return;

    setTradeSubmitting(true);
    setTradeError(null);
    try {
      const operation = await submitV2Operation('/markets/transactions', {
        action: 'MARKET_TRANSACTION',
        user_id: Number(userId),
        market_id: Number(marketId),
        token_id: Number(tradeForm.tokenId),
        side: tradeForm.side === 'YES',
        qty: Number(tradeForm.qty),
        transaction_id: Date.now(),
        transaction_type: tradeForm.transactionType,
      });
      await pollOperation(operation.operation_id, {
        headers: { 'X-Force-Leader': 'true' },
      });
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setTradeError(error.message || 'Trade failed');
    } finally {
      setTradeSubmitting(false);
    }
  };

  const handleRenameMarket = async () => {
    const question = window.prompt('Market question', market?.question || '');
    if (!question || !canManageMarket) return;
    setAdminError(null);
    try {
      await putJson(`/markets/${marketId}`, {
        user_id: Number(userId),
        question,
      });
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to update market');
    }
  };

  const handleAddMarketToken = async () => {
    if (!canManageMarket) return;
    setMarketTokenId(String(organizationTokens[0]?.token_id || ''));
    setShowAddTokenDialog(true);
  };

  const submitAddMarketToken = async () => {
    if (!marketTokenId || !canManageMarket) return;
    setAdminError(null);
    try {
      await postJson('/markets/designate-token', {
        user_id: Number(userId),
        market_id: Number(marketId),
        token_id: Number(marketTokenId),
      });
      setShowAddTokenDialog(false);
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to add market token');
    }
  };

  const handleAllowMarketRole = async () => {
    const roleId = window.prompt('Role id allowed in this market');
    if (!roleId || !canManageMarket) return;
    const asId = window.prompt('Access level (as_id / market_as code)');
    if (!asId) return;
    setAdminError(null);
    try {
      await postJson('/markets/designate-open-to-as', {
        user_id: Number(userId),
        market_id: Number(marketId),
        role_id: roleId,
        as_id: asId,
      });
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to update market access');
    }
  };

  const handleResolveMarket = async () => {
    const outcome = window.prompt('Resolve market to YES or NO');
    if (!outcome || !canManageMarket) return;
    const normalized = outcome.trim().toUpperCase();
    if (!['YES', 'NO', 'TRUE', 'FALSE'].includes(normalized)) {
      setAdminError('Enter YES or NO when resolving the market');
      return;
    }
    setAdminError(null);
    try {
      await postJson('/markets/designate-result', {
        user_id: Number(userId),
        market_id: Number(marketId),
        result: normalized === 'YES' || normalized === 'TRUE',
      });
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to resolve market');
    }
  };

  const handleAddMarketRule = async () => {
    const constraintId = window.prompt(
      'Constraint id for the market rule (for example, a configured max-spend rule id)'
    );
    const value = window.prompt('Constraint value');
    if (!constraintId || !value || !canManageMarket) return;
    setAdminError(null);
    try {
      await postJson('/markets/designate-constraint', {
        user_id: Number(userId),
        market_id: Number(marketId),
        constraint_id: Number(constraintId),
        value: Number(value),
      });
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to add market rule');
    }
  };

  return (
    <section className="market-page" aria-label="Market page">
      <div className="market-shell">
        <div className="market-actions">
          <Link to={`/organization/${organizationId}/events/${eventId}${userId ? `?userId=${userId}` : ''}`}>
            Back to event
          </Link>
          {canViewAnalytics && (
            <button
              type="button"
              className="market-toggle"
              onClick={() => setShowAnalytics((value) => !value)}
            >
              {showAnalytics ? 'Hide Analytics' : 'View Analytics'}
            </button>
          )}
          {canManageMarket && (
            <>
              <button type="button" className="market-toggle" onClick={handleRenameMarket}>Edit Market</button>
              <button type="button" className="market-toggle" onClick={handleAddMarketToken}>Add Token</button>
              <button type="button" className="market-toggle" onClick={handleAllowMarketRole}>Allow Role</button>
              <button type="button" className="market-toggle" onClick={handleAddMarketRule}>Add Rule</button>
              <button type="button" className="market-toggle" onClick={handleResolveMarket}>Resolve</button>
            </>
          )}
        </div>
        {adminError && <p className="market-error">{adminError}</p>}

        <header className="market-hero">
          <p className="market-kicker">Market Detail</p>
          <h1>{marketLoading ? 'Loading market...' : market?.question || 'Market not found'}</h1>
          <p>
            {market
              ? `Event #${market.event_id} · ${market.is_open ? 'Open' : 'Closed'} · Created by ${
                  memberNameById[String(market.created_by)] || `User #${market.created_by}`
                }`
              : 'Open a market from the event page to trade and review analytics.'}
          </p>
          {marketError && <p className="market-error">{marketError}</p>}
        </header>

        <section className="market-grid">
          <article className="market-card">
            <h2>Market State</h2>
            <ul className="market-list">
              <li>Organization: {market?.organization_id ?? '-'}</li>
              <li>
                Allowed tokens:{' '}
                {allowedTokenIds.length
                  ? allowedTokenIds.map((tokenId) => tokenNameById[String(tokenId)] || `Token #${tokenId}`).join(', ')
                  : 'None'}
              </li>
              <li>Access role: {market?.role_id || 'viewer'}</li>
              <li>Created: {market?.created_at ? new Date(market.created_at).toLocaleString() : 'Unknown'}</li>
              <li>Close at: {market?.close_at ? new Date(market.close_at).toLocaleString() : 'Not scheduled'}</li>
              <li>
                Result:{' '}
                {market?.result
                  ? `${market.result.outcome ? 'YES' : 'NO'} at ${new Date(market.result.resolved_at).toLocaleString()}`
                  : 'Unresolved'}
              </li>
            </ul>
          </article>

          <article className="market-card">
            <h2>Betting</h2>
            {!canBet && (
              <p className="market-muted">
                You can view this market, but your current role does not have betting controls.
              </p>
            )}
            {canBet && (
              <form className="market-form" onSubmit={handleSubmitTrade}>
                <label>
                  Transaction
                  <select value={tradeForm.transactionType} onChange={handleTradeChange('transactionType')}>
                    <option value="BUY">Buy</option>
                    <option value="SELL">Sell</option>
                  </select>
                </label>
                <label>
                  Side
                  <select value={tradeForm.side} onChange={handleTradeChange('side')}>
                    <option value="YES">Yes</option>
                    <option value="NO">No</option>
                  </select>
                </label>
                <label>
                  Quantity
                  <input
                    type="number"
                    min="1"
                    step="1"
                    value={tradeForm.qty}
                    onChange={handleTradeChange('qty')}
                  />
                </label>
                <label>
                  Token
                  <select value={tradeForm.tokenId} onChange={handleTradeChange('tokenId')}>
                    {allowedTokenIds.map((tokenId) => (
                      <option key={tokenId} value={String(tokenId)}>
                        {tokenNameById[String(tokenId)] || `Token #${tokenId}`}
                      </option>
                    ))}
                  </select>
                </label>
                <button type="submit" className="market-submit" disabled={tradeSubmitting || !tradeForm.tokenId}>
                  {tradeSubmitting ? 'Submitting…' : 'Place Trade'}
                </button>
                {tradeError && <p className="market-error">{tradeError}</p>}
              </form>
            )}
          </article>
        </section>

        {canViewAnalytics && showAnalytics && (
          <section className="market-analytics">
            <article className="market-card">
              <h2>Analytics</h2>
              {analyticsLoading && <p className="market-muted">Loading analytics...</p>}
              {analyticsError && <p className="market-error">{analyticsError}</p>}
              {!analyticsLoading && !analyticsError && analytics && (
                <div className="market-analytics-grid">
                  <div>
                    <h3>Liquidity</h3>
                    <p>
                      Yes {analytics.liquidity.yes_price}% · No {analytics.liquidity.no_price}% · Pool{' '}
                      {analytics.liquidity.total_pool}
                    </p>
                    <p>
                      Open tickets {analytics.liquidity.open_tickets} · Trades {analytics.liquidity.trade_count}
                    </p>
                  </div>
                  <div>
                    <h3>Time Focus</h3>
                    <p>
                      24h trades {analytics.timeFocus.trade_count} · Buy {analytics.timeFocus.buy_trades} · Sell{' '}
                      {analytics.timeFocus.sell_trades}
                    </p>
                    <p>24h traded value {analytics.timeFocus.traded_value}</p>
                  </div>
                  <div>
                    <h3>Whales</h3>
                    <p>
                      Whale holders {(analytics.whales.whales || []).length} · Open tickets{' '}
                      {analytics.whales.total_open_tickets}
                    </p>
                  </div>
                  <div>
                    <h3>Window Comparison</h3>
                    <p>
                      Current window {analytics.windowComparison.current_window.trade_count} vs previous{' '}
                      {analytics.windowComparison.previous_window.trade_count}
                    </p>
                  </div>
                  <div>
                    <h3>Trade Distribution</h3>
                    <p>{Object.keys(analytics.tradeDistribution || {}).join(', ') || 'No buckets yet'}</p>
                  </div>
                  <div>
                    <h3>Price Points</h3>
                    <p>{Array.isArray(analytics.points) ? analytics.points.length : 0} chart points loaded</p>
                  </div>
                </div>
              )}
            </article>
          </section>
        )}
      </div>
      {showAddTokenDialog && (
        <ActionDialog
          title="Add Market Token"
          description="Choose the token by name and Polaris will send the right token id."
          onClose={() => setShowAddTokenDialog(false)}
          onSubmit={submitAddMarketToken}
          submitLabel="Add Token"
          submitDisabled={!marketTokenId}
        >
          <label>
            Token
            <select value={marketTokenId} onChange={(event) => setMarketTokenId(event.target.value)}>
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
        </ActionDialog>
      )}
    </section>
  );
}
