import { Link, useParams, useSearchParams } from 'react-router-dom';
import { useEffect, useMemo, useState } from 'react';
import './MarketPage.css';
import InlineActionPanel from './InlineActionPanel';
import { pollOperation, postJson, putJson, readJson, submitV2Operation } from '../lib/api';
import { getStoredUserId } from '../lib/auth';
import { normalizeOrganizationMembershipList } from '../lib/organizations';
import {
  formatConstraintOption,
  formatMarketAccessOption,
  formatRoleOption,
  getMarketAccessLabel,
  getMarketAccessView,
  readPolicyOptions,
} from '../lib/policyOptions';

function formatAccessLevel(market, membership) {
  if (market?.is_leader || membership?.membership === 'leader') {
    return 'organization leader';
  }
  if (market?.role_id) {
    return String(market.role_id);
  }
  return 'viewer';
}

export default function MarketPage() {
  const { organizationId, eventId, marketId } = useParams();
  const [searchParams] = useSearchParams();
  const userId = searchParams.get('userId') || getStoredUserId();
  const numericUserId = Number(userId);
  const numericMarketId = Number(marketId);

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
  const [activeAdminPanel, setActiveAdminPanel] = useState(null);
  const [editMarketForm, setEditMarketForm] = useState({ question: '' });
  const [marketTokenId, setMarketTokenId] = useState('');
  const [allowRoleForm, setAllowRoleForm] = useState({ roleId: '', asId: '' });
  const [resolveMarketForm, setResolveMarketForm] = useState({ outcome: 'YES' });
  const [marketRuleForm, setMarketRuleForm] = useState({ constraintId: '', value: '' });
  const [policyOptions, setPolicyOptions] = useState({ constraints: [], market_access: [] });
  const [tradeForm, setTradeForm] = useState({
    transactionType: 'BUY',
    side: 'YES',
    qty: '1',
    tokenId: '',
  });

  const matchingAccessRole = useMemo(() => {
    if (membership?.membership === 'leader') {
      return { as_id: 'analytic' };
    }
    if (!membership?.role_id || !Array.isArray(market?.access_roles)) {
      return null;
    }
    return (
      market.access_roles.find((entry) => String(entry.role_id) === String(membership.role_id)) || null
    );
  }, [membership, market]);
  const roleView = getMarketAccessView(matchingAccessRole?.as_id);
  const canBet = roleView === 'bettor';
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
  const organizationRoles = Array.isArray(organizationData?.roles) ? organizationData.roles : [];
  const availableConstraints = Array.isArray(policyOptions?.constraints) ? policyOptions.constraints : [];
  const availableMarketAccess = Array.isArray(policyOptions?.market_access) ? policyOptions.market_access : [];

  const roleNameById = useMemo(
    () =>
      Object.fromEntries(
        organizationRoles.map((role) => [String(role.role_id), role.description || role.role_id])
      ),
    [organizationRoles]
  );
  const accessLevelLabel = useMemo(() => {
    if (market?.is_leader || membership?.membership === 'leader') {
      return 'organization leader';
    }
    if (market?.role_id) {
      const roleName = roleNameById[String(market.role_id)] || String(market.role_id);
      const matchingAccess = Array.isArray(market?.access_roles)
        ? market.access_roles.find((entry) => String(entry.role_id) === String(market.role_id))
        : null;
      const accessDescription = matchingAccess?.as_id
        ? getMarketAccessLabel(String(matchingAccess.as_id))
        : null;
      return accessDescription ? `${roleName} (${accessDescription})` : roleName;
    }
    return formatAccessLevel(market, membership);
  }, [market, membership, roleNameById]);

  const openAdminPanel = (panel) => {
    setAdminError(null);
    setActiveAdminPanel(panel);
  };

  const closeAdminPanel = () => {
    setActiveAdminPanel(null);
  };

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
    if (!showAnalytics || !canViewAnalytics || !Number.isFinite(numericUserId) || !Number.isFinite(numericMarketId)) {
      setAnalytics(null);
      setAnalyticsError(null);
      return;
    }

    let cancelled = false;

    const loadAnalytics = async () => {
      setAnalyticsLoading(true);
      setAnalyticsError(null);
      try {
        const q = `user_id=${encodeURIComponent(String(numericUserId))}&market_id=${encodeURIComponent(
          String(numericMarketId)
        )}`;
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
  }, [showAnalytics, canViewAnalytics, numericUserId, numericMarketId]);

  const handleTradeChange = (field) => (event) => {
    setTradeForm((current) => ({ ...current, [field]: event.target.value }));
  };

  const refreshAfterTrade = async () => {
    if (!Number.isFinite(numericUserId) || !Number.isFinite(numericMarketId)) return;
    const data = await readJson(
      `/markets/${numericMarketId}?user_id=${encodeURIComponent(String(numericUserId))}`
    );
    setMarket(data);
    if (showAnalytics && canViewAnalytics) {
      const q = `user_id=${encodeURIComponent(String(numericUserId))}&market_id=${encodeURIComponent(
        String(numericMarketId)
      )}`;
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
    if (!Number.isFinite(numericUserId) || !Number.isFinite(numericMarketId) || !tradeForm.tokenId) return;

    setTradeSubmitting(true);
    setTradeError(null);
    try {
      const operation = await submitV2Operation('/markets/transactions', {
        action: 'MARKET_TRANSACTION',
        user_id: numericUserId,
        market_id: numericMarketId,
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
    const question = editMarketForm.question.trim();
    if (!question || !canManageMarket) return;
    setAdminError(null);
    try {
      await putJson(`/markets/${marketId}`, {
        user_id: Number(userId),
        question,
      });
      closeAdminPanel();
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to update market');
    }
  };

  const handleAddMarketToken = async () => {
    if (!canManageMarket) return;
    setMarketTokenId(String(organizationTokens[0]?.token_id || ''));
    openAdminPanel('add-token');
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
      closeAdminPanel();
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to add market token');
    }
  };

  const handleAllowMarketRole = async () => {
    const roleId = allowRoleForm.roleId;
    if (!roleId || !canManageMarket) return;
    const asId = allowRoleForm.asId.trim();
    if (!asId) return;
    setAdminError(null);
    try {
      await postJson('/markets/designate-open-to-as', {
        user_id: Number(userId),
        market_id: Number(marketId),
        role_id: roleId,
        as_id: asId,
      });
      closeAdminPanel();
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to update market access');
    }
  };

  const handleResolveMarket = async () => {
    const outcome = resolveMarketForm.outcome;
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
      closeAdminPanel();
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to resolve market');
    }
  };

  const handleAddMarketRule = async () => {
    const constraintId = marketRuleForm.constraintId;
    const value = marketRuleForm.value;
    if (!constraintId || !value || !canManageMarket) return;
    setAdminError(null);
    try {
      await postJson('/markets/designate-constraint', {
        user_id: Number(userId),
        market_id: Number(marketId),
        constraint_id: Number(constraintId),
        value: Number(value),
      });
      setMarketRuleForm({ constraintId: '', value: '' });
      closeAdminPanel();
      await refreshAfterTrade();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to add market rule');
    }
  };

  return (
    <section className="market-page" aria-label="Market page">
      <div className="market-shell">
        <div className="market-nav">
          <Link
            className="page-back-link"
            to={`/organization/${organizationId}/events/${eventId}${userId ? `?userId=${userId}` : ''}`}
            aria-label="Back to event"
          >
            <span className="page-back-link__arrow" aria-hidden="true">
              {'<'}
            </span>
            <span className="page-back-link__label">Event</span>
          </Link>
        </div>
        <div className="market-action-groups">
          <section className="market-action-group">
            <div className="market-action-group__header">
              <span>Market actions</span>
              <p>Review the live forecast and switch between trading and analytics tools.</p>
            </div>
            <div className="market-actions">
              {canViewAnalytics && (
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={() => setShowAnalytics((value) => !value)}
                >
                  {showAnalytics ? 'Hide analytics' : 'View analytics'}
                </button>
              )}
            </div>
          </section>
          {canManageMarket && (
            <section className="market-action-group market-action-group--owner">
              <div className="market-action-group__header">
                <span>Owner actions</span>
                <p>Change market settings, access designations, and final resolution.</p>
              </div>
              <div className="market-actions">
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={() => {
                    setEditMarketForm({ question: market?.question || '' });
                    openAdminPanel('edit-market');
                  }}
                >
                  Edit market
                </button>
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={handleAddMarketToken}
                >
                  Add token
                </button>
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={() => {
                    setAllowRoleForm({
                      roleId: organizationRoles[0]?.role_id || '',
                      asId:
                        (Array.isArray(market?.access_roles) && market.access_roles[0]?.as_id) ||
                        availableMarketAccess[0]?.as_code ||
                        '',
                    });
                    openAdminPanel('allow-role');
                  }}
                >
                  Designate role
                </button>
                <button
                  type="button"
                  className="ui-action-button ui-action-button--secondary"
                  onClick={() => {
                    setMarketRuleForm({
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
                  className="ui-action-button ui-action-button--primary"
                  onClick={() => {
                    setResolveMarketForm({ outcome: 'YES' });
                    openAdminPanel('resolve-market');
                  }}
                >
                  Resolve
                </button>
              </div>
            </section>
          )}
        </div>
        {activeAdminPanel === 'edit-market' && (
          <InlineActionPanel
            title="Edit market"
            description="Update the market question from the same screen where you monitor and trade it."
            onSubmit={(event) => {
              event.preventDefault();
              handleRenameMarket();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Save market"
            submitDisabled={!editMarketForm.question.trim()}
          >
            <label data-span="full">
              Market question
              <input
                type="text"
                value={editMarketForm.question}
                onChange={(event) => setEditMarketForm({ question: event.target.value })}
              />
            </label>
          </InlineActionPanel>
        )}
        {activeAdminPanel === 'add-token' && (
          <InlineActionPanel
            title="Add market token"
            description="Choose which organization token can be used in this market."
            onSubmit={(event) => {
              event.preventDefault();
              submitAddMarketToken();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Add token"
            submitDisabled={!marketTokenId}
          >
            <label data-span="full">
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
          </InlineActionPanel>
        )}
        {activeAdminPanel === 'allow-role' && (
          <InlineActionPanel
            title="Designate market role"
            description="Choose an organization role, then designate it as Better, Analyzer, or Viewer for this market."
            onSubmit={(event) => {
              event.preventDefault();
              handleAllowMarketRole();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Save designation"
            submitDisabled={!allowRoleForm.roleId || !allowRoleForm.asId.trim()}
          >
            <label>
              Role
              <select
                value={allowRoleForm.roleId}
                onChange={(event) =>
                  setAllowRoleForm((current) => ({ ...current, roleId: event.target.value }))
                }
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
            <label>
              Designation
              <select
                value={allowRoleForm.asId}
                onChange={(event) =>
                  setAllowRoleForm((current) => ({ ...current, asId: event.target.value }))
                }
              >
                <option value="" disabled>
                  Select market access
                </option>
                {availableMarketAccess.map((option) => (
                  <option key={option.as_code} value={option.as_code}>
                    {formatMarketAccessOption(option)}
                  </option>
                ))}
              </select>
            </label>
          </InlineActionPanel>
        )}
        {activeAdminPanel === 'add-rule' && (
          <InlineActionPanel
            title="Add market rule"
            description="Attach a constraint id and value without a modal interruption."
            onSubmit={(event) => {
              event.preventDefault();
              handleAddMarketRule();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Add rule"
            submitDisabled={!marketRuleForm.constraintId || !marketRuleForm.value}
          >
            <label>
              Constraint
              <select
                value={marketRuleForm.constraintId}
                onChange={(event) =>
                  setMarketRuleForm((current) => ({ ...current, constraintId: event.target.value }))
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
                value={marketRuleForm.value}
                onChange={(event) =>
                  setMarketRuleForm((current) => ({ ...current, value: event.target.value }))
                }
              />
            </label>
          </InlineActionPanel>
        )}
        {activeAdminPanel === 'resolve-market' && (
          <InlineActionPanel
            title="Resolve market"
            description="Finalize the result here so users never have to bounce through prompts."
            onSubmit={(event) => {
              event.preventDefault();
              handleResolveMarket();
            }}
            onCancel={closeAdminPanel}
            submitLabel="Resolve market"
            submitDisabled={!resolveMarketForm.outcome}
          >
            <label data-span="full">
              Outcome
              <select
                value={resolveMarketForm.outcome}
                onChange={(event) => setResolveMarketForm({ outcome: event.target.value })}
              >
                <option value="YES">YES</option>
                <option value="NO">NO</option>
              </select>
            </label>
          </InlineActionPanel>
        )}
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
              <li>Access role: {accessLevelLabel}</li>
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
                <button
                  type="submit"
                  className="ui-action-button ui-action-button--primary"
                  disabled={tradeSubmitting || !tradeForm.tokenId}
                >
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
    </section>
  );
}
