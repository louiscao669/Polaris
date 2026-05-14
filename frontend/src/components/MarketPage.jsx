import { Link, useParams, useSearchParams } from 'react-router-dom';
import { useEffect, useMemo, useState } from 'react';
import './MarketPage.css';
import InlineActionPanel from './InlineActionPanel';
import { readJson, submitAndAwaitV2Operation } from '../lib/api';
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

function formatPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return '--';
  return `${numeric}%`;
}

function formatChartTimestamp(value) {
  if (!value) return '--';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '--';
  return date.toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function buildForecastPath(points, width, height, padding) {
  if (!Array.isArray(points) || points.length === 0) return '';

  const minTimestamp = new Date(points[0]?.ts || 0).getTime();
  const maxTimestamp = new Date(points[points.length - 1]?.ts || 0).getTime();
  const hasTimeSpread = Number.isFinite(minTimestamp) && Number.isFinite(maxTimestamp) && maxTimestamp > minTimestamp;

  if (points.length === 1) {
    const y = padding + ((100 - Number(points[0]?.yes_price || 0)) / 100) * (height - padding * 2);
    return `M ${padding} ${y} L ${width - padding} ${y}`;
  }

  return points
    .map((point, index) => {
      const timestamp = new Date(point?.ts || 0).getTime();
      const ratio = hasTimeSpread
        ? Math.min(1, Math.max(0, (timestamp - minTimestamp) / (maxTimestamp - minTimestamp)))
        : index / (points.length - 1);
      const x = padding + ratio * (width - padding * 2);
      const y = padding + ((100 - Number(point?.yes_price || 0)) / 100) * (height - padding * 2);
      return `${index === 0 ? 'M' : 'L'} ${x} ${y}`;
    })
    .join(' ');
}

function buildMarketDetailPath(marketId, userId) {
  const query = new URLSearchParams({
    user_id: String(userId),
    cache_mode: 'bypass',
  });
  return `/markets/${marketId}?${query.toString()}`;
}

const FORECAST_WINDOWS = [
  { value: 'auto', label: 'Auto', hours: null },
  { value: '1h', label: '1H', hours: 1 },
  { value: '24h', label: '24H', hours: 24 },
  { value: '168h', label: '7D', hours: 168 },
  { value: 'all', label: 'All', hours: null },
];

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
  const [tradeQuote, setTradeQuote] = useState(null);
  const [tradeQuoteLoading, setTradeQuoteLoading] = useState(false);
  const [tradeQuoteError, setTradeQuoteError] = useState(null);
  const [showAnalytics, setShowAnalytics] = useState(false);
  const [analyticsLoading, setAnalyticsLoading] = useState(false);
  const [analyticsError, setAnalyticsError] = useState(null);
  const [analytics, setAnalytics] = useState(null);
  const [forecastLoading, setForecastLoading] = useState(false);
  const [forecastError, setForecastError] = useState(null);
  const [forecastSnapshot, setForecastSnapshot] = useState(null);
  const [forecastPoints, setForecastPoints] = useState([]);
  const [forecastWindow, setForecastWindow] = useState('auto');
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
  const canBet = !!market?.is_leader || roleView === 'bettor';
  const canViewAnalytics = !!market?.is_leader || roleView === 'analyzer';
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
  const constraintDetailsById = useMemo(
    () =>
      Object.fromEntries(
        availableConstraints.map((constraint) => [
          String(constraint.constraint_id),
          {
            name: constraint.name || `Constraint ${constraint.constraint_id}`,
            description: constraint.description || '',
          },
        ])
      ),
    [availableConstraints]
  );

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
        const data = await readJson(buildMarketDetailPath(marketId, userId));
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
    if (
      !canBet ||
      !Number.isFinite(numericUserId) ||
      !Number.isFinite(numericMarketId) ||
      !tradeForm.tokenId ||
      Number(tradeForm.qty) <= 0
    ) {
      setTradeQuote(null);
      setTradeQuoteError(null);
      setTradeQuoteLoading(false);
      return;
    }

    let cancelled = false;

    const loadTradeQuote = async () => {
      setTradeQuoteLoading(true);
      setTradeQuoteError(null);
      try {
        const query = new URLSearchParams({
          user_id: String(numericUserId),
          market_id: String(numericMarketId),
          token_id: String(Number(tradeForm.tokenId)),
          side: String(tradeForm.side === 'YES'),
          qty: String(Number(tradeForm.qty)),
          transaction_type: tradeForm.transactionType,
        });
        const data = await readJson(`/markets/quote?${query.toString()}`);
        if (!cancelled) {
          setTradeQuote(data);
        }
      } catch (error) {
        console.error(error);
        if (!cancelled) {
          setTradeQuote(null);
          setTradeQuoteError(error.message || 'Failed to load trade quote');
        }
      } finally {
        if (!cancelled) {
          setTradeQuoteLoading(false);
        }
      }
    };

    loadTradeQuote();
    return () => {
      cancelled = true;
    };
  }, [canBet, numericUserId, numericMarketId, tradeForm]);

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
        )}&cache_mode=bypass`;
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

  useEffect(() => {
    if (!Number.isFinite(numericUserId) || !Number.isFinite(numericMarketId)) {
      setForecastSnapshot(null);
      setForecastPoints([]);
      setForecastError(null);
      return;
    }

    let cancelled = false;

    const loadForecast = async () => {
      setForecastLoading(true);
      setForecastError(null);
      try {
        const q = `user_id=${encodeURIComponent(String(numericUserId))}&market_id=${encodeURIComponent(
          String(numericMarketId)
        )}&cache_mode=bypass`;
        const selectedWindow = FORECAST_WINDOWS.find((option) => option.value === forecastWindow) || FORECAST_WINDOWS[0];
        const pointsParams =
          selectedWindow.value === 'auto'
            ? `${q}&span=200`
            : `${q}&span=200${selectedWindow.hours ? `&hours=${selectedWindow.hours}` : ''}`;
        const [liquidity, points] = await Promise.all([
          readJson(`/markets/stats/liquidity?${q}`),
          readJson(`/markets/points?${pointsParams}`),
        ]);
        if (cancelled) return;
        setForecastSnapshot(liquidity);
        setForecastPoints(Array.isArray(points) ? points : []);
      } catch (error) {
        console.error(error);
        if (!cancelled) {
          setForecastSnapshot(null);
          setForecastPoints([]);
          setForecastError(error.message || 'Failed to load forecast');
        }
      } finally {
        if (!cancelled) {
          setForecastLoading(false);
        }
      }
    };

    loadForecast();
    return () => {
      cancelled = true;
    };
  }, [numericUserId, numericMarketId, forecastWindow]);

  const handleTradeChange = (field) => (event) => {
    setTradeForm((current) => ({ ...current, [field]: event.target.value }));
  };

  const refreshAfterTrade = async () => {
    if (!Number.isFinite(numericUserId) || !Number.isFinite(numericMarketId)) return;
    const q = `user_id=${encodeURIComponent(String(numericUserId))}&market_id=${encodeURIComponent(
      String(numericMarketId)
    )}&cache_mode=bypass`;
    const selectedWindow = FORECAST_WINDOWS.find((option) => option.value === forecastWindow) || FORECAST_WINDOWS[0];
    const pointsParams =
      selectedWindow.value === 'auto'
        ? `${q}&span=200`
        : `${q}&span=200${selectedWindow.hours ? `&hours=${selectedWindow.hours}` : ''}`;
    const [data, liquidity, points] = await Promise.all([
      readJson(buildMarketDetailPath(numericMarketId, numericUserId)),
      readJson(`/markets/stats/liquidity?${q}`),
      readJson(`/markets/points?${pointsParams}`),
    ]);
    setMarket(data);
    setForecastSnapshot(liquidity);
    setForecastPoints(Array.isArray(points) ? points : []);
    if (showAnalytics && canViewAnalytics) {
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

  const chartPath = buildForecastPath(forecastPoints, 360, 180, 18);
  const latestForecastPoint =
    forecastPoints.length > 0 ? forecastPoints[forecastPoints.length - 1] : null;
  const yesForecast = latestForecastPoint?.yes_price ?? forecastSnapshot?.yes_price;
  const noForecast = latestForecastPoint?.no_price ?? forecastSnapshot?.no_price;
  const forecastStartLabel = forecastPoints[0]?.ts ? formatChartTimestamp(forecastPoints[0].ts) : 'Start';
  const forecastEndLabel =
    forecastPoints.length > 0 ? formatChartTimestamp(forecastPoints[forecastPoints.length - 1].ts) : 'Now';
  const forecastWindowLabel =
    (FORECAST_WINDOWS.find((option) => option.value === forecastWindow) || FORECAST_WINDOWS[0]).label;
  const tradeQuoteSummary = tradeQuote
    ? `${tradeQuote.transaction_type === 'BUY' ? 'Buying' : 'Selling'} ${tradeQuote.qty} ${
        tradeQuote.side ? 'YES' : 'NO'
      } ticket${tradeQuote.qty === 1 ? '' : 's'} for ${tradeQuote.total_token_value} token${
        tradeQuote.total_token_value === 1 ? '' : 's'
      }.`
    : null;

  const handleSubmitTrade = async (event) => {
    event.preventDefault();
    if (!Number.isFinite(numericUserId) || !Number.isFinite(numericMarketId) || !tradeForm.tokenId) return;

    setTradeSubmitting(true);
    setTradeError(null);
    try {
      await submitAndAwaitV2Operation('/markets/transactions', {
        action: 'MARKET_TRANSACTION',
        user_id: numericUserId,
        market_id: numericMarketId,
        token_id: Number(tradeForm.tokenId),
        side: tradeForm.side === 'YES',
        qty: Number(tradeForm.qty),
        transaction_id: Date.now(),
        transaction_type: tradeForm.transactionType,
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
      await submitAndAwaitV2Operation('/markets/lifecycle', {
        action: 'UPDATE_MARKET',
        user_id: Number(userId),
        market_id: Number(marketId),
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
      await submitAndAwaitV2Operation('/markets/lifecycle', {
        action: 'DESIGNATE_MARKET_TOKEN',
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
      await submitAndAwaitV2Operation('/markets/lifecycle', {
        action: 'DESIGNATE_MARKET_OPEN_TO_AS',
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
      await submitAndAwaitV2Operation('/markets/lifecycle', {
        action: 'DESIGNATE_MARKET_RESULT',
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
      await submitAndAwaitV2Operation('/markets/lifecycle', {
        action: 'DESIGNATE_MARKET_CONSTRAINT',
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

        <section className="market-primary">
          <article className="market-card market-card--forecast market-card--hero">
            <h2>Current Forecast</h2>
            {forecastLoading && <p className="market-muted">Loading forecast...</p>}
            {forecastError && <p className="market-error">{forecastError}</p>}
            {!forecastLoading && !forecastError && (
              <>
                <div className="market-forecast-pills">
                  <div className="market-forecast-pill market-forecast-pill--yes">
                    <span>YES</span>
                    <strong>{formatPercent(yesForecast)}</strong>
                  </div>
                  <div className="market-forecast-pill market-forecast-pill--no">
                    <span>NO</span>
                    <strong>{formatPercent(noForecast)}</strong>
                  </div>
                </div>
                <div className="market-forecast-toolbar">
                  <div className="market-forecast-toolbar__copy">
                    <span>History window</span>
                    <strong>{forecastWindowLabel}</strong>
                  </div>
                  <label className="market-forecast-toolbar__select">
                    <span className="sr-only">Choose forecast time range</span>
                    <select value={forecastWindow} onChange={(event) => setForecastWindow(event.target.value)}>
                      {FORECAST_WINDOWS.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <div className="market-forecast-chart" role="img" aria-label="Forecast trend">
                  {chartPath ? (
                    <svg viewBox="0 0 360 180" preserveAspectRatio="none">
                      <defs>
                        <linearGradient id="forecastLine" x1="0%" x2="100%" y1="0%" y2="0%">
                          <stop offset="0%" stopColor="#7dd3fc" />
                          <stop offset="100%" stopColor="#34d399" />
                        </linearGradient>
                      </defs>
                      <path className="market-forecast-chart__grid" d="M 18 18 L 18 162 L 342 162" />
                      <path className="market-forecast-chart__line" d={chartPath} />
                    </svg>
                  ) : (
                    <div className="market-forecast-chart__empty">No trading history yet.</div>
                  )}
                </div>
                <div className="market-forecast-axis">
                  <span>{forecastStartLabel}</span>
                  <span>{forecastEndLabel}</span>
                </div>
                <p className="market-muted">
                  {forecastSnapshot
                    ? `Pool ${forecastSnapshot.total_pool} · Open tickets ${forecastSnapshot.open_tickets} · Trades ${forecastSnapshot.trade_count}`
                    : 'Forecast updates will appear here as trading activity comes in.'}
                </p>
              </>
            )}
          </article>

          <article className="market-card market-card--betting market-card--hero">
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
                {tradeQuoteLoading && <p className="market-trade-quote">Updating trade quote...</p>}
                {!tradeQuoteLoading && tradeQuoteSummary && (
                  <p className="market-trade-quote">
                    {tradeQuoteSummary} Average price {tradeQuote.average_price} per ticket.
                  </p>
                )}
                <p className="market-trade-disclaimer">
                  Prices can change in real time as other trades come in. Refresh the quote right before you place your trade.
                </p>
                {!tradeQuoteLoading && tradeQuoteError && <p className="market-error">{tradeQuoteError}</p>}
                {tradeError && <p className="market-error">{tradeError}</p>}
              </form>
            )}
          </article>
        </section>

        <section className="market-supporting">
          <article className="market-card">
            <h2>Rules</h2>
            {!Array.isArray(market?.constraints) || market.constraints.length === 0 ? (
              <p className="market-muted">No market rules have been attached yet.</p>
            ) : (
              <ul className="market-list">
                {market.constraints.map((constraint) => {
                  const details = constraintDetailsById[String(constraint.constraint_id)];
                  const name = details?.name || `Constraint #${constraint.constraint_id}`;
                  const description = details?.description;
                  return (
                    <li key={`${constraint.constraint_id}-${constraint.value}`}>
                      <div>
                        <strong>{name}</strong>
                        <span>
                          Limit: {constraint.value}
                          {description ? ` · ${description}` : ''}
                        </span>
                      </div>
                    </li>
                  );
                })}
              </ul>
            )}
          </article>

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
