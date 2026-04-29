import './Organization.css';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import OrganizationMembership from './OrganizationMembership';
import InlineActionPanel from './InlineActionPanel';
import { readJson, submitAndAwaitV2Operation } from '../lib/api';
import { getStoredUserId } from '../lib/auth';
import { formatRoleOption } from '../lib/policyOptions';

function formatMemberLabel(member) {
  const fullName = [member?.first, member?.last].filter(Boolean).join(' ').trim();
  if (fullName && member?.username) return `${fullName} (@${member.username})`;
  if (fullName) return fullName;
  if (member?.username) return `@${member.username}`;
  return `User #${member?.user_id ?? ''}`;
}

function Organization() { 
  const navigate = useNavigate();
  const { organizationId } = useParams();
  const normalizedOrganizationId = (() => {
    const parsed = Number(organizationId);
    return Number.isFinite(parsed) ? parsed : null;
  })();
  const [orgData, setOrgData] = useState(null);
  const [orgLoading, setOrgLoading] = useState(false);
  const [events, setEvents] = useState([]);
  const [eventsLoading, setEventsLoading] = useState(false);
  const [numParticipants, setNumParticipants] = useState(0);
  const [numEvents, setNumEvents] = useState(0);
  const [numMarkets, setNumMarkets] = useState(0);
  const [totalVolume, setTotalVolume] = useState(0);
  const [numParticipantsLoading, setNumParticipantsLoading] = useState(false);
  const [numEventsLoading, setNumEventsLoading] = useState(false);
  const [numMarketsLoading, setNumMarketsLoading] = useState(false);
  const [adminError, setAdminError] = useState(null);
  const [activeAdminPanel, setActiveAdminPanel] = useState(null);
  const [createEventForm, setCreateEventForm] = useState({ name: '' });
  const [editOrganizationForm, setEditOrganizationForm] = useState({ name: '', description: '' });
  const [createRoleForm, setCreateRoleForm] = useState({ name: '', description: '' });
  const [createTokenForm, setCreateTokenForm] = useState({ name: '', description: '' });
  const [assignRoleForm, setAssignRoleForm] = useState({ targetUserId: '', roleId: '' });
  const [grantTokensForm, setGrantTokensForm] = useState({ targetUserId: '', tokenId: '', qty: '1' });
  const [removeMemberForm, setRemoveMemberForm] = useState({ targetUserId: '' });
  const [copiedOrgId, setCopiedOrgId] = useState(false);

  const [searchParams] = useSearchParams();
  const userId = searchParams.get('userId') || getStoredUserId();
  const organizationQuery = userId ? `?user_id=${encodeURIComponent(userId)}` : '';

  const loadOrganization = async () => {
    if (normalizedOrganizationId == null || !userId) {
      setOrgData(null);
      return;
    }
    setOrgLoading(true);
    try {
      const data = await readJson(`/organizations/${normalizedOrganizationId}${organizationQuery}`);
      setOrgData(data);
    } catch (e) {
      console.error(e);
      setOrgData(null);
    } finally {
      setOrgLoading(false);
    }
  };

  const loadOrganizationStats = async () => {
    if (normalizedOrganizationId == null || !userId) {
      setNumParticipants(0);
      setNumEvents(0);
      setNumMarkets(0);
      setTotalVolume(0);
      return;
    }
  
    setNumParticipantsLoading(true);
    setNumParticipants(Array.isArray(orgData?.members) ? orgData.members.length : 0);
    setNumParticipantsLoading(false);

    setNumEventsLoading(true);
    setNumEvents(Array.isArray(events) ? events.length : 0);
    setNumEventsLoading(false);

    setNumMarketsLoading(true);
    try {
      const marketCounts = await Promise.all(
        events.map(async (event) => {
          const markets = await readJson(
            `/events/${event.event_id}/markets?user_id=${encodeURIComponent(userId)}`
          );
          return Array.isArray(markets) ? markets.length : 0;
        })
      );
      setNumMarkets(marketCounts.reduce((sum, count) => sum + count, 0));
    } catch (e) {
      console.error(e);
      setNumMarkets(0);
    } finally {
      setNumMarketsLoading(false);
    }

    // The event-bus API does not expose organization total-volume yet.
    setTotalVolume(0);
  };

  useEffect(() => {
    loadOrganizationStats();
  }, [normalizedOrganizationId, userId, orgData, events]);

  const loadOrganizationEvents = async () => {
    if (normalizedOrganizationId == null || !userId) {
      setEvents([]);
      return;
    }
    setEventsLoading(true);
    try {
      const data = await readJson(`/organizations/${normalizedOrganizationId}/events${organizationQuery}`);
      setEvents(Array.isArray(data) ? data : []);
    } catch (e) {
      console.error(e);
      setEvents([]);
    } finally {
      setEventsLoading(false);
    }
  };

  useEffect(() => {
    setEvents([]);
    loadOrganization();
    loadOrganizationEvents();
  }, [normalizedOrganizationId, userId]);

  const stats = [
    { label: 'Active Events', value: numEvents },
    { label: 'Open Markets', value: numMarkets },
    { label: 'Participants', value: numParticipants },
    { label: 'Total Volume', value: totalVolume },
  ];

  const canManageOrganization = !!orgData?.is_leader && !!userId && normalizedOrganizationId != null;
  const canLeaveOrganization =
    !!userId && normalizedOrganizationId != null && !!orgData?.role_id && !orgData?.is_leader;
  const availableMembers = Array.isArray(orgData?.members) ? orgData.members : [];
  const availableRoles = Array.isArray(orgData?.roles) ? orgData.roles : [];
  const availableTokens = Array.isArray(orgData?.tokens) ? orgData.tokens : [];
  const removableMembers = availableMembers.filter((member) => member.role_id !== 'leader');

  const closeAdminPanel = () => {
    setActiveAdminPanel(null);
  };

  const openAdminPanel = (panel) => {
    setAdminError(null);
    setActiveAdminPanel(panel);
  };

  const refreshOrganization = async () => {
    await Promise.all([loadOrganization(), loadOrganizationEvents()]);
  };

  const handleCreateNewEvent = async () => {
    try {
      if (createEventForm.name.trim() && userId && normalizedOrganizationId != null && canManageOrganization) {
        await submitAndAwaitV2Operation('/events/lifecycle', {
          action: 'CREATE_EVENT',
          user_id: Number(userId),
          organization_id: normalizedOrganizationId,
          caption: createEventForm.name.trim(),
        });
        setCreateEventForm({ name: '' });
        closeAdminPanel();
        loadOrganizationEvents();
      } else {
        setAdminError('Only the organization owner can create a new event.');
      }
    } catch (e) {
      console.error(e);
      setAdminError(e.message || 'Error creating new event');
    }
  };

  const handleEditOrganization = async () => {
    if (!editOrganizationForm.name.trim() || !canManageOrganization) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/org/management', {
        action: 'UPDATE_ORGANIZATION',
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
        name: editOrganizationForm.name.trim(),
        description: editOrganizationForm.description.trim(),
      });
      closeAdminPanel();
      await loadOrganization();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to update organization');
    }
  };

  const handleCreateRole = async () => {
    if (!createRoleForm.name.trim() || !canManageOrganization) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/org/management', {
        action: 'CREATE_ORGANIZATION_ROLE',
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
        name: createRoleForm.name.trim(),
        desc: createRoleForm.description.trim(),
      });
      setCreateRoleForm({ name: '', description: '' });
      closeAdminPanel();
      await loadOrganization();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to create role');
    }
  };

  const handleCreateToken = async () => {
    if (!createTokenForm.name.trim() || !canManageOrganization) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/org/management', {
        action: 'CREATE_ORGANIZATION_TOKEN',
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
        token_name: createTokenForm.name.trim(),
        description: createTokenForm.description.trim(),
      });
      setCreateTokenForm({ name: '', description: '' });
      closeAdminPanel();
      await loadOrganization();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to create token');
    }
  };

  const handleAssignRole = async () => {
    if (!canManageOrganization) return;
    setAssignRoleForm({
      targetUserId: String(availableMembers[0]?.user_id || ''),
      roleId: availableRoles[0]?.role_id || '',
    });
    openAdminPanel('assign-role');
  };

  const submitAssignRole = async () => {
    if (!assignRoleForm.targetUserId || !assignRoleForm.roleId || !canManageOrganization) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/org/management', {
        action: 'CREATE_ORGANIZATION_MEMBER',
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
        target_user_id: Number(assignRoleForm.targetUserId),
        role_id: assignRoleForm.roleId,
      });
      closeAdminPanel();
      await loadOrganization();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to assign role');
    }
  };

  const handleGrantTokens = async () => {
    if (!canManageOrganization) return;
    setGrantTokensForm({
      targetUserId: String(availableMembers[0]?.user_id || ''),
      tokenId: String(availableTokens[0]?.token_id || ''),
      qty: '1',
    });
    openAdminPanel('grant-tokens');
  };

  const submitGrantTokens = async () => {
    if (
      !grantTokensForm.targetUserId ||
      !grantTokensForm.tokenId ||
      !grantTokensForm.qty ||
      !canManageOrganization
    ) {
      return;
    }
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/org/management', {
        action: 'GRANT_ORGANIZATION_TOKEN',
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
        token_id: Number(grantTokensForm.tokenId),
        target_user_id: Number(grantTokensForm.targetUserId),
        qty: Number(grantTokensForm.qty),
      });
      closeAdminPanel();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to grant tokens');
    }
  };

  const handleRemoveMember = async () => {
    if (!canManageOrganization) return;
    setRemoveMemberForm({
      targetUserId: String(removableMembers[0]?.user_id || ''),
    });
    openAdminPanel('remove-member');
  };

  const submitRemoveMember = async () => {
    if (!removeMemberForm.targetUserId || !canManageOrganization) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/org/management', {
        action: 'REMOVE_ORGANIZATION_MEMBER',
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
        target_user_id: Number(removeMemberForm.targetUserId),
      });
      closeAdminPanel();
      await refreshOrganization();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to remove member');
    }
  };

  const submitLeaveOrganization = async () => {
    if (!canLeaveOrganization) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/org/management', {
        action: 'LEAVE_ORGANIZATION',
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
      });
      closeAdminPanel();
      navigate(`/dashboard${userId ? `?userId=${userId}` : ''}`);
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to leave organization');
    }
  };

  const submitDeleteOrganization = async () => {
    if (!canManageOrganization) return;
    setAdminError(null);
    try {
      await submitAndAwaitV2Operation('/org/management', {
        action: 'DELETE_ORGANIZATION',
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
      });
      closeAdminPanel();
      navigate(`/dashboard${userId ? `?userId=${userId}` : ''}`);
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to delete organization');
    }
  };

  const handleCopyOrganizationId = async () => {
    if (normalizedOrganizationId == null || typeof navigator === 'undefined' || !navigator.clipboard) {
      return;
    }
    try {
      await navigator.clipboard.writeText(String(normalizedOrganizationId));
      setCopiedOrgId(true);
      window.setTimeout(() => setCopiedOrgId(false), 1800);
    } catch (error) {
      console.error(error);
    }
  };

  return (
    <section className="organization-page" aria-label="Organizer dashboard">
      <div className="organization-shell">
        <header className="organization-hero">
          <div className="organization-nav">
            <Link
              className="page-back-link"
              to={`/dashboard${userId ? `?userId=${userId}` : ''}`}
              aria-label="Back to dashboard"
            >
              <span className="page-back-link__arrow" aria-hidden="true">
                {'<'}
              </span>
              <span className="page-back-link__label">Dashboard</span>
            </Link>
          </div>
          <p className="organization-kicker">Organizer View</p>
          <h1>
            {orgLoading
              ? 'Loading organization...'
              : orgData?.name
                ? orgData.name
                : 'Organization Dashboard'}
          </h1>
          <p>
          {orgData?.description
            ? orgData.description
            : 'Manage events, monitor market activity, and keep your organization aligned with transparent forecasting.'}
          </p>
          {normalizedOrganizationId != null && (
            <div className="organization-share-id" role="note" aria-label="Organization join id">
              <span className="organization-share-id__label">Share Organization ID</span>
              <div className="organization-share-id__row">
                <code className="organization-share-id__value">{normalizedOrganizationId}</code>
                <button
                  type="button"
                  className="organization-share-id__button"
                  onClick={handleCopyOrganizationId}
                >
                  {copiedOrgId ? 'Copied' : 'Copy'}
                </button>
              </div>
              <p className="organization-share-id__hint">
                Members can use this ID when joining the organization.
              </p>
            </div>
          )}
          {userId ? (
            <OrganizationMembership organizationId={normalizedOrganizationId} userId={userId} />
          ) : (
            <div className="organization-membership organization-membership--guest" role="note">
              <span className="organization-membership__label">Your role</span>
              <p className="organization-membership__value">
                <Link to="/signin">Sign in</Link>
                {' '}
                and open this page from your dashboard (with your user id) to see whether you are a
                leader or member.
              </p>
            </div>
          )}
          <div className="organization-action-groups">
            <section className="organization-action-group">
              <div className="organization-action-group__header">
                <span>Organization actions</span>
                <p>Everyday actions for working inside this organization.</p>
              </div>
              <div className="organization-actions ui-action-bar">
                {canLeaveOrganization && (
                  <button
                    type="button"
                    className="ui-action-button ui-action-button--ghost"
                    onClick={() => openAdminPanel('leave-organization')}
                  >
                    Leave organization
                  </button>
                )}
              </div>
            </section>
            {canManageOrganization && (
              <section className="organization-action-group organization-action-group--owner">
                <div className="organization-action-group__header">
                  <span>Owner actions</span>
                  <p>Organization setup, events, access control, and administrative tools.</p>
                </div>
                <div className="organization-actions ui-action-bar">
                  <button
                    type="button"
                    className="ui-action-button ui-action-button--primary"
                    onClick={() => {
                      setCreateEventForm({ name: '' });
                      openAdminPanel('create-event');
                    }}
                  >
                    Create new event
                  </button>
                  <button
                    type="button"
                    className="ui-action-button ui-action-button--secondary"
                    onClick={() => {
                      setEditOrganizationForm({
                        name: orgData?.name || '',
                        description: orgData?.description || '',
                      });
                      openAdminPanel('edit-organization');
                    }}
                  >
                    Edit org
                  </button>
                  <button
                    type="button"
                    className="ui-action-button ui-action-button--secondary"
                    onClick={() => {
                      setCreateRoleForm({ name: '', description: '' });
                      openAdminPanel('create-role');
                    }}
                  >
                    Create role
                  </button>
                  <button
                    type="button"
                    className="ui-action-button ui-action-button--secondary"
                    onClick={() => {
                      setCreateTokenForm({ name: '', description: '' });
                      openAdminPanel('create-token');
                    }}
                  >
                    Create token
                  </button>
                  <button
                    type="button"
                    className="ui-action-button ui-action-button--secondary"
                    onClick={handleAssignRole}
                  >
                    Assign role
                  </button>
                  <button
                    type="button"
                    className="ui-action-button ui-action-button--secondary"
                    onClick={handleGrantTokens}
                  >
                    Grant tokens
                  </button>
                  <button
                    type="button"
                    className="ui-action-button ui-action-button--secondary"
                    onClick={handleRemoveMember}
                  >
                    Remove member
                  </button>
                  <button
                    type="button"
                    className="ui-action-button ui-action-button--ghost"
                    onClick={() => openAdminPanel('delete-organization')}
                  >
                    Delete org
                  </button>
                </div>
              </section>
            )}
          </div>
          {activeAdminPanel === 'create-event' && (
            <InlineActionPanel
              title="Create new event"
              description="Add a new event directly from the organization page."
              onSubmit={(event) => {
                event.preventDefault();
                handleCreateNewEvent();
              }}
              onCancel={closeAdminPanel}
              submitLabel="Create event"
              submitDisabled={!createEventForm.name.trim()}
            >
              <label data-span="full">
                Event name
                <input
                  type="text"
                  value={createEventForm.name}
                  onChange={(event) => setCreateEventForm({ name: event.target.value })}
                  placeholder="2026 UND Spring Forecast Challenge"
                />
              </label>
            </InlineActionPanel>
          )}
          {activeAdminPanel === 'edit-organization' && (
            <InlineActionPanel
              title="Edit organization"
              description="Update the organization name and description in place."
              onSubmit={(event) => {
                event.preventDefault();
                handleEditOrganization();
              }}
              onCancel={closeAdminPanel}
              submitLabel="Save changes"
              submitDisabled={!editOrganizationForm.name.trim()}
            >
              <label>
                Organization name
                <input
                  type="text"
                  value={editOrganizationForm.name}
                  onChange={(event) =>
                    setEditOrganizationForm((current) => ({ ...current, name: event.target.value }))
                  }
                />
              </label>
              <label data-span="full">
                Description
                <textarea
                  value={editOrganizationForm.description}
                  onChange={(event) =>
                    setEditOrganizationForm((current) => ({
                      ...current,
                      description: event.target.value,
                    }))
                  }
                />
              </label>
            </InlineActionPanel>
          )}
          {activeAdminPanel === 'create-role' && (
            <InlineActionPanel
              title="Create role"
              description="Set up a new role without leaving the organization workspace."
              onSubmit={(event) => {
                event.preventDefault();
                handleCreateRole();
              }}
              onCancel={closeAdminPanel}
              submitLabel="Create role"
              submitDisabled={!createRoleForm.name.trim()}
            >
              <label>
                Role name
                <input
                  type="text"
                  value={createRoleForm.name}
                  onChange={(event) =>
                    setCreateRoleForm((current) => ({ ...current, name: event.target.value }))
                  }
                />
              </label>
              <label data-span="full">
                Role description
                <textarea
                  value={createRoleForm.description}
                  onChange={(event) =>
                    setCreateRoleForm((current) => ({ ...current, description: event.target.value }))
                  }
                />
              </label>
            </InlineActionPanel>
          )}
          {activeAdminPanel === 'create-token' && (
            <InlineActionPanel
              title="Create token"
              description="Add a new organization token in the same management area."
              onSubmit={(event) => {
                event.preventDefault();
                handleCreateToken();
              }}
              onCancel={closeAdminPanel}
              submitLabel="Create token"
              submitDisabled={!createTokenForm.name.trim()}
            >
              <label>
                Token name
                <input
                  type="text"
                  value={createTokenForm.name}
                  onChange={(event) =>
                    setCreateTokenForm((current) => ({ ...current, name: event.target.value }))
                  }
                />
              </label>
              <label data-span="full">
                Token description
                <textarea
                  value={createTokenForm.description}
                  onChange={(event) =>
                    setCreateTokenForm((current) => ({ ...current, description: event.target.value }))
                  }
                />
              </label>
            </InlineActionPanel>
          )}
          {activeAdminPanel === 'assign-role' && (
            <InlineActionPanel
              title="Assign role"
              description="Choose a member and assign one of the organization roles."
              onSubmit={(event) => {
                event.preventDefault();
                submitAssignRole();
              }}
              onCancel={closeAdminPanel}
              submitLabel="Assign role"
              submitDisabled={!assignRoleForm.targetUserId || !assignRoleForm.roleId}
            >
              <label>
                Member
                <select
                  value={assignRoleForm.targetUserId}
                  onChange={(event) =>
                    setAssignRoleForm((current) => ({ ...current, targetUserId: event.target.value }))
                  }
                >
                  <option value="" disabled>
                    Select a member
                  </option>
                  {availableMembers.map((member) => (
                    <option key={`${member.user_id}-${member.role_id || 'member'}`} value={String(member.user_id)}>
                      {formatMemberLabel(member)}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Role
                <select
                  value={assignRoleForm.roleId}
                  onChange={(event) =>
                    setAssignRoleForm((current) => ({ ...current, roleId: event.target.value }))
                  }
                >
                  <option value="" disabled>
                    Select a role
                  </option>
                  {availableRoles.map((role) => (
                    <option key={role.role_id} value={role.role_id}>
                      {formatRoleOption(role)}
                    </option>
                  ))}
                </select>
              </label>
            </InlineActionPanel>
          )}
          {activeAdminPanel === 'grant-tokens' && (
            <InlineActionPanel
              title="Grant tokens"
              description="Pick a member, choose a token, and grant the exact amount."
              onSubmit={(event) => {
                event.preventDefault();
                submitGrantTokens();
              }}
              onCancel={closeAdminPanel}
              submitLabel="Grant tokens"
              submitDisabled={
                !grantTokensForm.targetUserId || !grantTokensForm.tokenId || Number(grantTokensForm.qty) <= 0
              }
            >
              <label>
                Member
                <select
                  value={grantTokensForm.targetUserId}
                  onChange={(event) =>
                    setGrantTokensForm((current) => ({ ...current, targetUserId: event.target.value }))
                  }
                >
                  <option value="" disabled>
                    Select a member
                  </option>
                  {availableMembers.map((member) => (
                    <option key={`${member.user_id}-${member.role_id || 'member'}`} value={String(member.user_id)}>
                      {formatMemberLabel(member)}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Token
                <select
                  value={grantTokensForm.tokenId}
                  onChange={(event) =>
                    setGrantTokensForm((current) => ({ ...current, tokenId: event.target.value }))
                  }
                >
                  <option value="" disabled>
                    Select a token
                  </option>
                  {availableTokens.map((token) => (
                    <option key={token.token_id} value={String(token.token_id)}>
                      {token.name}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Quantity
                <input
                  type="number"
                  min="1"
                  step="1"
                  value={grantTokensForm.qty}
                  onChange={(event) =>
                    setGrantTokensForm((current) => ({ ...current, qty: event.target.value }))
                  }
                />
              </label>
            </InlineActionPanel>
          )}
          {activeAdminPanel === 'remove-member' && (
            <InlineActionPanel
              title="Remove member"
              description="Select a member to remove from the organization. Leaders cannot be removed here."
              onSubmit={(event) => {
                event.preventDefault();
                submitRemoveMember();
              }}
              onCancel={closeAdminPanel}
              submitLabel="Remove member"
              submitDisabled={!removeMemberForm.targetUserId}
            >
              <label data-span="full">
                Member
                <select
                  value={removeMemberForm.targetUserId}
                  onChange={(event) => setRemoveMemberForm({ targetUserId: event.target.value })}
                >
                  <option value="" disabled>
                    Select a member
                  </option>
                  {removableMembers.map((member) => (
                    <option key={`${member.user_id}-${member.role_id || 'member'}`} value={String(member.user_id)}>
                      {formatMemberLabel(member)}
                    </option>
                  ))}
                </select>
              </label>
            </InlineActionPanel>
          )}
          {activeAdminPanel === 'leave-organization' && (
            <InlineActionPanel
              title="Leave organization"
              description="You will lose member access to this organization after leaving."
              onSubmit={(event) => {
                event.preventDefault();
                submitLeaveOrganization();
              }}
              onCancel={closeAdminPanel}
              submitLabel="Leave organization"
            >
              <label data-span="full">
                Confirmation
                <input type="text" value="Leave this organization and return to your dashboard." readOnly />
              </label>
            </InlineActionPanel>
          )}
          {activeAdminPanel === 'delete-organization' && (
            <InlineActionPanel
              title="Delete organization"
              description="This permanently removes the organization and its related records."
              onSubmit={(event) => {
                event.preventDefault();
                submitDeleteOrganization();
              }}
              onCancel={closeAdminPanel}
              submitLabel="Delete organization"
            >
              <label data-span="full">
                Confirmation
                <input type="text" value="Delete this organization and all of its events, markets, roles, and tokens." readOnly />
              </label>
            </InlineActionPanel>
          )}
          {adminError && <p className="organization-inline-note">{adminError}</p>}
        </header>

        <section className="organization-stats" aria-label="Organization metrics">
          {stats.map((item) => (
            <article key={item.label} className="organization-stat">
              <p>{item.label}</p>
              <strong>{item.value}</strong>
            </article>
          ))}
        </section>

        <section className="organization-grid">
          <article className="organization-card">
            <h2>All Events</h2>
            {eventsLoading && <p className="organization-inline-note">Loading events...</p>}
            {!eventsLoading && events.length === 0 && (
              <p className="organization-inline-note">No events yet for this organization.</p>
            )}
            <ul>
              {events.map((event) => (
                <li key={event.event_id}>
                  <div>
                    <strong>
                      <Link to={`/organization/${normalizedOrganizationId}/events/${event.event_id}${userId ? `?userId=${userId}` : ''}`}>
                        {event.caption}
                      </Link>
                    </strong>
                  </div>
                </li>
              ))}
            </ul>
          </article>

          <article className="organization-card">
            <h2>Organizer Tasks</h2>
            <ul className="organization-checklist">
              {/* <li>Review unresolved markets before Friday</li>
              <li>Publish next week&apos;s roadmap event</li>
              <li>Approve role requests from 6 pending users</li>
              <li>Export monthly participation report</li> */}
            </ul>
            {Array.isArray(orgData?.roles) && orgData.roles.length > 0 && (
              <>
                <h3>Roles</h3>
                <ul>
                  {orgData.roles.map((role) => (
                    <li key={role.role_id}>
                      <strong>{role.role_id}</strong>
                      {role.description ? `: ${role.description}` : ''}
                    </li>
                  ))}
                </ul>
              </>
            )}
            {Array.isArray(orgData?.tokens) && orgData.tokens.length > 0 && (
              <>
                <h3>Tokens</h3>
                <ul>
                  {orgData.tokens.map((token) => (
                    <li key={token.token_id}>
                      <strong>{token.name}</strong> (#{token.token_id})
                    </li>
                  ))}
                </ul>
              </>
            )}
            {Array.isArray(orgData?.members) && orgData.members.length > 0 && (
              <>
                <h3>Members</h3>
                <ul>
                  {orgData.members.map((member) => (
                    <li key={`${member.user_id}-${member.role_id}`}>
                      {member.username} (#{member.user_id}) - {member.role_id}
                    </li>
                  ))}
                </ul>
              </>
            )}
          </article>
        </section>
      </div>
    </section>
  );
}

export default Organization;
