import './Organization.css';
import { Link, useParams } from 'react-router-dom';
import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import OrganizationMembership from './OrganizationMembership';
import { pollOperation, postJson, putJson, readJson, submitV2Operation } from '../lib/api';
import { getStoredUserId } from '../lib/auth';

function Organization() { 
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

  const refreshOrganization = async () => {
    await Promise.all([loadOrganization(), loadOrganizationEvents()]);
  };

  const handleCreateNewEvent = async () => {
    const eventName = prompt('Enter the name of the new event');
    try {
      if (eventName && userId && normalizedOrganizationId != null) {
        const op = await submitV2Operation('/events/lifecycle', {
          action: 'CREATE_EVENT',
          user_id: Number(userId),
          organization_id: normalizedOrganizationId,
          caption: eventName,
        });
        await pollOperation(op.operation_id, {
          headers: { 'X-Force-Leader': 'true' },
        });
        loadOrganizationEvents();
      } else {
        alert('Open a valid organization and sign in to create a new event');
      }
    } catch (e) {
      console.error(e);
      alert('Error creating new event');
    }
  };

  const handleEditOrganization = async () => {
    const nextName = window.prompt('Organization name', orgData?.name || '');
    if (!nextName || !canManageOrganization) return;
    const nextDescription = window.prompt('Organization description', orgData?.description || '') || '';
    setAdminError(null);
    try {
      await putJson(`/organizations/${normalizedOrganizationId}`, {
        user_id: Number(userId),
        name: nextName,
        description: nextDescription,
      });
      await loadOrganization();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to update organization');
    }
  };

  const handleCreateRole = async () => {
    const roleName = window.prompt('Role name');
    if (!roleName || !canManageOrganization) return;
    const desc = window.prompt('Role description') || '';
    setAdminError(null);
    try {
      await postJson('/organization-roles', {
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
        name: roleName,
        desc,
      });
      await loadOrganization();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to create role');
    }
  };

  const handleCreateToken = async () => {
    const tokenName = window.prompt('Token name');
    if (!tokenName || !canManageOrganization) return;
    const description = window.prompt('Token description') || '';
    setAdminError(null);
    try {
      await postJson('/organization-tokens', {
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
        token_name: tokenName,
        description,
      });
      await loadOrganization();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to create token');
    }
  };

  const handleAssignRole = async () => {
    const targetUserId = window.prompt('User id to assign');
    const roleId = window.prompt('Role id to assign');
    if (!targetUserId || !roleId || !canManageOrganization) return;
    setAdminError(null);
    try {
      await postJson('/organization-members', {
        user_id: Number(userId),
        organization_id: normalizedOrganizationId,
        target_user_id: Number(targetUserId),
        role_id: roleId,
      });
      await loadOrganization();
    } catch (error) {
      console.error(error);
      setAdminError(error.message || 'Failed to assign role');
    }
  };

  return (
    <section className="organization-page" aria-label="Organizer dashboard">
      <div className="organization-shell">
        <header className="organization-hero">
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
          <div className="organization-actions">
            <button type="button" onClick={handleCreateNewEvent}>Create New Event</button>
            <button type="button" className="organization-actions__secondary">
              Invite Members
            </button>
            {canManageOrganization && (
              <>
                <button type="button" className="organization-actions__secondary" onClick={handleEditOrganization}>
                  Edit Org
                </button>
                <button type="button" className="organization-actions__secondary" onClick={handleCreateRole}>
                  Create Role
                </button>
                <button type="button" className="organization-actions__secondary" onClick={handleCreateToken}>
                  Create Token
                </button>
                <button type="button" className="organization-actions__secondary" onClick={handleAssignRole}>
                  Assign Role
                </button>
              </>
            )}
          </div>
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
                      <strong>{role.role_id}</strong>: {role.description}
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
