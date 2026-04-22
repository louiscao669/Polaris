import './Organization.css';
import { Link, useParams } from 'react-router-dom';
import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import OrganizationMembership from './OrganizationMembership';
import { API_BASE, pollOperation, readJson, submitV2Operation } from '../lib/api';
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

  const [searchParams] = useSearchParams();
  const userId = searchParams.get('userId') || getStoredUserId();

  const loadOrganization = async () => {
    if (normalizedOrganizationId == null) {
      setOrgData(null);
      return;
    }
    setOrgLoading(true);
    try {
      const data = await readJson(`/organizations/${normalizedOrganizationId}`);
        setOrgData(data);
    } catch (e) {
      console.error(e);
      setOrgData(null);
    } finally {
      setOrgLoading(false);
    }
  };

  const loadOrganizationStats = async () => {

    if (normalizedOrganizationId == null) {
      setNumParticipants(0);
      setNumEvents(0);
      setNumMarkets(0);
      setTotalVolume(0);
      return;
    }
  
    setNumParticipantsLoading(true);
    try {
      const data = await readJson(`/dashboard/organizations/${normalizedOrganizationId}/num-participants`);
      setNumParticipants(data);
    } catch (e) {
      console.error(e);
      setNumParticipants(0);
    } finally {
      setNumParticipantsLoading(false);
    }

    setNumEventsLoading(true);
    try {
      const data = await readJson(`/dashboard/organizations/${normalizedOrganizationId}/num-events`);
      setNumEvents(data);
    } catch (e) {
      console.error(e);
      setNumEvents(0);
    } finally {
      setNumEventsLoading(false);
    }

    setNumMarketsLoading(true);
    try {
      let numMarkets = 0;
      for (const event of events) {
        const data = await readJson(`/dashboard/events/${event.event_id}/num-markets`);
        numMarkets += data;
      }
      setNumMarkets(numMarkets);
    } catch (e) {
      console.error(e);
      setNumMarkets(0);
    } finally {
      setNumMarketsLoading(false);
    }

    try {
      const data = await readJson(`/dashboard/organizations/${normalizedOrganizationId}/total-volume`);
      setTotalVolume(Number(data) || 0);
    } catch (e) {
      console.error(e);
      setTotalVolume(0);
    }
  };

  useEffect(() => {
    loadOrganizationStats();
  }, [normalizedOrganizationId, events]);

  const loadOrganizationEvents = async () => {
    if (normalizedOrganizationId == null) {
      setEvents([]);
      return;
    }
    setEventsLoading(true);
    try {
      const data = await readJson(`/organizations/${normalizedOrganizationId}/events`);
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
  }, [normalizedOrganizationId]);

  const stats = [
    { label: 'Active Events', value: numEvents },
    { label: 'Open Markets', value: numMarkets },
    { label: 'Participants', value: numParticipants },
    { label: 'Total Volume', value: totalVolume },
  ];

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
          </div>
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
          </article>
        </section>

      </div>
    </section>
  );
}

export default Organization;
