import './App.css'
import Layout from './components/Layout'
import Footer from './components/Footer'
import LandingPage from './components/LandingPage'
import Organization from './components/Organization'
import EventDashboard from './components/EventDashboard'
import UserDashboard from './components/UserDashboard'
import SignUp from './my_app/sign_up/SignUp';
import SignIn from './my_app/sign_in/SignIn';
import { Routes, Route } from 'react-router-dom';

function HomePage({ user }) {
  return (
    <Layout user={user} homeUrl="/">
      <LandingPage />
      <section id="signup" className="signup-section" aria-label="Sign up">
        <SignUp />
        <Footer />
      </section>
    </Layout>
  );
}

function SignUpPage({ user }) {
  return (
    <Layout user={user} homeUrl="/">
      <section className="signup-section" aria-label="Sign up">
        <SignUp />
        <Footer />
      </section>
    </Layout>
  );
}

function SignInPage({ user }) {
  return (
    <Layout user={user} homeUrl="/">
      <section className="signup-section" aria-label="Sign in">
        <SignIn />
        <Footer />
      </section>
    </Layout>
  );
}

function OrganizationPage({ user }) {
  return (
    <Layout user={user} homeUrl="/">
      <section className="signup-section" aria-label="Organization">
        <Organization />
      </section>
    </Layout>
  );
}

function UserDashboardPage({ user }) {
  return (
    <Layout user={user} homeUrl="/">
      <section className="signup-section" aria-label="User dashboard">
        <UserDashboard />
      </section>
    </Layout>
  );
}

function EventDashboardPage({ user }) {
  return (
    <Layout user={user} homeUrl="/">
      <section className="signup-section" aria-label="Event dashboard">
        <EventDashboard />
      </section>
    </Layout>
  );
}

function App() {
  const user = { isAuthenticated: true };

  return (
    <Routes>
      <Route path="/" element={<HomePage user={user} />} />
      <Route path="/signup" element={<SignUpPage user={user} />} />
      <Route path="/signin" element={<SignInPage user={user} />} />
      <Route path="/organization/:organizationId" element={<OrganizationPage user={user} />} />
      <Route path="/organization/:organizationId/events/:eventId" element={<EventDashboardPage user={user} />}/>
      <Route path="/organization" element={<OrganizationPage user={user} />} />
      <Route path="/dashboard/:organizationId" element={<UserDashboardPage user={user} />} />
      <Route path="/dashboard" element={<UserDashboardPage user={user} />} />
    </Routes>
  )
}

export default App;
