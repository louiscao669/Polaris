import './App.css'
import Layout from './components/Layout'
import Footer from './components/Footer'
import LandingPage from './components/LandingPage'
import SignUp from './my_app/sign-up/SignUp';
function App() {
  const user = { isAuthenticated: true };

  return (
    <Layout user={user} homeUrl="/">
      <LandingPage />
      <section id="signup" className="signup-section" aria-label="Sign up">
        <SignUp />
        <Footer />
      </section>
    </Layout>
  )
}

export default App;
