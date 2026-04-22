// Layout.js
import Navbar from './Navbar';
import './Layout.css';

function Layout({ children, user, homeUrl }) {
  return (
    <div className="layout">
      {user?.isAuthenticated ? <Navbar user={user} homeUrl={homeUrl} /> : null}

      <div className="page-wrapper">{children}</div>
    </div>
  );
}

export default Layout;
