// Layout.js
import Navbar from './Navbar';
import './Layout.css';

function Layout({ children, user, homeUrl }) {
  return (
    <div className="layout">
      <Navbar user={user} homeUrl={homeUrl} />

      <div className="page-wrapper">{children}</div>
    </div>
  );
}

export default Layout;