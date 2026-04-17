import React from 'react';
import { Link } from 'react-router-dom';

const Navbar = ({ user, homeUrl }) => {
    return (
        <nav className="navbar">
            <div className="navbar-left">
                <span className="navbar-brand">Polaris</span>
                <Link to={homeUrl}>Home</Link>
                <Link to="/organization">Organization</Link>
                <Link to="/dashboard">Dashboard</Link>
                {/* <a href="/game/">Play</a> */}
            </div>

            <div className="navbar-right">
                <Link to="/signin">Sign in</Link>
                <Link to="/signup">Sign up</Link>
            </div>
        </nav>
    );
};

export default Navbar;