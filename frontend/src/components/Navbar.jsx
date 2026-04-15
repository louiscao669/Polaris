import React from 'react';

const Navbar = ({ user, homeUrl }) => {
    return (
        <div>
            <nav className="navbar">
                <span>Polaris</span>
                <a href={homeUrl}>Home</a>
                <a href="/game/">Play</a>
            </nav>
        </div>
    );
};

export default Navbar;