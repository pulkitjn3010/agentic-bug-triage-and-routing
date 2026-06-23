import { useState, useEffect } from 'react'
import { Link, useNavigate, useLocation } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'
import { getMetrics } from '../api/bugs'
import { useHelp } from '../context/HelpContext'

const NAV = [
  { to: '/dashboard', label: 'Dashboard' },
  { to: '/bugs',      label: 'Bugs' },
  { to: '/history',   label: 'History' },
  { to: '/settings',  label: 'Settings' },
]

const AsteriskSvg = () => (
  <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
    <path d="M9 2v14M2 9h14M3.4 3.4l11.2 11.2M14.6 3.4L3.4 14.6" stroke="#fff" strokeWidth="2" strokeLinecap="round"/>
  </svg>
)

const HelpIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="10" />
    <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
    <line x1="12" y1="17" x2="12.01" y2="17" />
  </svg>
)

export default function TopBar() {
  const { user, logout } = useAuth()
  const { toggleHelp } = useHelp()
  const navigate = useNavigate()
  const location = useLocation()
  const [connCount, setConnCount] = useState(0)

  useEffect(() => {
    getMetrics()
      .then((data) => setConnCount(data?.sources_online ?? 0))
      .catch(() => setConnCount(0))
  }, [location.pathname])

  const handleLogout = () => {
    logout()
    navigate('/login')
  }

  return (
    <header className="topbar">
      <div className="topbar-left">
        <Link to="/bugs" style={{ display: 'flex', alignItems: 'center', textDecoration: 'none' }}>
          <div className="logo-box">
            <AsteriskSvg />
          </div>
          <div className="logo-brand">
            <strong>Bug Triage Tool</strong>
            <small>Agentic Bug Triage System</small>
          </div>
        </Link>

        <div className="topbar-sep" />

        <nav className="nav">
          {NAV.map(({ to, label }) => (
            <Link
              key={to}
              to={to}
              className={`nav-link${location.pathname.startsWith(to) ? ' active' : ''}`}
            >
              {label}
            </Link>
          ))}
        </nav>
      </div>

      <div className="topbar-right">
        <button
          onClick={toggleHelp}
          title="Open Quick Reference Help"
          style={{
            background: 'var(--white)',
            border: '1px solid var(--border)',
            borderRadius: '8px',
            height: '32px',
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            padding: '0 10px',
            color: 'var(--text2)',
            cursor: 'pointer',
            outline: 'none',
            marginRight: '6px',
            fontSize: '12px',
            fontWeight: '600',
            transition: 'all 0.15s ease-in-out',
            boxShadow: '0 1px 3px rgba(0,0,0,0.05)'
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = 'var(--teal-lt)';
            e.currentTarget.style.color = 'var(--teal)';
            e.currentTarget.style.borderColor = 'var(--teal-bd)';
            e.currentTarget.style.transform = 'translateY(-1px)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = 'var(--white)';
            e.currentTarget.style.color = 'var(--text2)';
            e.currentTarget.style.borderColor = 'var(--border)';
            e.currentTarget.style.transform = 'none';
          }}
        >
          <HelpIcon />
          <span>Help</span>
        </button>

        <div className="sys-chip">
          <span className="sys-chip-dot" />
          {connCount} system{connCount !== 1 ? 's' : ''} connected
        </div>

        <span className="email-chip">{user?.email || '—'}</span>

        <span className="role-badge">{user?.role || 'Engineer'}</span>

        <button className="btn-logout" onClick={handleLogout}>Logout</button>
      </div>
    </header>
  )
}
