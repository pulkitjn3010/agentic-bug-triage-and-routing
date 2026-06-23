import { Outlet } from 'react-router-dom'
import TopBar from './TopBar'
import HelpDrawer from '../components/HelpDrawer'

export default function AppLayout() {
  return (
    <div style={{ minHeight: '100vh', background: 'var(--bg)' }}>
      <TopBar />
      <main className="page">
        <Outlet />
      </main>
      <HelpDrawer />
    </div>
  )
}
