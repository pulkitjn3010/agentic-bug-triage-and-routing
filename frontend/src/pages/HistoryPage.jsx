import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { getTriageHistory } from '../api/bugs'
import { startTriage } from '../api/triage'

const SEV_CLS = { P0: 'sev-p0', P1: 'sev-p1', P2: 'sev-p2', P3: 'sev-p3' }
const HISTORY_CACHE_MAX_AGE_MS = 120000

let historyCache = {
  history: [],
  lastFetched: 0,
}

const InfoIcon = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ display: 'block' }}>
    <circle cx="12" cy="12" r="10" />
    <line x1="12" y1="16" x2="12" y2="12" />
    <line x1="12" y1="8" x2="12.01" y2="8" />
  </svg>
)

function InfoTooltip({ text, position = '', align = '' }) {
  return (
    <span className="tooltip-wrap">
      <span className="tooltip-icon">
        <InfoIcon />
      </span>
      <span className={`tooltip-box ${position} ${align}`}>
        {text}
      </span>
    </span>
  )
}

function confColor(val) {
  if (val >= 0.8) return 'var(--green)'
  if (val >= 0.6) return 'var(--amber)'
  return 'var(--red)'
}

function fmtDate(iso) {
  if (!iso) return '—'
  try {
    const d = new Date(iso)
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) +
      ' · ' +
      d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })
  } catch { return '—' }
}

export default function HistoryPage() {
  const [history,     setHistory]     = useState(() => historyCache.history || [])
  const [loading,     setLoading]     = useState(() => !(historyCache.history || []).length)
  const [error,       setError]       = useState('')
  const [retriagingId, setRetriagingId] = useState(null)
  const navigate = useNavigate()

  useEffect(() => {
    const hasCachedHistory = (historyCache.history || []).length > 0
    const cacheIsFresh = hasCachedHistory && (Date.now() - historyCache.lastFetched < HISTORY_CACHE_MAX_AGE_MS)

    if (cacheIsFresh) {
      setHistory(historyCache.history)
      setLoading(false)
      setError('')
      return
    }

    if (!hasCachedHistory) setLoading(true)
    setError('')
    getTriageHistory(50)
      .then((data) => {
        historyCache = {
          history: data || [],
          lastFetched: Date.now(),
        }
        setHistory(data || [])
      })
      .catch((err) => {
        console.error(err)
        setError('Unable to load triage history. Showing last known data if available.')
      })
      .finally(() => setLoading(false))
  }, [])

  const handleRetriage = async (bugId, sourceId) => {
    setRetriagingId(bugId)
    try {
      const data = await startTriage(bugId, sourceId)
      navigate(`/triage/${data.case_id}`)
    } catch (e) {
      alert('Failed to start triage: ' + (e.response?.data?.detail || e.message))
    } finally {
      setRetriagingId(null)
    }
  }

  const handleView = (caseId) => {
    if (caseId) {
      navigate(`/triage/${caseId}?from=history`)
    } else {
      alert('Result no longer cached. Please re-triage.')
    }
  }

  return (
    <div>
      <div className="page-hdr" style={{ position: 'relative' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <h1 style={{ margin: 0 }}>Triage History</h1>
          <InfoTooltip 
            text={
              <ul className="tooltip-list">
                <li><strong>Recent Runs:</strong> Logs the last 50 completed agentic triage runs.</li>
                <li><strong>Integrations Alert:</strong> If connected systems count is less than 2, some trackers are offline, and related duplicates from those sources cannot be fetched.</li>
                <li><strong>Fresh Run:</strong> Click <strong>Re-triage</strong> at any time to run a new analysis on the latest tracker data.</li>
              </ul>
            } 
            align="align-left"
          />
        </div>
        <p style={{ marginTop: 4 }}>Recent pipeline completions · last 50</p>
      </div>

      {error && (
        <div style={{
          padding: '9px 14px', marginBottom: 14,
          background: 'var(--red-lt)', border: '1px solid var(--red-bd)',
          borderRadius: 7, color: 'var(--red)', fontSize: 13,
        }}>
          {error}
        </div>
      )}

      {loading && history.length === 0 ? (
        <div className="card" style={{ textAlign: 'center', padding: '40px', color: 'var(--text3)', fontSize: 13 }}>
          Loading triage history...
        </div>
      ) : history.length === 0 ? (
        <div className="card" style={{ textAlign: 'center', padding: '56px 40px' }}>
          <p style={{ margin: '0 0 6px', fontSize: 14, color: 'var(--text2)', fontWeight: 600 }}>
            No triage history yet.
          </p>
          <p style={{ margin: '0 0 20px', fontSize: 13, color: 'var(--text3)' }}>
            Triage a bug from the Bug List to see it here.
          </p>
          <button className="btn btn-teal btn-sm" onClick={() => navigate('/bugs')}>
            Go to Bug List
          </button>
        </div>
      ) : (
        <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
          <table className="hist-table">
            <thead>
              <tr>
                {['Triage ID', 'Bug ID', 'Source', 'Severity', 'Confidence', 'Root Cause', 'Duration', 'Triaged At', 'Actions'].map((h) => (
                  <th key={h} className="hist-th">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {history.map((entry) => {
                const toPercent = (s) => s == null ? 0 : s > 1 ? Math.min(Math.round(s), 100) : Math.min(Math.round(s * 100), 100)
                const confVal = entry.confidence != null ? toPercent(entry.confidence) : null
                const rootCause = (entry.root_cause || '').slice(0, 80) + ((entry.root_cause || '').length > 80 ? '…' : '')
                const triageTimeMs = new Date(entry.triaged_at).getTime()
                const isExpired = !isNaN(triageTimeMs) && (Date.now() - triageTimeMs > 120000)
                return (
                  <tr key={entry.id} className="hist-tr">
                    <td className="hist-td">
                      <span className="bt-badge">{`BT-${String(entry.id).padStart(3, '0')}`}</span>
                    </td>
                    <td className="hist-td hist-mono" style={{ color: 'var(--teal)', fontWeight: 700 }}>
                      {entry.bug_id || '—'}
                    </td>
                    <td className="hist-td">
                      {entry.source_id ? (
                        <span className={`sb ${entry.source_id.includes('github') ? 'sb-gh' : entry.source_id.includes('bugzilla') ? 'sb-bz' : entry.source_id.includes('confluence') ? 'sb-cf' : 'sb-jira'}`}>
                          {entry.source_id.includes('github') ? 'GH' : entry.source_id.includes('bugzilla') ? 'BZ' : entry.source_id.includes('confluence') ? 'CF' : 'JIRA'}
                        </span>
                      ) : <span style={{ color: 'var(--text3)' }}>—</span>}
                    </td>
                    <td className="hist-td">
                      {entry.severity
                        ? <span className={`sev ${SEV_CLS[entry.severity] || 'sev-unk'}`}>{entry.severity}</span>
                        : <span style={{ color: 'var(--text3)' }}>—</span>
                      }
                    </td>
                    <td className="hist-td">
                      {confVal != null
                        ? <span style={{ fontWeight: 700, fontFamily: 'JetBrains Mono, monospace', fontSize: 12, color: confColor(entry.confidence) }}>{confVal}%</span>
                        : <span style={{ color: 'var(--text3)' }}>—</span>
                      }
                    </td>
                    <td className="hist-td" title={entry.root_cause || ''} style={{ fontSize: 12, color: 'var(--text3)', fontStyle: 'italic', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {rootCause || '—'}
                    </td>
                    <td className="hist-td hist-mono">
                      {entry.duration_ms ? `${(entry.duration_ms / 1000).toFixed(1)}s` : '—'}
                    </td>
                    <td className="hist-td hist-mono" style={{ fontSize: 11, whiteSpace: 'nowrap' }}>
                      {fmtDate(entry.triaged_at)}
                    </td>
                    <td className="hist-td">
                      <div style={{ display: 'flex', gap: 6, flexWrap: 'nowrap' }}>
                        {isExpired ? (
                          <span style={{ fontSize: 11, color: 'var(--orange)', fontWeight: 600, padding: '4px 8px', background: 'var(--orange-lt)', borderRadius: 4, display: 'flex', alignItems: 'center' }}>Triage expired</span>
                        ) : (
                          <button
                            className="btn btn-outline btn-sm"
                            onClick={() => handleView(entry.case_id)}
                          >
                            View Results
                          </button>
                        )}
                        <button
                          className="btn btn-ghost btn-sm"
                          onClick={() => handleRetriage(entry.bug_id, entry.source_id)}
                          disabled={retriagingId === entry.bug_id}
                        >
                          {retriagingId === entry.bug_id ? '…' : 'Re-triage'}
                        </button>
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
