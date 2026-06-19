import { useState, useEffect, useRef } from 'react'
import { useHelp } from '../context/HelpContext'

export default function HelpDrawer() {
  const {
    isHelpOpen,
    activeSection,
    setActiveSection,
    searchTerm,
    setSearchTerm,
    highlightedTerm,
    setHighlightedTerm,
    closeHelp,
  } = useHelp()

  // Track expanded accordion sections
  const [expandedSections, setExpandedSections] = useState({})
  const glossaryRefs = useRef({})

  // Reset search and scroll states when drawer opens/closes
  useEffect(() => {
    if (isHelpOpen) {
      setExpandedSections(
        activeSection ? { [activeSection]: true } : {}
      )
    } else {
      setExpandedSections({})
      setSearchTerm('')
    }
  }, [isHelpOpen, activeSection, setSearchTerm])

  // Handle jump-to-glossary navigation
  const handleGlossaryJump = (e, termId) => {
    e.preventDefault()
    setSearchTerm('') // Clear search so Glossary is visible
    setExpandedSections((prev) => ({ ...prev, glossary: true }))
    setHighlightedTerm(termId)

    setTimeout(() => {
      const el = document.getElementById(termId)
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' })
      }
    }, 150)
  };

  const toggleSection = (id) => {
    setExpandedSections((prev) => ({
      ...prev,
      [id]: !prev[id],
    }))
  }

  // Quick search keywords
  const handleTagClick = (tag) => {
    setSearchTerm(tag)
  }

  if (!isHelpOpen) return null

  // Complete documentation content structured in 10 items
  const helpData = [
    {
      id: 'getting-started',
      title: '1. Getting Started',
      keywords: ['login', 'role', 'logout', 'engineer', 'access', 'auth'],
      content: (
        <div>
          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>What is this Bug Triage Tool?</h4>
          <p style={{ margin: '0 0 12px' }}>
            This bug triage tool is an enterprise-grade <strong>Agentic Bug Triage & Investigation Platform</strong>. It automates duplicate issue discovery, cross-references internal and external knowledge bases, and aggregates data from multiple project management tools without storing code or sensitive logs permanently.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Platform Access & Permissions</h4>
          <p style={{ margin: '0 0 12px' }}>
            The platform is tailored for Engineers. As an Engineer, you have access to investigate and triage bugs, trigger automated triage pipelines, inspect duplicate issue clusters, and audit historic triage runs.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Log In (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: '0 0 12px' }}>
            <li style={{ marginBottom: '4px' }}>Open your browser and navigate to the Bug Triage Tool URL (e.g., <code>http://localhost:5173</code>).</li>
            <li style={{ marginBottom: '4px' }}>On the Login page, enter your corporate email address.</li>
            <li style={{ marginBottom: '4px' }}>In the Password field, enter your password.</li>
            <li style={{ marginBottom: '4px' }}>Click the green <strong>Login</strong> button.</li>
            <li style={{ marginBottom: '4px' }}>Upon verification, your active session is managed by a secure, encrypted <a href="#" onClick={(e) => handleGlossaryJump(e, 'term-jwt')}>JWT</a> token saved in session cookies, and you will be redirected to the bugs list.</li>
          </ol>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Log Out (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: 0 }}>
            <li style={{ marginBottom: '4px' }}>Locate the <strong>Logout</strong> button in the top right corner of the header.</li>
            <li style={{ marginBottom: '4px' }}>Click <strong>Logout</strong> to terminate your active token immediately.</li>
            <li style={{ marginBottom: '4px' }}>This action clears the session cookies and redirects your browser back to the login page.</li>
          </ol>
        </div>
      ),
    },
    {
      id: 'bug-list',
      title: '2. Bug List Page',
      keywords: ['discover', 'discovered', 'strip', 'refresh', 'filter', 'badge', 'severity', 'status', 'open', 'pills'],
      content: (
        <div>
          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Auto-Discovery Engine</h4>
          <p style={{ margin: '0 0 12px' }}>
            This bug triage tool fetches tickets in real-time from all connected issue trackers (GitHub, Jira, Bugzilla). Bug lists are not stored locally; instead, they are held in a shared <a href="#" onClick={(e) => handleGlossaryJump(e, 'term-cache')}>Redis Cache</a> to minimize external network requests.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Understanding the Dashboard Strip</h4>
          <ul style={{ paddingLeft: '16px', margin: '0 0 12px' }}>
            <li style={{ marginBottom: '4px' }}><strong>🔴 P0 / 🟠 P1:</strong> High-severity bugs discovered and active in source repositories.</li>
            <li style={{ marginBottom: '4px' }}><strong>✅ Triaged Today:</strong> Total unique triage actions ran by you during the current UTC day.</li>
            <li style={{ marginBottom: '4px' }}><strong>⏳ Needs Triage:</strong> Discovered bugs that do not have an active triage record in the database.</li>
          </ul>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Badges & Status Codes</h4>
          <p style={{ margin: '0 0 12px' }}>
            Source tags are color-coded (GitHub is purple, Jira is blue, Bugzilla is amber, Confluence is teal). Severity ratings range from critical <strong>P0</strong> down to low-priority <strong>P3</strong>. Workflow pills (e.g. Open, In Progress, Blocked) match the external tracker's status.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Search for Specific Bugs (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: '0 0 12px' }}>
            <li style={{ marginBottom: '4px' }}>Locate the <strong>Search box</strong> in the filter bar at the top of the Bugs page.</li>
            <li style={{ marginBottom: '4px' }}>Type a bug ID (e.g., <code>56515</code> or <code>GH-41</code>) or a keyword from the ticket title.</li>
            <li style={{ marginBottom: '4px' }}>The bug list will filter in real-time. To clear search filters, click the <strong>✕</strong> button inside the input field.</li>
          </ol>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Filter by Category & Status (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: '0 0 12px' }}>
            <li style={{ marginBottom: '4px' }}>Locate the dropdown selectors (<strong>Source</strong>, <strong>Severity</strong>, <strong>Status</strong>, <strong>Project</strong>) in the filter bar.</li>
            <li style={{ marginBottom: '4px' }}>Click any dropdown and select a criteria (e.g., Severity: <strong>P0</strong>).</li>
            <li style={{ marginBottom: '4px' }}>You can select multiple filters at once to combine criteria.</li>
          </ol>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Force a Live Sync (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: 0 }}>
            <li style={{ marginBottom: '4px' }}>Redis caches external ticket lists with a <a href="#" onClick={(e) => handleGlossaryJump(e, 'term-ttl')}>TTL</a> of <strong>120 seconds (2 minutes)</strong>.</li>
            <li style={{ marginBottom: '4px' }}>To bypass this cache and load the latest changes immediately, locate and click the manual <strong>Refresh</strong> button next to the filter bar.</li>
            <li style={{ marginBottom: '4px' }}>This action clears the Redis cache and requests fresh tickets directly from all connected trackers.</li>
          </ol>
        </div>
      ),
    },
    {
      id: 'tree-view',
      title: '3. Tree View & Triage States',
      keywords: ['group', 'duplicate', 'similarity', 'match', 'percent', 'excellent', 'good', 'fair'],
      content: (
        <div>
          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Flat Rows vs. Tree Groups</h4>
          <p style={{ margin: '0 0 12px' }}>
            Un-triaged bugs display as flat, standalone items. Once triaged, if the system discovers duplicate or semantic matches, they are grouped under a tree root. Clicking the root arrow expands the panel to show all related child bugs.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>BT-XXX Case IDs</h4>
          <p style={{ margin: '0 0 12px' }}>
            When a bug group is established in the registry, it is assigned a tool-level ID (e.g. <a href="#" onClick={(e) => handleGlossaryJump(e, 'term-btid')}>BT-001</a>).
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Similarity Scores & Match Labels</h4>
          <p style={{ margin: '0 0 12px' }}>
            Calculated as a percentage representing textual and semantic overlap (0.0 to 1.0):
            <br />• <strong>Excellent:</strong> &ge; 90% match.
            <br />• <strong>Good:</strong> &ge; 75% match.
            <br />• <strong>Fair:</strong> &ge; 50% match.
            <br /><em>Bugs matching below 50% are automatically omitted from tree groups.</em>
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Inspect and Expand Bug Groups (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: 0 }}>
            <li style={{ marginBottom: '4px' }}>On the Bugs page, look for items that have a folder/arrow symbol (these are grouped bugs under a <code>BT-XXX</code> ID).</li>
            <li style={{ marginBottom: '4px' }}>Click anywhere on the root row or on the arrow icon on the left side of the row.</li>
            <li style={{ marginBottom: '4px' }}>The group will expand downwards to show all matching child bugs, including their similarity ratings and details.</li>
            <li style={{ marginBottom: '4px' }}>Click the arrow again to collapse the group and simplify your view.</li>
          </ol>
        </div>
      ),
    },
    {
      id: 'running-triage',
      title: '4. Running a Triage',
      keywords: ['pipeline', 'agents', 'context', 'duration', 'confidence', 'synthesize', 'results', 'temporary'],
      content: (
        <div>
          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>The 4 Investigation Phases</h4>
          <p style={{ margin: '0 0 12px' }}>
            The process takes roughly <strong>30–40 seconds on average</strong> (depending on the speed of external issue trackers and the AI model server) and calls four autonomous AI agents in sequence:
            <br />1. <a href="#" onClick={(e) => handleGlossaryJump(e, 'agent-context')}>ContextFetchAgent</a>: Collects logs, comments, and stack traces.
            <br />2. <a href="#" onClick={(e) => handleGlossaryJump(e, 'agent-cross')}>CrossSystemFetchAgent</a>: Scans external connectors for duplicates.
            <br />3. <a href="#" onClick={(e) => handleGlossaryJump(e, 'agent-enrich')}>EnrichmentAgent</a>: Queries Confluence spaces and Customer portals.
            <br />4. <a href="#" onClick={(e) => handleGlossaryJump(e, 'agent-synthesis')}>AISynthesisAgent</a>: Synthesizes final root causes, recommends severity, and tags responsible teams.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>The 4 Triage Panels</h4>
          <ul style={{ paddingLeft: '16px', margin: '0 0 12px' }}>
            <li style={{ marginBottom: '4px' }}><strong>Panel 1 — Bug Context:</strong> Raw descriptions, ticket status, and linked customer issues.</li>
            <li style={{ marginBottom: '4px' }}><strong>Panel 2 — Related Issues:</strong> Cross-system duplicates with similarity bars and matching reasons.</li>
            <li style={{ marginBottom: '4px' }}><strong>Panel 3 — Linked Context:</strong> Linked Confluence runbooks, wiki guides, and KB articles.</li>
            <li style={{ marginBottom: '4px' }}><strong>Panel 4 — AI Summary:</strong> The final synthesized root cause, severity recommendation, confidence level, and action list.</li>
          </ul>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Confidence Score & Data Retention</h4>
          <p style={{ margin: '0 0 12px' }}>
            Groq outputs a <strong>Confidence Score</strong> based on historical context availability. To preserve data sovereignty, the pipeline context is deleted from database memory immediately after completion.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Run a Triage (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: 0 }}>
            <li style={{ marginBottom: '4px' }}>Navigate to the <strong>Bugs</strong> page.</li>
            <li style={{ marginBottom: '4px' }}>Look for any flat, un-triaged bug row (it will say <strong>Needs Triage</strong> or have a grey state indicator).</li>
            <li style={{ marginBottom: '4px' }}>Click the green <strong>▶ Triage</strong> button on the right side of the row.</li>
            <li style={{ marginBottom: '4px' }}>A loading screen will appear showing the active phase of the pipeline. The triage run will complete in <strong>30–40 seconds on average</strong>.</li>
            <li style={{ marginBottom: '4px' }}>Upon completion, the results view will display the four detailed panels.</li>
          </ol>
        </div>
      ),
    },
    {
      id: 'retriage-changes',
      title: '5. Re-triage Guide',
      keywords: ['re-triage', 'retriage', 'refresh', 're-run', 'stale', 'update'],
      content: (
        <div>
          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>When to Re-triage a Bug</h4>
          <p style={{ margin: '0 0 12px' }}>
            You should re-triage a bug when you suspect the underlying code or tracker ticket has been updated, or when new comments have been added to the external ticket. Doing so refreshes findings by pulling live context from external databases.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Re-triage a Bug (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: 0 }}>
            <li style={{ marginBottom: '4px' }}>Open the triaged bug's details page (either from the Bugs list or by expanding the row).</li>
            <li style={{ marginBottom: '4px' }}>Locate the <strong>Run Fresh Triage</strong> or <strong>Re-triage</strong> button at the top-right corner of the triage results panels.</li>
            <li style={{ marginBottom: '4px' }}>Click the button to start a fresh run. The system will discard the cached data, fetch current external tickets, and re-run all AI agents.</li>
            <li style={{ marginBottom: '4px' }}>Wait <strong>30–40 seconds on average</strong> for the new triage results to compile and display.</li>
          </ol>
        </div>
      ),
    },
    {
      id: 'history',
      title: '6. History Page',
      keywords: ['history', 'audit_log', 'audit', 'retention', 'stored', 'sovereignty', 'fresh results'],
      content: (
        <div>
          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Triage Audit Logs</h4>
          <p style={{ margin: '0 0 12px' }}>
            The History page renders your local <a href="#" onClick={(e) => handleGlossaryJump(e, 'term-audit')}>audit_log</a> table entries. It tracks what you have triaged, how long it took, and what AI decisions were made.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>What is Stored vs. Not Stored</h4>
          <ul style={{ paddingLeft: '16px', margin: '0 0 12px' }}>
            <li style={{ marginBottom: '6px' }}>
              <strong>Stored:</strong> Case Group IDs, root bug IDs, severity recommendations, confidence scores, and engineer identifiers.
            </li>
            <li style={{ marginBottom: '6px' }}>
              <strong>Not Stored:</strong> Raw bug descriptions, commenter identities, confluence runbook texts, similar bug lists, and security tokens.
            </li>
          </ul>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Why are historical details unavailable?</h4>
          <p style={{ margin: '0 0 12px' }}>
            To enforce strict <strong>data sovereignty</strong>, this bug triage tool does not double-store descriptions or code. Historical data consists of indices.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Audit Past Investigations (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: 0 }}>
            <li style={{ marginBottom: '4px' }}>Click <strong>History</strong> in the top navigation bar.</li>
            <li style={{ marginBottom: '4px' }}>Scroll through the logs of past triage actions, showing when they were run and by whom.</li>
            <li style={{ marginBottom: '4px' }}>Click the <strong>View Results</strong> button on any history row to load the cached AI summaries.</li>
            <li style={{ marginBottom: '4px' }}>If the details are outdated, click the <strong>Re-triage</strong> button in the summary view to pull fresh information.</li>
          </ol>
        </div>
      ),
    },
    {
      id: 'connectors',
      title: '7. Token Settings',
      keywords: ['connector', 'connection', 'auth', 'secret', 'vault', 'prefix', 'token', 'edit'],
      content: (
        <div>
          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Connector Integrations</h4>
          <p style={{ margin: '0 0 12px' }}>
            Connectors bind this bug triage tool to your JIRA, GitHub, Bugzilla, and Confluence instances. They are registered in the <a href="#" onClick={(e) => handleGlossaryJump(e, 'term-registry')}>source_registry</a> table.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Auto-Detection Prefixes</h4>
          <p style={{ margin: '0 0 12px' }}>
            Prefixes map tickets to specific teams (e.g. <code>STOR-441</code> is matched to the Storage team's Jira instance).
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Encrypted Credentials Vault</h4>
          <p style={{ margin: '0 0 12px' }}>
            API tokens are not saved inside the core database. They are stored inside an encrypted vault container (<a href="#" onClick={(e) => handleGlossaryJump(e, 'term-vault')}>Credentials Vault</a> using AES-256).
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Add a Connector (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: '0 0 12px' }}>
            <li style={{ marginBottom: '4px' }}>Navigate to the <strong>Settings</strong> page via the top navigation bar.</li>
            <li style={{ marginBottom: '4px' }}>Select <strong>Token Settings</strong> in the Settings sidebar.</li>
            <li style={{ marginBottom: '4px' }}>Click the <strong>Add Connector</strong> button.</li>
            <li style={{ marginBottom: '4px' }}>Fill in the following fields:
              <br />• <strong>Display Name</strong>: E.g., "Company Jira" or "GitHub Core".
              <br />• <strong>Base URL</strong>: The exact API URL (e.g., <code>https://api.github.com</code> or <code>https://your-domain.atlassian.net</code>).
              <br />• <strong>Project Key</strong>: The key used by the tracker (e.g., <code>PROJ</code>).
              <br />• <strong>Ticket Prefix</strong>: The uppercase prefix for mapping tickets (e.g., <code>STOR</code>).
            </li>
            <li style={{ marginBottom: '4px' }}>In the API Token field, paste your Personal Access Token (PAT).</li>
            <li style={{ marginBottom: '4px' }}>Click the <strong>Test</strong> button to verify connectivity.</li>
            <li style={{ marginBottom: '4px' }}>Click <strong>Save</strong> to activate the connector instantly. No server restart is required.</li>
          </ol>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>How to Edit or Disable a Connector (Step-by-Step)</h4>
          <ol style={{ paddingLeft: '16px', margin: 0 }}>
            <li style={{ marginBottom: '4px' }}>Locate the connector inside the Settings page list.</li>
            <li style={{ marginBottom: '4px' }}>Click <strong>Edit</strong> to modify values, or toggle the connector off to <strong>Disable</strong> it.</li>
            <li style={{ marginBottom: '4px' }}>Disabling performs a soft-delete: it keeps historical audit logs intact but prevents new triage runs from query-calling that tracker.</li>
          </ol>
        </div>
      ),
    },
    {
      id: 'troubleshooting',
      title: '8. Troubleshooting',
      keywords: ['empty', 'slow', 'fail', 'failing', 'error', 'low confidence', 'fallback'],
      content: (
        <div>
          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Empty Bug List</h4>
          <ol style={{ paddingLeft: '16px', margin: '0 0 12px' }}>
            <li style={{ marginBottom: '4px' }}>Confirm that at least one Connector is added and toggled to <strong>Enabled</strong> in Settings.</li>
            <li style={{ marginBottom: '4px' }}>Double-check that project keys and prefixes match the ticket labels in Jira/GitHub.</li>
            <li style={{ marginBottom: '4px' }}>Verify that your API tokens are active and have not expired.</li>
          </ol>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Triage Taking Too Long / Timing Out</h4>
          <p style={{ margin: '0 0 12px' }}>
            Running a triage usually takes <strong>30–40 seconds on average</strong>. If it is taking a lot of time or times out, it indicates that the AI gateway or external tracker APIs are experiencing temporary network latency or rate limiting. The system is designed to handle this by automatically falling back to heuristic search matching if the AI gateway takes too long or fails to respond.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Connection Test Fails</h4>
          <ol style={{ paddingLeft: '16px', margin: '0 0 12px' }}>
            <li style={{ marginBottom: '4px' }}>Check for typos in base URLs (ensure correct HTTPS protocols are used).</li>
            <li style={{ marginBottom: '4px' }}>Ensure your firewalls allow outgoing requests to the endpoint.</li>
            <li style={{ marginBottom: '4px' }}>Verify that your token has correct access permissions (e.g. repo read scopes).</li>
          </ol>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>Low Confidence Score (&lt; 40%)</h4>
          <p style={{ margin: '0 0 12px' }}>
            Indicates that the agents could not locate similar bugs or relevant runbooks. Try linking Confluence pages with descriptive keywords to improve future matching indices.
          </p>

          <h4 style={{ margin: '0 0 6px', fontSize: '13px', color: 'var(--text)' }}>LLM Fallback Activated</h4>
          <p style={{ margin: 0 }}>
            If the Groq AI service is unresponsive, the system falls back to internal TF-IDF heuristic index overlapping to provide basic triage context.
          </p>
        </div>
      ),
    },
    {
      id: 'glossary',
      title: '9. Glossary',
      keywords: ['bt-id', 'audit_log', 'pipeline_context', 'connectorregistry', 'confidence', 'redis', 'jwt', 'vault', 'agents', 'synthesis', 'adapter'],
      content: (
        <div>
          <div id="term-btid" className={highlightedTerm === 'term-btid' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>BT-ID:</strong> Bug Triage identifier assigned by the database to group duplicates.
          </div>
          <div id="term-audit" className={highlightedTerm === 'term-audit' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>audit_log:</strong> PostgreSQL table storing triage summary logs (Unified Severity, confidence, timestamps).
          </div>
          <div id="term-pipeline" className={highlightedTerm === 'term-pipeline' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>pipeline_context:</strong> Temporary memory cache cleared upon completion of a triage run.
          </div>
          <div id="term-registry" className={highlightedTerm === 'term-registry' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>source_registry / ConnectorRegistry:</strong> Tables and objects storing and managing integration endpoints.
          </div>
          <div id="term-cache" className={highlightedTerm === 'term-cache' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>Redis Cache:</strong> Memory cache with a 120-second TTL used to store list queries.
          </div>
          <div id="term-ttl" className={highlightedTerm === 'term-ttl' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>TTL:</strong> Time-To-Live expiration flag for cached values.
          </div>
          <div id="term-jwt" className={highlightedTerm === 'term-jwt' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>JWT:</strong> JSON Web Token securing client-server communications.
          </div>
          <div id="term-vault" className={highlightedTerm === 'term-vault' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>Credentials Vault:</strong> Encryption engine securing PAT and API keys on disk.
          </div>
          <div id="agent-context" className={highlightedTerm === 'agent-context' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>ContextFetchAgent:</strong> Agent responsible for fetching description and comments.
          </div>
          <div id="agent-cross" className={highlightedTerm === 'agent-cross' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>CrossSystemFetchAgent:</strong> Agent searching for semantic duplicates across trackers.
          </div>
          <div id="agent-enrich" className={highlightedTerm === 'agent-enrich' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>EnrichmentAgent:</strong> Agent matching articles and customer issues.
          </div>
          <div id="agent-synthesis" className={highlightedTerm === 'agent-synthesis' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>AISynthesisAgent:</strong> Synthesis agent mapping final severity and root cause details.
          </div>
          <div id="pattern-adapter" className={highlightedTerm === 'pattern-adapter' ? 'glossary-highlight' : ''} style={{ marginBottom: '10px' }}>
            <strong>Adapter Pattern:</strong> Structural pattern allowing uniform interaction with different bug tracker APIs.
          </div>
        </div>
      ),
    },
  ]

  // Filter content based on active search text
  const filteredData = helpData.filter((sec) => {
    if (!searchTerm) return true
    const q = searchTerm.toLowerCase().trim()
    const matchesTitle = sec.title.toLowerCase().includes(q)
    const matchesKeywords = sec.keywords.some((k) => k.toLowerCase().includes(q))
    return matchesTitle || matchesKeywords
  })

  return (
    <>
      {/* Backdrop overlay */}
      <div className={`help-overlay ${isHelpOpen ? 'open' : ''}`} onClick={closeHelp} />

      {/* Slide-out help panel */}
      <div className={`help-drawer ${isHelpOpen ? 'open' : ''}`} style={{ fontFamily: "'Sora', system-ui, sans-serif" }}>
        {/* Header box */}
        <div className="help-hdr-box">
          <h2>Bug Triage Tool Help Center</h2>
          <button className="help-close-btn" onClick={closeHelp} title="Close Help Center">
            ✕
          </button>
        </div>

        {/* Live Search and Quick Tags */}
        <div className="help-search-area">
          <div className="search-wrap" style={{ position: 'relative' }}>
            <span className="search-icon" style={{ position: 'absolute', left: 9, top: '50%', transform: 'translateY(-50%)', color: 'var(--text3)', fontSize: '13px' }}>🔍</span>
            <input
              type="text"
              className="form-input"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              placeholder="Search articles, keywords, error codes..."
              style={{ paddingLeft: '32px', width: '100%', border: '1px solid var(--border)', borderRadius: '7px', height: '36px' }}
            />
            {searchTerm && (
              <button
                onClick={() => setSearchTerm('')}
                style={{ position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)', background: 'none', border: 'none', color: 'var(--text3)', cursor: 'pointer' }}
              >
                ✕
              </button>
            )}
          </div>
          <div className="help-chips">
            <span className="help-chip" onClick={() => handleTagClick('login')}>Login</span>
            <span className="help-chip" onClick={() => handleTagClick('filter')}>Filtering</span>
            <span className="help-chip" onClick={() => handleTagClick('group')}>Groupings</span>
            <span className="help-chip" onClick={() => handleTagClick('pipeline')}>Triage Run</span>
            <span className="help-chip" onClick={() => handleTagClick('connector')}>Connectors</span>
          </div>
        </div>

        {/* Accordion scroll area */}
        <div className="help-content-scroll">
          {filteredData.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '40px 10px', color: 'var(--text3)', fontSize: '13px' }}>
              No help articles found matching "{searchTerm}".
            </div>
          ) : (
            filteredData.map((sec) => {
              const isExpanded = !!(expandedSections[sec.id] || searchTerm)
              return (
                <div key={sec.id} className="help-section">
                  <button className="help-section-trigger" onClick={() => toggleSection(sec.id)}>
                    <span>{sec.title}</span>
                    <span
                      style={{
                        transform: isExpanded ? 'rotate(90deg)' : 'none',
                        transition: 'transform 0.15s',
                        fontSize: '10px',
                        color: 'var(--text3)',
                      }}
                    >
                      ▶
                    </span>
                  </button>
                  {isExpanded && <div className="help-section-content fade-in">{sec.content}</div>}
                </div>
              )
            })
          )}
        </div>
      </div>
    </>
  )
}
