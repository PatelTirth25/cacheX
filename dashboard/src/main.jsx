import React, { useCallback, useEffect, useMemo, useState } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'

const initialOverview = {
  node: { id: '—', address: '—', keys: 0, memory_bytes: 0, uptime_secs: 0 },
  nodes: [],
  replication_factor: 1,
}

function formatBytes(bytes) {
  if (!bytes) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB']
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  return `${(bytes / 1024 ** index).toFixed(index ? 1 : 0)} ${units[index]}`
}

function formatUptime(seconds) {
  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  return hours ? `${hours}h ${minutes}m` : `${minutes}m ${seconds % 60}s`
}

function responseLabel(response) {
  if (!response) return ''
  if (typeof response === 'string') return response.toUpperCase()
  if (response.Ok !== undefined) return 'OK'
  if (response.Pong !== undefined) return 'PONG'
  if (response.Error) return `ERROR: ${response.Error}`
  if (response.Value !== undefined) return response.Value ? `VALUE ${new TextDecoder().decode(new Uint8Array(response.Value))}` : '(nil)'
  return JSON.stringify(response)
}

function App() {
  const [apiBase, setApiBase] = useState(() => localStorage.getItem('cachex-api') || 'http://127.0.0.1:7600')
  const [overview, setOverview] = useState(initialOverview)
  const [history, setHistory] = useState([])
  const [activeView, setActiveView] = useState('overview')
  const [connected, setConnected] = useState(false)
  const [loading, setLoading] = useState(false)
  const [lastUpdated, setLastUpdated] = useState(null)
  const [operation, setOperation] = useState('GET')
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [ttl, setTtl] = useState('')
  const [result, setResult] = useState('')

  const fetchOverview = useCallback(async () => {
    setLoading(true)
    try {
      const response = await fetch(`${apiBase.replace(/\/$/, '')}/api/overview`)
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      setOverview(await response.json())
      setConnected(true)
      setLastUpdated(new Date())
    } catch (error) {
      setConnected(false)
      setResult(`Dashboard API unavailable: ${error.message}`)
    } finally {
      setLoading(false)
    }
  }, [apiBase])

  useEffect(() => {
    fetchOverview()
    const timer = setInterval(fetchOverview, 5000)
    return () => clearInterval(timer)
  }, [fetchOverview])

  const metrics = useMemo(() => [
    { label: 'Cluster health', value: connected ? 'Operational' : 'Offline', note: connected ? 'API responding' : 'Check server', tone: connected ? 'green' : 'red' },
    { label: 'Known nodes', value: overview.nodes.length || '—', note: `RF ${overview.replication_factor}`, tone: 'blue' },
    { label: 'Keys on node', value: overview.node.keys.toLocaleString(), note: 'Local store', tone: 'purple' },
    { label: 'Memory footprint', value: formatBytes(overview.node.memory_bytes), note: formatUptime(overview.node.uptime_secs), tone: 'orange' },
  ], [connected, overview])

  async function runOperation(event) {
    event.preventDefault()
    if (!key.trim()) return setResult('Enter a key first.')
    const payload = { operation, key: key.trim() }
    if (operation === 'SET') {
      if (!value) return setResult('Enter a value for SET.')
      payload.value = value
      if (ttl) payload.ttl_secs = Number(ttl)
    }
    try {
      const response = await fetch(`${apiBase.replace(/\/$/, '')}/api/command`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
      })
      const data = await response.json()
      const label = data.response ? responseLabel(data.response) : data.error
      setResult(label)
      setHistory((items) => [{ operation, key, result: label, time: new Date() }, ...items].slice(0, 6))
      fetchOverview()
    } catch (error) {
      setResult(`Request failed: ${error.message}`)
    }
  }

  function saveEndpoint(event) {
    event.preventDefault()
    localStorage.setItem('cachex-api', apiBase)
    fetchOverview()
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand"><span className="brand-mark">CX</span><span>CacheX</span></div>
        <div className="eyebrow">CONTROL ROOM</div>
        <nav>
          <button className={activeView === 'overview' ? 'nav-item active' : 'nav-item'} onClick={() => setActiveView('overview')}><span>⌂</span> Overview</button>
          <button className={activeView === 'explorer' ? 'nav-item active' : 'nav-item'} onClick={() => setActiveView('explorer')}><span>⌕</span> Key explorer</button>
          <button className={activeView === 'nodes' ? 'nav-item active' : 'nav-item'} onClick={() => setActiveView('nodes')}><span>◈</span> Cluster nodes</button>
        </nav>
        <div className="sidebar-footer"><span className={connected ? 'status-dot live' : 'status-dot'}></span><span>{connected ? 'Live connection' : 'Disconnected'}</span></div>
      </aside>

      <main className="main-content">
        <header className="topbar">
          <div><p className="kicker">DISTRIBUTED CACHE / PHASE 5</p><h1>{activeView === 'overview' ? 'System overview' : activeView === 'explorer' ? 'Key explorer' : 'Cluster nodes'}</h1></div>
          <div className="topbar-actions"><span className="last-sync">{lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString()}` : 'Waiting for sync'}</span><button className="icon-button" onClick={fetchOverview} aria-label="Refresh dashboard">↻</button></div>
        </header>

        {activeView === 'overview' && <>
          <section className="hero-panel"><div><p className="kicker accent">REPLICATION MONITOR</p><h2>Keep every byte<br /><em>within reach.</em></h2><p className="hero-copy">A clear view of CacheX health, capacity, and node coordination.</p></div><div className="pulse-visual"><div className="pulse-ring ring-one"></div><div className="pulse-ring ring-two"></div><div className="pulse-core"><span>{overview.replication_factor}×</span><small>copies</small></div></div></section>
          <section className="metric-grid">{metrics.map((metric) => <article className="metric-card" key={metric.label}><div className={`metric-icon ${metric.tone}`}></div><p>{metric.label}</p><strong>{metric.value}</strong><span>{metric.note}</span></article>)}</section>
          <div className="content-grid"><section className="panel operations-panel"><div className="panel-heading"><div><p className="kicker">LIVE OPERATION</p><h3>Inspect the cache</h3></div><span className="live-pill"><span className="status-dot live"></span> Ready</span></div><form onSubmit={runOperation}><div className="segmented">{['GET', 'SET', 'DELETE'].map((item) => <button type="button" className={operation === item ? 'selected' : ''} onClick={() => setOperation(item)} key={item}>{item}</button>)}</div><label>Key<input value={key} onChange={(event) => setKey(event.target.value)} placeholder="e.g. session:42" /></label>{operation === 'SET' && <div className="form-row"><label>Value<input value={value} onChange={(event) => setValue(event.target.value)} placeholder="value" /></label><label>TTL <span className="muted">seconds</span><input type="number" min="1" value={ttl} onChange={(event) => setTtl(event.target.value)} placeholder="optional" /></label></div>}<button className="primary-button" disabled={loading}>{loading ? 'Syncing…' : `Run ${operation}`}</button>{result && <div className={result.includes('failed') || result.includes('unavailable') ? 'result-box error' : 'result-box'}>{result}</div>}</form></section><section className="panel activity-panel"><div className="panel-heading"><div><p className="kicker">RECENT ACTIVITY</p><h3>Command trail</h3></div><span className="count-badge">{history.length}</span></div>{history.length === 0 ? <div className="empty-state"><span>✦</span><p>Your command history will appear here.</p></div> : <div className="activity-list">{history.map((item, index) => <div className="activity-row" key={`${item.time.toISOString()}-${index}`}><span className={`activity-method ${item.operation.toLowerCase()}`}>{item.operation}</span><span className="activity-key">{item.key}</span><span className="activity-result">{item.result}</span></div>)}</div>}</section></div>
        </>}

        {activeView === 'explorer' && <section className="panel full-panel"><div className="panel-heading"><div><p className="kicker">KEY EXPLORER</p><h3>Run a cache operation</h3></div></div><p className="section-copy">Use the operation console to read, write, and delete values through the dashboard HTTP bridge.</p><form className="explorer-form" onSubmit={runOperation}><div className="segmented">{['GET', 'SET', 'DELETE'].map((item) => <button type="button" className={operation === item ? 'selected' : ''} onClick={() => setOperation(item)} key={item}>{item}</button>)}</div><label>Key<input value={key} onChange={(event) => setKey(event.target.value)} placeholder="session:42" /></label>{operation === 'SET' && <><label>Value<input value={value} onChange={(event) => setValue(event.target.value)} placeholder="value" /></label><label>TTL in seconds<input type="number" min="1" value={ttl} onChange={(event) => setTtl(event.target.value)} placeholder="optional" /></label></>}<button className="primary-button">Run operation</button>{result && <div className="result-box">{result}</div>}</form></section>}

        {activeView === 'nodes' && <NodeTable nodes={overview.nodes} localNode={overview.node} />}

        <section className="endpoint-bar"><div><span className="endpoint-label">DASHBOARD API</span><span className="endpoint-hint">Connect this view to any CacheX node</span></div><form onSubmit={saveEndpoint}><input value={apiBase} onChange={(event) => setApiBase(event.target.value)} aria-label="Dashboard API endpoint" /><button>Connect</button></form></section>
      </main>
    </div>
  )
}

function NodeTable({ nodes, localNode }) {
  return <section className="panel full-panel"><div className="panel-heading"><div><p className="kicker">TOPOLOGY</p><h3>Cluster nodes</h3></div><span className="live-pill"><span className="status-dot live"></span>{nodes.length} configured</span></div><div className="node-table"><div className="node-table-head"><span>Node</span><span>Address</span><span>Role</span><span>Status</span></div>{nodes.map((node) => <div className="node-row" key={node.id}><span className="node-name"><span className="node-avatar">{node.id.slice(-1).toUpperCase()}</span>{node.id}{node.id === localNode.id && <small> local</small>}</span><span className="mono">{node.address}</span><span>{node.id === localNode.id ? 'Serving API' : 'Replica candidate'}</span><span className="node-status"><span className="status-dot live"></span> Configured</span></div>)}</div></section>
}

createRoot(document.getElementById('root')).render(<App />)
