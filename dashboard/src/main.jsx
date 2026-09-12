import React, { useCallback, useEffect, useMemo, useState } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'

const initialOverview = {
  node: { id: '—', address: '—', keys: 0, memory_bytes: 0, uptime_secs: 0 },
  nodes: [],
  replication_factor: 1,
  partitioner: '—',
  capacity: 0,
  aof_path: '—',
}

const initialServerForm = {
  node_id: 'node-b',
  address: '127.0.0.1:7002',
  dashboard_address: '127.0.0.1:7602',
  aof_path: 'node-b.aof',
  nodes: 'node-a=127.0.0.1:7001,node-b=127.0.0.1:7002',
  partitioner: 'consistent',
  replication_factor: '2',
  capacity: '10000',
  heartbeat_interval_ms: '1000',
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
  const [servers, setServers] = useState([])
  const [serverForm, setServerForm] = useState(initialServerForm)
  const [serverLoading, setServerLoading] = useState(false)

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

  const fetchServers = useCallback(async () => {
    try {
      const response = await fetch(`${apiBase.replace(/\/$/, '')}/api/servers`)
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      setServers((await response.json()).servers || [])
    } catch {
      setServers([])
    }
  }, [apiBase])

  useEffect(() => {
    fetchOverview()
    fetchServers()
    const timer = setInterval(fetchOverview, 5000)
    const serverTimer = setInterval(fetchServers, 3000)
    return () => {
      clearInterval(timer)
      clearInterval(serverTimer)
    }
  }, [fetchOverview, fetchServers])

  const metrics = useMemo(() => [
    { label: 'Cluster health', value: connected ? 'Operational' : 'Offline', note: connected ? 'API responding' : 'Check server', tone: connected ? 'green' : 'red' },
    { label: 'Known nodes', value: overview.nodes.length || '—', note: `${overview.partitioner} · RF ${overview.replication_factor}`, tone: 'blue' },
    { label: 'Keys on node', value: overview.node.keys.toLocaleString(), note: 'Local store', tone: 'purple' },
    { label: 'Memory footprint', value: formatBytes(overview.node.memory_bytes), note: `${formatUptime(overview.node.uptime_secs)} · cap ${overview.capacity.toLocaleString()}`, tone: 'orange' },
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
      const route = data.route?.primary
        ? `Primary ${data.route.primary.id}; replicas ${data.route.replicas.map((node) => node.id).join(', ')}`
        : ''
      const displayResult = route ? `${label} · ${route}` : label
      setResult(displayResult)
      setHistory((items) => [{ operation, key, result: displayResult, time: new Date() }, ...items].slice(0, 6))
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

  function updateServerField(event) {
    const { name, value: fieldValue } = event.target
    setServerForm((form) => ({ ...form, [name]: fieldValue }))
  }

  async function startServer(event) {
    event.preventDefault()
    setServerLoading(true)
    try {
      const response = await fetch(`${apiBase.replace(/\/$/, '')}/api/servers/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...serverForm,
          replication_factor: Number(serverForm.replication_factor),
          capacity: Number(serverForm.capacity),
          heartbeat_interval_ms: Number(serverForm.heartbeat_interval_ms),
        }),
      })
      const data = await response.json()
      if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`)
      setResult(`Started ${data.server.node_id} (PID ${data.server.pid})`)
      fetchServers()
      fetchOverview()
    } catch (error) {
      setResult(`Server start failed: ${error.message}`)
    } finally {
      setServerLoading(false)
    }
  }

  async function stopServer(nodeId) {
    try {
      const response = await fetch(`${apiBase.replace(/\/$/, '')}/api/servers/stop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ node_id: nodeId }),
      })
      const data = await response.json()
      if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`)
      setResult(`Stopped ${data.server.node_id}`)
      fetchServers()
      fetchOverview()
    } catch (error) {
      setResult(`Server stop failed: ${error.message}`)
    }
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
          <button className={activeView === 'servers' ? 'nav-item active' : 'nav-item'} onClick={() => setActiveView('servers')}><span>＋</span> Server manager</button>
        </nav>
        <div className="sidebar-footer"><span className={connected ? 'status-dot live' : 'status-dot'}></span><span>{connected ? 'Live connection' : 'Disconnected'}</span></div>
      </aside>

      <main className="main-content">
        <header className="topbar">
          <div><p className="kicker">DISTRIBUTED CACHE / PHASE 5</p><h1>{activeView === 'overview' ? 'System overview' : activeView === 'explorer' ? 'Key explorer' : activeView === 'nodes' ? 'Cluster nodes' : 'Server manager'}</h1></div>
          <div className="topbar-actions"><span className="last-sync">{lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString()}` : 'Waiting for sync'}</span><button className="icon-button" onClick={fetchOverview} aria-label="Refresh dashboard">↻</button></div>
        </header>

        {activeView === 'overview' && <>
          <section className="hero-panel"><div><p className="kicker accent">REPLICATION MONITOR</p><h2>Keep every byte<br /><em>within reach.</em></h2><p className="hero-copy">A clear view of CacheX health, capacity, and node coordination.</p></div><div className="pulse-visual"><div className="pulse-ring ring-one"></div><div className="pulse-ring ring-two"></div><div className="pulse-core"><span>{overview.replication_factor}×</span><small>copies</small></div></div></section>
          <section className="metric-grid">{metrics.map((metric) => <article className="metric-card" key={metric.label}><div className={`metric-icon ${metric.tone}`}></div><p>{metric.label}</p><strong>{metric.value}</strong><span>{metric.note}</span></article>)}</section>
          <section className="panel activity-panel overview-activity"><div className="panel-heading"><div><p className="kicker">RECENT ACTIVITY</p><h3>Command trail</h3></div><span className="count-badge">{history.length}</span></div>{history.length === 0 ? <div className="empty-state"><span>✦</span><p>Your command history will appear here.</p></div> : <div className="activity-list">{history.map((item, index) => <div className="activity-row" key={`${item.time.toISOString()}-${index}`}><span className={`activity-method ${item.operation.toLowerCase()}`}>{item.operation}</span><span className="activity-key">{item.key}</span><span className="activity-result">{item.result}</span></div>)}</div>}</section>
        </>}

        {activeView === 'explorer' && <section className="panel full-panel"><div className="panel-heading"><div><p className="kicker">KEY EXPLORER</p><h3>Run a cache operation</h3></div></div><p className="section-copy">Use the operation console to read, write, and delete values through the dashboard HTTP bridge.</p><form className="explorer-form" onSubmit={runOperation}><div className="segmented">{['GET', 'SET', 'DELETE'].map((item) => <button type="button" className={operation === item ? 'selected' : ''} onClick={() => setOperation(item)} key={item}>{item}</button>)}</div><label>Key<input value={key} onChange={(event) => setKey(event.target.value)} placeholder="session:42" /></label>{operation === 'SET' && <><label>Value<input value={value} onChange={(event) => setValue(event.target.value)} placeholder="value" /></label><label>TTL in seconds<input type="number" min="1" value={ttl} onChange={(event) => setTtl(event.target.value)} placeholder="optional" /></label></>}<button className="primary-button">Run operation</button>{result && <div className="result-box">{result}</div>}</form></section>}

        {activeView === 'nodes' && <NodeTable nodes={overview.nodes} localNode={overview.node} />}

        {activeView === 'servers' && <section className="panel full-panel"><div className="panel-heading"><div><p className="kicker">LOCAL SUPERVISOR</p><h3>Start a CacheX server</h3></div><span className="live-pill"><span className="status-dot live"></span>{servers.length} managed</span></div><p className="section-copy">Start a second node from this dashboard. The running dashboard server launches and supervises the child process locally.</p><form className="server-form" onSubmit={startServer}><div className="server-form-grid"><label>Node ID<input name="node_id" value={serverForm.node_id} onChange={updateServerField} placeholder="node-b" /></label><label>Cache address<input name="address" value={serverForm.address} onChange={updateServerField} placeholder="127.0.0.1:7002" /></label><label>Dashboard address<input name="dashboard_address" value={serverForm.dashboard_address} onChange={updateServerField} placeholder="127.0.0.1:7602" /></label><label>AOF path<input name="aof_path" value={serverForm.aof_path} onChange={updateServerField} placeholder="node-b.aof" /></label><label className="server-form-wide">Cluster nodes<input name="nodes" value={serverForm.nodes} onChange={updateServerField} placeholder="node-a=127.0.0.1:7001,node-b=127.0.0.1:7002" /></label><label>Partitioner<select name="partitioner" value={serverForm.partitioner} onChange={updateServerField}><option value="consistent">Consistent hashing</option><option value="modulo">Modulo</option></select></label><label>Replication factor<input name="replication_factor" type="number" min="1" max="2" value={serverForm.replication_factor} onChange={updateServerField} /></label><label>Capacity<input name="capacity" type="number" min="1" value={serverForm.capacity} onChange={updateServerField} /></label><label>Heartbeat interval (ms)<input name="heartbeat_interval_ms" type="number" min="100" value={serverForm.heartbeat_interval_ms} onChange={updateServerField} /></label></div><button className="primary-button" disabled={serverLoading}>{serverLoading ? 'Starting…' : `Start ${serverForm.node_id || 'server'}`}</button>{result && activeView === 'servers' && <div className={result.includes('failed') ? 'result-box error' : 'result-box'}>{result}</div>}</form><div className="managed-server-list"><div className="panel-heading"><div><p className="kicker">CHILD PROCESSES</p><h3>Managed servers</h3></div></div>{servers.length === 0 ? <div className="empty-state"><span>＋</span><p>No child servers are managed by this dashboard yet.</p></div> : servers.map((server) => <div className="managed-server-row" key={server.node_id}><div><strong>{server.node_id}</strong><span className="mono">{server.address} · dashboard {server.dashboard_address}</span></div><span className="node-status healthy"><span className="status-dot live"></span>{server.status} · PID {server.pid}</span><button className="danger-button" type="button" onClick={() => stopServer(server.node_id)}>Stop</button></div>)}</div></section>}

        <section className="endpoint-bar"><div><span className="endpoint-label">DASHBOARD API</span><span className="endpoint-hint">Connect this view to any CacheX node</span></div><form onSubmit={saveEndpoint}><input value={apiBase} onChange={(event) => setApiBase(event.target.value)} aria-label="Dashboard API endpoint" /><button>Connect</button></form></section>
      </main>
    </div>
  )
}

function NodeTable({ nodes }) {
  return <section className="panel full-panel"><div className="panel-heading"><div><p className="kicker">TOPOLOGY</p><h3>Cluster nodes</h3></div><span className="live-pill"><span className="status-dot live"></span>{nodes.length} configured</span></div><div className="node-table"><div className="node-table-head"><span>Node</span><span>Address</span><span>Role</span><span>Status</span></div>{nodes.map((node) => <div className="node-row" key={node.id}><span className="node-name"><span className="node-avatar">{node.id.slice(-1).toUpperCase()}</span>{node.id}{node.local && <small> local</small>}</span><span className="mono">{node.address}</span><span>{node.local ? 'Serving API' : 'Replica candidate'}</span><span className={`node-status ${node.status}`}><span className={`status-dot ${node.status === 'healthy' ? 'live' : ''}`}></span>{node.status}{node.failure_count ? ` · ${node.failure_count} failures` : ''}</span></div>)}</div></section>
}

createRoot(document.getElementById('root')).render(<App />)
