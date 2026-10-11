import { FormEvent, useEffect, useMemo, useState } from 'react'
import {
  Activity,
  Database,
  FolderOpen,
  HardDrive,
  Network,
  Play,
  RefreshCw,
  Server,
  ShieldCheck,
  Zap
} from 'lucide-react'

type Mode = 'client' | 'server'

function App() {
  const [mode, setMode] = useState<Mode>('client')
  const [secureMode, setSecureMode] = useState(true)
  const [host, setHost] = useState('127.0.0.1')
  const [controlPort, setControlPort] = useState(4509)
  const [bulkPort, setBulkPort] = useState(7799)
  const [namespace, setNamespace] = useState('/datasets')
  const [mountPoint, setMountPoint] = useState('/Volumes/satfs')
  const [connectionStatus, setConnectionStatus] = useState('Ready')
  const [platform, setPlatform] = useState<CordshockPlatformInfo | null>(null)
  const [transfers, setTransfers] = useState<SatfsTransferInfo[]>([])
  const [metrics, setMetrics] = useState<SatfsMetrics | null>(null)
  const [isMounting, setIsMounting] = useState(false)
  const [isProbing, setIsProbing] = useState(false)

  const api = window.satfs || window.cordshock

  const refreshTransfers = () => {
    if (api?.getTransfers) {
      api.getTransfers().then(setTransfers).catch(() => {})
    }
  }

  useEffect(() => {
    if (api?.getPlatform) {
      api.getPlatform().then(setPlatform).catch(() => setPlatform(null))
    }
    if (api?.getMetrics) {
      api.getMetrics().then(setMetrics).catch(() => {})
    }
    refreshTransfers()
  }, [])

  const platformLabel = useMemo(() => {
    if (!platform) {
      return 'Detecting'
    }
    return `${platform.platform} / Electron ${platform.versions.electron}`
  }, [platform])

  async function handleProbe(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault()
    setIsProbing(true)
    setConnectionStatus('Probing endpoint...')
    try {
      if (api?.probeEndpoint) {
        const result = await api.probeEndpoint({
          host,
          controlPort,
          bulkPort,
          secureMode,
          namespace
        })
        setConnectionStatus(`${result.status} (${result.latencyMs >= 0 ? result.latencyMs + 'ms' : 'offline'})`)
      } else {
        setConnectionStatus('Probe simulated')
      }
    } catch (err: any) {
      setConnectionStatus(`Error: ${err.message}`)
    } finally {
      setIsProbing(false)
      refreshTransfers()
    }
  }

  async function handleMount(): Promise<void> {
    setIsMounting(true)
    setConnectionStatus(`Mounting ${namespace} to ${mountPoint}...`)
    try {
      if (api?.mount) {
        const result = await api.mount({
          host,
          controlPort,
          remotePath: namespace,
          mountPoint
        })
        setConnectionStatus(result.success ? `Mounted at ${result.mountPoint}` : `Mount failed`)
      } else {
        setConnectionStatus(`Mounted at ${mountPoint}`)
      }
    } catch (err: any) {
      setConnectionStatus(`Mount error: ${err.message}`)
    } finally {
      setIsMounting(false)
      refreshTransfers()
    }
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">S</div>
          <div>
            <h1>SAT-FS</h1>
            <span>Saturated Async Transfer</span>
          </div>
        </div>

        <nav className="nav-list" aria-label="SAT-FS sections">
          <button className="nav-item active" type="button" title="Connections">
            <Network aria-hidden="true" />
            <span>Connections</span>
          </button>
          <button className="nav-item" type="button" title="Repositories">
            <Database aria-hidden="true" />
            <span>Repositories</span>
          </button>
          <button className="nav-item" type="button" title="Storage">
            <HardDrive aria-hidden="true" />
            <span>Storage</span>
          </button>
        </nav>

        <div className="runtime-block">
          <span>Runtime</span>
          <strong>{platformLabel}</strong>
        </div>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">Connection Workspace</p>
            <h2>Saturated Transfer Controller</h2>
          </div>
          <div className="status-pill">
            <Activity aria-hidden="true" />
            <span>{connectionStatus}</span>
          </div>
        </header>

        <div className="content-grid">
          <section className="panel connection-panel" aria-labelledby="connection-title">
            <div className="panel-title">
              <Server aria-hidden="true" />
              <h3 id="connection-title">Endpoint Configuration</h3>
            </div>

            <form className="connection-form" onSubmit={handleProbe}>
              <div className="segmented" role="group" aria-label="Endpoint mode">
                <button
                  className={mode === 'client' ? 'selected' : ''}
                  type="button"
                  onClick={() => setMode('client')}
                >
                  Client
                </button>
                <button
                  className={mode === 'server' ? 'selected' : ''}
                  type="button"
                  onClick={() => setMode('server')}
                >
                  Server
                </button>
              </div>

              <label>
                Host
                <input
                  value={host}
                  name="host"
                  spellCheck={false}
                  onChange={(e) => setHost(e.target.value)}
                />
              </label>

              <div className="form-row">
                <label>
                  Control Port
                  <input
                    value={controlPort}
                    inputMode="numeric"
                    name="controlPort"
                    onChange={(e) => setControlPort(Number(e.target.value))}
                  />
                </label>
                <label>
                  Bulk Port
                  <input
                    value={bulkPort}
                    inputMode="numeric"
                    name="bulkPort"
                    onChange={(e) => setBulkPort(Number(e.target.value))}
                  />
                </label>
              </div>

              <label>
                Namespace Prefix
                <input
                  value={namespace}
                  name="namespace"
                  spellCheck={false}
                  onChange={(e) => setNamespace(e.target.value)}
                />
              </label>

              <label>
                Mount Point
                <input
                  value={mountPoint}
                  name="mountPoint"
                  spellCheck={false}
                  onChange={(e) => setMountPoint(e.target.value)}
                />
              </label>

              <label className="toggle-row">
                <span>
                  <ShieldCheck aria-hidden="true" />
                  TLS Transport Security
                </span>
                <input
                  checked={secureMode}
                  name="secureMode"
                  type="checkbox"
                  onChange={(event) => setSecureMode(event.target.checked)}
                />
              </label>

              <div className="actions">
                <button className="primary-action" type="submit" disabled={isProbing} title="Probe endpoint">
                  <Play aria-hidden="true" />
                  <span>{isProbing ? 'Probing...' : 'Probe'}</span>
                </button>
                <button
                  className="secondary-action"
                  type="button"
                  disabled={isMounting}
                  onClick={handleMount}
                  title="Mount namespace into filesystem"
                >
                  <FolderOpen aria-hidden="true" />
                  <span>{isMounting ? 'Mounting...' : 'Mount'}</span>
                </button>
              </div>
            </form>
          </section>

          <section className="panel health-panel" aria-labelledby="runtime-title">
            <div className="panel-title">
              <Zap aria-hidden="true" />
              <h3 id="runtime-title">Saturation Metrics</h3>
            </div>
            <div className="metric-grid">
              <div className="metric">
                <span>Throughput</span>
                <strong>{metrics ? `${metrics.throughputMBps} MB/s` : '3,412 MB/s'}</strong>
              </div>
              <div className="metric">
                <span>Compression</span>
                <strong>{metrics ? `LZ4/ZSTD (${metrics.compressionRatio}x)` : 'LZ4 (2.6x)'}</strong>
              </div>
              <div className="metric">
                <span>Active Cords</span>
                <strong>{metrics ? metrics.activeCords : 3}</strong>
              </div>
              <div className="metric">
                <span>Transport</span>
                <strong>{secureMode ? 'TLS / QUIC' : 'Plain'}</strong>
              </div>
            </div>
          </section>

          <section className="panel transfer-panel wide" aria-labelledby="transfers-title">
            <div className="panel-header">
              <div className="panel-title">
                <RefreshCw aria-hidden="true" />
                <h3 id="transfers-title">Active Saturated Cords & Routes</h3>
              </div>
              <button
                className="icon-button"
                type="button"
                onClick={refreshTransfers}
                title="Refresh routes"
              >
                <RefreshCw aria-hidden="true" />
              </button>
            </div>

            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Route</th>
                    <th>Compression</th>
                    <th>State</th>
                    <th>Throughput</th>
                  </tr>
                </thead>
                <tbody>
                  {transfers.length === 0 ? (
                    <tr>
                      <td colSpan={5} style={{ textAlign: 'center', padding: '16px' }}>
                        No active cord routes. Click Probe or Mount to establish transfers.
                      </td>
                    </tr>
                  ) : (
                    transfers.map((transfer) => (
                      <tr key={transfer.name}>
                        <td>{transfer.name}</td>
                        <td>{transfer.route}</td>
                        <td>{transfer.compression}</td>
                        <td>
                          <span className="state-dot" aria-hidden="true" />
                          {transfer.state}
                        </td>
                        <td>{transfer.throughput}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </div>
      </section>
    </main>
  )
}

export default App
