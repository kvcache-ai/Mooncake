'use client'

import { useEffect, useState, useCallback } from 'react'

interface Cluster {
  metadata: { name: string; namespace: string }
  status?: { phase?: string; masterReady?: number; workerReady?: number }
}

interface OperatorPod {
  name: string
  phase: string
  node: string
  ready: boolean
}

interface OperatorLeaderInfo {
  holder: string
  leaseDuration: number
  acquireTime: string
  renewTime: string
  leaderTransitions: number
}

interface OperatorStatus {
  pods: OperatorPod[]
  readyCount: number
  totalCount: number
  leader: OperatorLeaderInfo | null
  error?: string
}

function getLeaderPodName(holder: string): string {
  return holder ? holder.split('_')[0] : ''
}

export default function Home() {
  const [clusters, setClusters] = useState<Cluster[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [opStatus, setOpStatus] = useState<OperatorStatus | null>(null)
  const [opLoading, setOpLoading] = useState(true)
  const [scaling, setScaling] = useState(false)
  const [scaleInput, setScaleInput] = useState('2')
  const [rdmaStatus, setRdmaStatus] = useState<Record<string, any[]>>({})
  const [rdmaLoading, setRdmaLoading] = useState<Record<string, boolean>>({})

  useEffect(() => {
    let cancelled = false
    fetch('/api/clusters')
      .then(r => r.json())
      .then(d => {
        if (cancelled) return
        if (d.error) {
          setError(d.error)
        } else {
          setError(null)
          setClusters(d.clusters || [])
        }
      })
      .catch(() => {
        if (!cancelled) setError('Failed to connect to server')
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    let cancelled = false
    function fetchOp() {
      fetch('/api/operator')
        .then(r => r.json())
        .then(d => {
          if (cancelled) return
          setOpStatus(d)
          if (!cancelled && d?.totalCount) {
            setScaleInput(String(d.totalCount))
          }
        })
        .catch(() => {})
        .finally(() => {
          if (!cancelled) setOpLoading(false)
        })
    }
    fetchOp()
    const interval = setInterval(fetchOp, 15000)
    return () => {
      cancelled = true
      clearInterval(interval)
    }
  }, [])

  const handleScale = useCallback(async (replicas: number) => {
    if (replicas < 1) return
    setScaling(true)
    try {
      const resp = await fetch('/api/operator/scale', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ replicas }),
      })
      const data = await resp.json()
      if (data.success) {
        setScaleInput(String(replicas))
        // Refresh status after a short delay
        setTimeout(async () => {
          const r = await fetch('/api/operator')
          const d = await r.json()
          setOpStatus(d)
          setScaling(false)
        }, 2000)
        return
      }
    } catch (e) {
      console.error('Scale failed', e)
    }
    setScaling(false)
  }, [])

  // Fetch RDMA status for all clusters
  useEffect(() => {
    if (clusters.length === 0) return
    let cancelled = false
    async function fetchRdma() {
      const newStatus: Record<string, any[]> = {}
      const newLoading: Record<string, boolean> = {}
      for (const cluster of clusters) {
        const ns = cluster.metadata?.namespace || 'default'
        const name = cluster.metadata?.name || ''
        const key = `${ns}/${name}`
        newLoading[key] = true
        try {
          const r = await fetch(`/api/clusters/${ns}/${name}/rdma-status`)
          const d = await r.json()
          if (!cancelled) newStatus[key] = d.workers || []
        } catch (e) {
          if (!cancelled) newStatus[key] = []
        }
        if (!cancelled) newLoading[key] = false
      }
      if (!cancelled) {
        setRdmaStatus(newStatus)
        setRdmaLoading(newLoading)
      }
    }
    fetchRdma()
    const interval = setInterval(fetchRdma, 15000)
    return () => {
      cancelled = true
      clearInterval(interval)
    }
  }, [clusters])

  const total = clusters.length
  const running = clusters.filter(c => c.status?.phase === 'Running').length
  const failed = clusters.filter(c => c.status?.phase === 'Failed').length

  const leaderPodName = opStatus?.leader ? getLeaderPodName(opStatus.leader.holder) : ''

  return (
    <div>
      <h1 className="text-2xl font-semibold text-gray-900">Dashboard</h1>

      {error && (
        <div className="mt-4 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
          Error: {error}
        </div>
      )}

      {/* Cluster Summary Cards */}
      <div className="mt-6 grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3">
        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <svg className="h-6 w-6 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
                </svg>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">Total Clusters</dt>
                  <dd className="text-lg font-semibold text-gray-900">{loading ? '...' : total}</dd>
                </dl>
              </div>
            </div>
          </div>
          <div className="bg-gray-50 px-5 py-3">
            <div className="text-sm">
              <a href="/clusters" className="font-medium text-indigo-700 hover:text-indigo-900">View all</a>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <svg className="h-6 w-6 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">Running</dt>
                  <dd className="text-lg font-semibold text-gray-900">{loading ? '...' : running}</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <svg className="h-6 w-6 text-yellow-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">Failed</dt>
                  <dd className="text-lg font-semibold text-gray-900">{loading ? '...' : failed}</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Operator HA Status Section */}
      <div className="mt-8">
        <h2 className="text-xl font-semibold text-gray-900">Operator HA Status</h2>
        <div className="mt-4 bg-white overflow-hidden shadow rounded-lg">
          {opLoading ? (
            <div className="p-5 text-gray-500">Loading operator status...</div>
          ) : opStatus?.error ? (
            <div className="p-5 text-red-600">Error: {opStatus.error}</div>
          ) : (
            <div className="p-5">
              {/* Summary row */}
              <div className="flex flex-wrap items-center gap-6 mb-4">
                <div>
                  <span className="text-sm text-gray-500">Pods</span>
                  <span className="ml-2 text-lg font-semibold text-gray-900">
                    {opStatus?.readyCount || 0}/{opStatus?.totalCount || 0}
                  </span>
                  <span className="ml-1 text-sm text-gray-500">ready</span>
                </div>
                {opStatus?.leader && (
                  <>
                    <div>
                      <span className="text-sm text-gray-500">Transitions</span>
                      <span className="ml-2 text-sm font-semibold text-gray-900">
                        {opStatus.leader.leaderTransitions}
                      </span>
                    </div>
                    <div>
                      <span className="text-sm text-gray-500">Lease Duration</span>
                      <span className="ml-2 text-sm text-gray-900">
                        {opStatus.leader.leaseDuration}s
                      </span>
                    </div>
                  </>
                )}

                {/* Scale controls */}
                <div className="ml-auto flex items-center gap-2">
                  <span className="text-sm text-gray-500">Replicas</span>
                  <div className="flex items-center border border-gray-300 rounded">
                    <button
                      onClick={() => handleScale(Math.max(1, parseInt(scaleInput || '1') - 1))}
                      disabled={scaling || parseInt(scaleInput || '1') <= 1}
                      className="px-2 py-1 text-gray-600 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed border-r border-gray-300"
                    >
                      -
                    </button>
                    <input
                      type="number"
                      min={1}
                      value={scaleInput}
                      onChange={e => setScaleInput(e.target.value)}
                      onBlur={e => {
                        const v = parseInt(e.target.value)
                        if (!isNaN(v) && v >= 1) handleScale(v)
                        else setScaleInput(String(opStatus?.totalCount || 1))
                      }}
                      className="w-14 text-center text-sm py-1 outline-none [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
                    />
                    <button
                      onClick={() => handleScale(Math.max(1, parseInt(scaleInput || '1') + 1))}
                      disabled={scaling}
                      className="px-2 py-1 text-gray-600 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed border-l border-gray-300"
                    >
                      +
                    </button>
                  </div>
                  {scaling && <span className="text-xs text-indigo-600">updating...</span>}
                </div>
              </div>

              {/* Pod table */}
              {opStatus && opStatus.pods.length > 0 && (
                <div className="overflow-x-auto">
                  <table className="min-w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-200">
                        <th className="text-left py-2 pr-4 text-gray-500 font-medium">Pod</th>
                        <th className="text-left py-2 pr-4 text-gray-500 font-medium">Status</th>
                        <th className="text-left py-2 pr-4 text-gray-500 font-medium">Ready</th>
                        <th className="text-left py-2 pr-4 text-gray-500 font-medium">Leader</th>
                        <th className="text-left py-2 text-gray-500 font-medium">Node</th>
                      </tr>
                    </thead>
                    <tbody>
                      {opStatus.pods.map(pod => (
                        <tr key={pod.name} className="border-b border-gray-100 hover:bg-gray-50">
                          <td className="py-2 pr-4 font-mono text-xs text-gray-900 truncate max-w-xs">
                            {pod.name}
                          </td>
                          <td className="py-2 pr-4">
                            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                              pod.phase === 'Running' ? 'bg-green-100 text-green-800' :
                              pod.phase === 'Pending' ? 'bg-yellow-100 text-yellow-800' :
                              'bg-gray-100 text-gray-800'
                            }`}>
                              {pod.phase}
                            </span>
                          </td>
                          <td className="py-2 pr-4">
                            {pod.ready ? (
                              <span className="text-green-600 font-medium">Yes</span>
                            ) : (
                              <span className="text-red-600 font-medium">No</span>
                            )}
                          </td>
                          <td className="py-2 pr-4">
                            {leaderPodName && pod.name.startsWith(leaderPodName) ? (
                              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-indigo-100 text-indigo-800">
                                Leader
                              </span>
                            ) : (
                              <span className="text-gray-300">-</span>
                            )}
                          </td>
                          <td className="py-2 text-xs text-gray-500 font-mono">{pod.node}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* RDMA Transport Status Section */}
      {clusters.length > 0 && (
        <div className="mt-8">
          <h2 className="text-xl font-semibold text-gray-900">RDMA Transport Status</h2>
          {Object.keys(rdmaStatus).length === 0 ? (
            <div className="mt-4 bg-white overflow-hidden shadow rounded-lg p-5 text-gray-500">
              Loading RDMA status...
            </div>
          ) : (
            Object.entries(rdmaStatus).map(([clusterKey, workers]) => {
              const healthy = workers.filter(w => w.rdmaAvailable).length
              const total = workers.length
              const requested = workers.filter(w => w.rdmaRequested).length
              return (
                <div key={clusterKey} className="mt-4 bg-white overflow-hidden shadow rounded-lg">
                  <div className="p-5">
                    <div className="flex items-center mb-4">
                      <span className="text-sm font-medium text-gray-500">Cluster:</span>
                      <span className="ml-2 text-sm font-semibold text-gray-900">{clusterKey}</span>
                      {requested > 0 && (
                        <span className={`ml-4 inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                          healthy === total ? 'bg-green-100 text-green-800' :
                          healthy > 0 ? 'bg-yellow-100 text-yellow-800' :
                          'bg-red-100 text-red-800'
                        }`}>
                          {healthy}/{total} workers RDMA healthy
                        </span>
                      )}
                    </div>

                    {requested === 0 ? (
                      <p className="text-sm text-gray-500">No workers configured with RDMA</p>
                    ) : (
                      <div className="overflow-x-auto">
                        <table className="min-w-full text-sm">
                          <thead>
                            <tr className="border-b border-gray-200">
                              <th className="text-left py-2 pr-4 text-gray-500 font-medium">Worker Pod</th>
                              <th className="text-left py-2 pr-4 text-gray-500 font-medium">Node</th>
                              <th className="text-left py-2 pr-4 text-gray-500 font-medium">RDMA Status</th>
                              <th className="text-left py-2 text-gray-500 font-medium">Active/Total</th>
                            </tr>
                          </thead>
                          <tbody>
                            {workers.filter(w => w.rdmaRequested).map(w => (
                              <tr key={w.podName} className="border-b border-gray-100 hover:bg-gray-50">
                                <td className="py-2 pr-4 font-mono text-xs text-gray-900 truncate max-w-xs">
                                  {w.podName}
                                </td>
                                <td className="py-2 pr-4 text-xs text-gray-500 font-mono">{w.nodeName}</td>
                                <td className="py-2 pr-4">
                                  <RdmaStatusBadge available={w.rdmaAvailable} requested={w.rdmaRequested} />
                                </td>
                                <td className="py-2 text-xs text-gray-500 font-mono">
                                  {w.rdmaAllocated}/{w.nodeRdmaDevices}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </div>
              )
            })
          )}
        </div>
      )}
    </div>
  )
}

function RdmaStatusBadge({ available, requested }: { available: boolean; requested: boolean }) {
  if (!requested) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-500">
        N/A
      </span>
    )
  }
  return available ? (
    <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-800">
      Available
    </span>
  ) : (
    <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-800">
      Unavailable
    </span>
  )
}
