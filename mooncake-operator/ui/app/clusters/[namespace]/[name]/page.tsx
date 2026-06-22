'use client'

import { useEffect, useState } from 'react'

interface Cluster {
  metadata: { name: string; namespace: string; creationTimestamp: string; uid: string }
  spec: {
    image?: string
    master?: {
      replicas?: number
      metricsPort?: number
      rpcPort?: number
      rpcThreadNum?: number
      httpMetadataServerPort?: number
      enableHTTPMetadataServer?: boolean
      configOverrides?: Record<string, string>
    }
    workers?: {
      replicas?: number
      rdmaEnabled?: boolean
      gpuEnabled?: boolean
      segmentSize?: string
      rdmaPortRange?: string
      memoryAllocator?: string
      allocationStrategy?: string
      configOverrides?: Record<string, string>
    }
    ha?: { type?: string; connectionString?: string; etcdEndpoints?: string }
    snapshot?: { enabled?: boolean; intervalSeconds?: number }
    offload?: { enabled?: boolean }
    eviction?: { ratio?: number; highWatermarkRatio?: number }
  }
  status?: {
    phase?: string
    masterReady?: number
    workerReady?: number
    leaderNode?: string
    autoDrainJobs?: AutoDrainJob[]
    conditions?: Array<{ type: string; status: string; reason: string; message: string; lastTransitionTime: string }>
    observedGeneration?: number
  }
}

interface PodInfo {
  metadata: { name: string; uid: string }
  spec: {
    nodeName: string
    containers: Array<{
      name: string
      resources?: { requests?: { cpu?: string; memory?: string }; limits?: { cpu?: string; memory?: string } }
      ports?: Array<{ name: string; containerPort: number }>
    }>
  }
  status: { phase: string; podIP: string; conditions?: Array<{ type: string; status: string }> }
}

interface PodMetric {
  metadata: { name: string }
  containers: Array<{ name: string; usage: { cpu: string; memory: string } }>
}

interface MasterStat {
  name: string
  node: string
  phase: string
  podIP: string
  health: { status: string; role: string; ha_state: string; service_ready: boolean; leader_address?: string } | null
  summary: string | null
  role: string | null
  leader: { present: boolean; leader_address?: string; view_version?: number } | null
}

interface SegmentStat {
  [ip: string]: { used: number; capacity: number }
}

interface AutoDrainJob {
  podName: string
  podIP: string
  segmentName: string
  jobId: string
  status: string
  migratedBytes: number
  speedMbps: number
  succeededUnits: number
  failedUnits: number
  error?: string
  createdAt: string
}

function DrainStatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    CREATED: 'bg-blue-100 text-blue-800',
    PLANNING: 'bg-yellow-100 text-yellow-800',
    RUNNING: 'bg-yellow-100 text-yellow-800',
    SUCCEEDED: 'bg-green-100 text-green-800',
    FAILED: 'bg-red-100 text-red-800',
    CANCELED: 'bg-gray-100 text-gray-600',
  }
  const color = colors[status] || 'bg-gray-100 text-gray-600'
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${color}`}>
      {status}
    </span>
  )
}

function formatSpeed(mbps: number): string {
  if (mbps <= 0) return '-'
  if (mbps < 1) return `${(mbps * 1000).toFixed(0)} Kbps`
  if (mbps < 1000) return `${mbps.toFixed(1)} Mbps`
  return `${(mbps / 1000).toFixed(2)} Gbps`
}


function Toast({ message, onClose }: { message: string; onClose: () => void }) {
  useEffect(() => {
    const t = setTimeout(onClose, 4000)
    return () => clearTimeout(t)
  }, [message, onClose])
  return (
    <div className="fixed top-4 right-4 z-50 bg-gray-800 text-white px-4 py-3 rounded shadow-lg text-sm max-w-md">
      {message}
      <button onClick={onClose} className="ml-3 text-gray-300 hover:text-white">&times;</button>
    </div>
  )
}

function PhaseBadge({ phase }: { phase: string | undefined }) {
  const colors: Record<string, string> = {
    Running: 'bg-green-100 text-green-800',
    Creating: 'bg-yellow-100 text-yellow-800',
    Updating: 'bg-blue-100 text-blue-800',
    Failed: 'bg-red-100 text-red-800',
    Deleting: 'bg-gray-100 text-gray-800',
  }
  const color = colors[phase || ''] || 'bg-gray-100 text-gray-800'
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${color}`}>
      {phase || 'Unknown'}
    </span>
  )
}

function PodStatusBadge({ phase }: { phase: string }) {
  const colors: Record<string, string> = {
    Running: 'bg-green-100 text-green-800',
    Pending: 'bg-yellow-100 text-yellow-800',
    Succeeded: 'bg-blue-100 text-blue-800',
    Failed: 'bg-red-100 text-red-800',
    Unknown: 'bg-gray-100 text-gray-800',
  }
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${colors[phase] || colors.Unknown}`}>
      {phase}
    </span>
  )
}

function MasterRoleBadge({ role }: { role: string | null }) {
  if (!role) return <span className="text-xs text-gray-400">-</span>
  const isLeader = role === 'leader' || role === 'primary'
  const color = isLeader ? 'bg-purple-100 text-purple-800' : 'bg-gray-100 text-gray-800'
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${color}`}>
      {role}
    </span>
  )
}

function parseSummary(summary: string | null): {
  memUsed: string; memCapacity: string; keys: string; clients: string; ssdUsed: string; ssdCapacity: string
} | null {
  if (!summary) return null
  const memMatch = summary.match(/Mem Storage:\s*([^|]+)\s*\/\s*([^|)]+)/)
  const keysMatch = summary.match(/Keys:\s*([^|(]+)/)
  const clientsMatch = summary.match(/Clients:\s*(\d+)/)
  const ssdMatch = summary.match(/SSD Storage:\s*([^|]+)\s*\/\s*([^|}]+)/)
  return {
    memUsed: memMatch?.[1]?.trim() || '-',
    memCapacity: memMatch?.[2]?.trim() || '-',
    keys: keysMatch?.[1]?.trim() || '0',
    clients: clientsMatch?.[1] || '0',
    ssdUsed: ssdMatch?.[1]?.trim() || '-',
    ssdCapacity: ssdMatch?.[2]?.trim() || '-',
  }
}

// Parse K8s quantity strings (e.g., "100m", "1Gi", "128Mi") to a number in the base unit
function parseResourceQuantity(q: string | undefined): number {
  if (!q) return 0
  const match = q.match(/^(\d+(?:\.\d+)?)\s*(m|Ki|Mi|Gi|Ti|k|M|G|T)?$/)
  if (!match) return 0
  const val = parseFloat(match[1])
  const suffix = match[2]
  if (!suffix) return val
  switch (suffix) {
    case 'm': return val / 1000      // milli-CPU
    case 'Ki': return val * 1024
    case 'Mi': return val * 1024 * 1024
    case 'Gi': return val * 1024 * 1024 * 1024
    case 'Ti': return val * 1024 * 1024 * 1024 * 1024
    case 'k': return val * 1000
    case 'M': return val * 1000 * 1000
    case 'G': return val * 1000 * 1000 * 1000
    case 'T': return val * 1000 * 1000 * 1000 * 1000
    default: return val
  }
}

function formatCpu(q: string | undefined): string {
  if (!q) return '-'
  const val = parseResourceQuantity(q)
  if (val < 1) return `${Math.round(val * 1000)}m`
  return val.toFixed(2)
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  return `${(bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0)} ${units[i]}`
}

function ResourceBar({ usage, limit, label }: { usage: number; limit: number; label: string }) {
  if (!limit) {
    return <span className="text-xs text-gray-500">{label}</span>
  }
  const pct = Math.min((usage / limit) * 100, 100)
  const color = pct > 90 ? 'bg-red-500' : pct > 70 ? 'bg-yellow-500' : 'bg-green-500'
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 bg-gray-200 rounded-full h-2 min-w-[60px]">
        <div className={`${color} h-2 rounded-full transition-all`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-gray-600 whitespace-nowrap">{pct.toFixed(0)}%</span>
    </div>
  )
}

function containerResources(containers: PodInfo['spec']['containers']) {
  const c = containers?.[0]
  return {
    cpuLimit: parseResourceQuantity(c?.resources?.limits?.cpu),
    cpuRequest: parseResourceQuantity(c?.resources?.requests?.cpu),
    memLimit: parseResourceQuantity(c?.resources?.limits?.memory),
    memRequest: parseResourceQuantity(c?.resources?.requests?.memory),
  }
}

function containerUsage(containers: PodMetric['containers']) {
  const c = containers?.[0]
  return {
    cpuUsage: parseResourceQuantity(c?.usage?.cpu),
    memUsage: parseResourceQuantity(c?.usage?.memory),
  }
}

export default function ClusterDetailPage({
  params,
}: {
  params: { namespace: string; name: string }
}) {
  const { namespace, name } = params
  const [cluster, setCluster] = useState<Cluster | null>(null)
  const [pods, setPods] = useState<PodInfo[]>([])
  const [masterStats, setMasterStats] = useState<MasterStat[]>([])
  const [podMetrics, setPodMetrics] = useState<PodMetric[]>([])
  const [segmentStats, setSegmentStats] = useState<SegmentStat>({})
  const [toast, setToast] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchData = (silent?: boolean) => {
    if (!silent) {
      setLoading(true)
      setError(null)
    }
    Promise.all([
      fetch(`/api/clusters/${namespace}/${name}`).then(r => r.json()),
      fetch(`/api/clusters/${namespace}/${name}/pods`).then(r => r.json()),
      fetch(`/api/clusters/${namespace}/${name}/master-stats`).then(r => r.json()),
      fetch(`/api/clusters/${namespace}/${name}/pod-metrics`).then(r => r.json()),
      fetch(`/api/clusters/${namespace}/${name}/worker-segments`).then(r => r.json()),
    ])
      .then(([clusterData, podsData, statsData, metricsData, segmentsData]) => {
        if (clusterData.error) throw new Error(clusterData.error)
        setCluster(clusterData.cluster)
        setPods(podsData.pods || [])
        setMasterStats(statsData.stats || [])
        setPodMetrics(metricsData.metrics || [])
        setSegmentStats(segmentsData.segments || {})
      })
      .catch(e => {
        if (!silent) setError(e.message)
      })
      .finally(() => {
        if (!silent) setLoading(false)
      })
  }

  useEffect(() => { fetchData() }, [namespace, name])

  // Auto-refresh cluster data every 5s
  useEffect(() => {
    const interval = setInterval(() => fetchData(true), 5000)
    return () => clearInterval(interval)
  }, [namespace, name])

  const handleDelete = async () => {
    if (!confirm(`Delete cluster "${name}" in namespace "${namespace}"?`)) return
    try {
      await fetch(`/api/clusters/${namespace}/${name}`, { method: 'DELETE' })
      window.location.href = '/clusters'
    } catch (e: any) {
      alert('Failed to delete: ' + e.message)
    }
  }

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <div className="text-gray-500">Loading cluster...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
        Error loading cluster: {error}
        <button onClick={() => fetchData()} className="ml-3 underline text-red-600 hover:text-red-800">Retry</button>
      </div>
    )
  }

  if (!cluster) {
    return <div className="text-gray-500 py-12 text-center">Cluster not found.</div>
  }

  const spec = cluster.spec
  const status = cluster.status || {}
  const masterReplicas = spec?.master?.replicas || 1
  const workerReplicas = spec?.workers?.replicas || 0
  const masterReady = status.masterReady ?? 0
  const workerReady = status.workerReady ?? 0

  // Determine leader from master stats (health.role === "leader" or "primary")
  const leaderPod = masterStats.find(s => s.health?.role === 'leader' || s.health?.role === 'primary')
  const leaderDisplay = leaderPod?.name || status.leaderNode || '-'

  // Filter to only worker pods for the pods table
  const workerPods = pods.filter(p => p.metadata.name.includes('-worker-'))

  // Auto-drain jobs from cluster status
  const autoDrainJobs: AutoDrainJob[] = status.autoDrainJobs || []

  return (
    <div>
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <a href="/clusters" className="text-sm text-indigo-600 hover:text-indigo-800">&larr; Back to clusters</a>
          <h1 className="text-2xl font-semibold text-gray-900 mt-1">{name}</h1>
          <p className="text-sm text-gray-500">Namespace: {namespace}</p>
        </div>
        <div className="flex space-x-3">
          <a
            href={`/clusters/${namespace}/${name}/migrate`}
            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700"
          >
            Migrate
          </a>
          <a
            href={`/clusters/${namespace}/${name}/edit`}
            className="inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50"
          >
            Edit
          </a>
          <a
            href={`/clusters/${namespace}/${name}/test`}
            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700"
          >
            Test
          </a>
          <a
            href={`/clusters/${namespace}/${name}/benchmark`}
            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700"
          >
            Benchmark
          </a>
          <button
            type="button"
            onClick={handleDelete}
            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-red-600 hover:bg-red-700"
          >
            Delete
          </button>
        </div>
      </div>

      {toast && <Toast message={toast} onClose={() => setToast(null)} />}

      {/* Summary cards */}
      <div className="mt-6 grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
        <div className="bg-white overflow-hidden shadow rounded-lg p-5">
          <dt className="text-sm font-medium text-gray-500">Phase</dt>
          <dd className="mt-1"><PhaseBadge phase={status.phase} /></dd>
        </div>
        <div className="bg-white overflow-hidden shadow rounded-lg p-5">
          <dt className="text-sm font-medium text-gray-500">Masters Ready</dt>
          <dd className="mt-1 text-lg font-semibold text-gray-900">{masterReady}/{masterReplicas}</dd>
        </div>
        <div className="bg-white overflow-hidden shadow rounded-lg p-5">
          <dt className="text-sm font-medium text-gray-500">Workers Ready</dt>
          <dd className="mt-1 text-lg font-semibold text-gray-900">{workerReady}/{workerReplicas}</dd>
        </div>
        <div className="bg-white overflow-hidden shadow rounded-lg p-5">
          <dt className="text-sm font-medium text-gray-500">Leader</dt>
          <dd className="mt-1 text-lg font-semibold text-gray-900 text-sm truncate" title={leaderDisplay}>{leaderDisplay}</dd>
        </div>
      </div>

      {/* Master Capacity */}
      {masterStats.length > 0 && (
        <div className="mt-6 bg-white shadow sm:rounded-lg">
          <div className="px-4 py-5 sm:px-6">
            <h3 className="text-lg leading-6 font-medium text-gray-900">Master Capacity</h3>
          </div>
          <div className="border-t border-gray-200">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Pod</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Role</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Mem Storage</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">SSD Storage</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Keys</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Clients</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {masterStats.map(s => {
                  const parsed = parseSummary(s.summary)
                  return (
                    <tr key={s.name}>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{s.name}</td>
                      <td className="px-6 py-4 whitespace-nowrap"><MasterRoleBadge role={s.health?.role || s.role} /></td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {parsed ? `${parsed.memUsed} / ${parsed.memCapacity}` : '-'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {parsed && parsed.ssdUsed !== '-' ? `${parsed.ssdUsed} / ${parsed.ssdCapacity}` : '-'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{parsed?.keys || '-'}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{parsed?.clients || '-'}</td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <PodStatusBadge phase={s.phase} />
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Pods table */}
      <div className="mt-6 bg-white shadow sm:rounded-lg">
        <div className="px-4 py-5 sm:px-6">
          <h3 className="text-lg leading-6 font-medium text-gray-900">Pods</h3>
        </div>
        <div className="border-t border-gray-200 overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Role</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Node</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">CPU</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Memory</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Segment</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {workerPods.length === 0 ? (
                <tr>
                  <td colSpan={8} className="px-6 py-12 text-center text-sm text-gray-500">No pods found.</td>
                </tr>
              ) : (
                workerPods.map(p => {
                  const ms = masterStats.find(s => s.name === p.metadata.name)
                  const metric = podMetrics.find(m => m.metadata.name === p.metadata.name)
                  const res = containerResources(p.spec.containers)
                  const { cpuUsage, memUsage } = containerUsage(metric?.containers || [])
                  const handleDeletePod = async () => {
                    if (!confirm(`Delete pod "${p.metadata.name}"?`)) return
                    try {
                      const res = await fetch(`/api/clusters/${namespace}/${name}/delete-pod`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ podName: p.metadata.name }),
                      })
                      const data = await res.json()
                      if (!res.ok) throw new Error(data.error || 'Failed to delete pod')
                      setToast(`Pod "${p.metadata.name}" deleted`)
                      fetchData(true)
                    } catch (e: any) {
                      setToast('Delete failed: ' + e.message)
                    }
                  }
                  return (
                    <tr key={p.metadata.uid} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{p.metadata.name}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">worker</span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap"><PodStatusBadge phase={p.status.phase} /></td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{p.spec.nodeName}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm min-w-[120px]">
                        {metric ? (
                          <div className="flex flex-col gap-0.5">
                            <ResourceBar usage={cpuUsage} limit={res.cpuLimit} label={formatCpu(metric?.containers?.[0]?.usage?.cpu)} />
                            <span className="text-[10px] text-gray-400">{formatCpu(p.spec.containers?.[0]?.resources?.limits?.cpu || p.spec.containers?.[0]?.resources?.requests?.cpu)} limit</span>
                          </div>
                        ) : <span className="text-xs text-gray-400">-</span>}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm min-w-[140px]">
                        {metric ? (
                          <div className="flex flex-col gap-0.5">
                            <ResourceBar usage={memUsage} limit={res.memLimit} label={formatBytes(memUsage)} />
                            <span className="text-[10px] text-gray-400">{formatBytes(res.memLimit || res.memRequest)} limit</span>
                          </div>
                        ) : <span className="text-xs text-gray-400">-</span>}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm min-w-[140px]">
                        {(() => {
                          const seg = segmentStats[p.status.podIP]
                          return seg ? (
                            <div className="flex flex-col gap-0.5">
                              <ResourceBar usage={seg.used} limit={seg.capacity} label={formatBytes(seg.used)} />
                              <span className="text-[10px] text-gray-400">{formatBytes(seg.capacity)} limit</span>
                            </div>
                          ) : (
                            <span className="text-xs text-gray-400">-</span>
                          )
                        })()}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm">
                        <button
                          onClick={handleDeletePod}
                          className="inline-flex items-center px-2.5 py-1 border border-transparent text-xs font-medium rounded text-red-700 bg-red-100 hover:bg-red-200"
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        </div>
      </div>

      {autoDrainJobs.length > 0 && (
        <div className="mt-6 bg-white shadow sm:rounded-lg">
          <div className="px-4 py-5 sm:px-6">
            <h3 className="text-lg leading-6 font-medium text-gray-900">Data Migration in Progress</h3>
            <p className="mt-1 text-sm text-gray-500">Auto-migration of worker data during pod termination</p>
          </div>
          <div className="border-t border-gray-200">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Worker</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Segment</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Migrated</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Speed</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Objects</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {autoDrainJobs.map(job => (
                  <tr key={job.jobId} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{job.podName}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{job.segmentName}</td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <DrainStatusBadge status={job.status} />
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {formatBytes(job.migratedBytes)}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {job.status === 'RUNNING' || job.status === 'PLANNING' ? formatSpeed(job.speedMbps) : '-'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      <span className="text-green-600">{job.succeededUnits}</span>
                      {job.failedUnits > 0 && (
                        <span className="text-red-600 ml-1">/ {job.failedUnits} failed</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {autoDrainJobs.some(j => ['CREATED', 'PLANNING', 'RUNNING'].includes(j.status)) && (
              <div className="px-6 py-3 bg-gray-50 text-sm text-gray-500 border-t">
                Migration in progress — worker pods will terminate when data migration completes
              </div>
            )}
          </div>
        </div>
      )}

      {/* Configuration */}
      <div className="mt-6 bg-white shadow sm:rounded-lg">
        <div className="px-4 py-5 sm:px-6">
          <h3 className="text-lg leading-6 font-medium text-gray-900">Configuration</h3>
        </div>
        <div className="border-t border-gray-200 px-4 py-5 sm:p-0">
          <dl className="sm:divide-y sm:divide-gray-200">
            <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
              <dt className="text-sm font-medium text-gray-500">Image</dt>
              <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{spec.image || 'mooncake/mooncake-store:latest'}</dd>
            </div>
            <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
              <dt className="text-sm font-medium text-gray-500">HA Backend</dt>
              <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{spec.ha?.type || 'none'}
                {spec.ha?.etcdEndpoints && <span className="ml-2 text-gray-500">({spec.ha.etcdEndpoints})</span>}
              </dd>
            </div>
            <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
              <dt className="text-sm font-medium text-gray-500">Master Replicas</dt>
              <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{masterReplicas}</dd>
            </div>
            <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
              <dt className="text-sm font-medium text-gray-500">Worker Replicas</dt>
              <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{workerReplicas}</dd>
            </div>
            <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
              <dt className="text-sm font-medium text-gray-500">RDMA</dt>
              <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                {spec.workers?.rdmaEnabled ? 'Enabled' : 'Disabled'}
              </dd>
            </div>
            <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
              <dt className="text-sm font-medium text-gray-500">Segment Size</dt>
              <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{spec.workers?.segmentSize || '4Gi'}</dd>
            </div>
            {spec.workers?.rdmaPortRange && (
              <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                <dt className="text-sm font-medium text-gray-500">RDMA Port Range</dt>
                <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{spec.workers.rdmaPortRange}</dd>
              </div>
            )}
            <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
              <dt className="text-sm font-medium text-gray-500">GPU</dt>
              <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                {spec.workers?.gpuEnabled ? 'Enabled' : 'Disabled'}
              </dd>
            </div>
            {spec.master?.configOverrides && Object.keys(spec.master.configOverrides).length > 0 && (
              <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                <dt className="text-sm font-medium text-gray-500">Master Config Overrides</dt>
                <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                  <code className="text-xs bg-gray-100 p-1 rounded block whitespace-pre-wrap">
                    {JSON.stringify(spec.master.configOverrides, null, 2)}
                  </code>
                </dd>
              </div>
            )}
          </dl>
        </div>
      </div>
    </div>
  )
}
