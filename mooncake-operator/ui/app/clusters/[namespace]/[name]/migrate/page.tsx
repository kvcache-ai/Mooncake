'use client'

import { useEffect, useState } from 'react'

interface PodInfo {
  metadata: { name: string; uid: string }
  spec: { nodeName: string }
  status: { phase: string; podIP: string }
}

interface DrainStatusEntry {
  status: string
  jobId?: string
  migratedBytes?: number
  succeededUnits?: number
  failedUnits?: number
  message?: string
  error?: string
}

interface MigrationJob {
  podIP: string
  podName: string
  jobId: string
  status: string
  migratedBytes: number
  succeededUnits: number
  failedUnits: number
  message: string
  error?: string
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

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  return `${(bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0)} ${units[i]}`
}

export default function MigratePage({
  params,
}: {
  params: { namespace: string; name: string }
}) {
  const { namespace, name } = params

  // Worker pods
  const [workerPods, setWorkerPods] = useState<PodInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Selection
  const [selectedSources, setSelectedSources] = useState<Set<string>>(new Set())
  const [selectedTargets, setSelectedTargets] = useState<Set<string>>(new Set())

  // Config
  const [maxConcurrency, setMaxConcurrency] = useState(4)
  const [bandwidthMBPS, setBandwidthMBPS] = useState(0)
  const [timeoutSeconds, setTimeoutSeconds] = useState(300)

  // Migration state
  const [migrating, setMigrating] = useState(false)
  const [migrationJobs, setMigrationJobs] = useState<MigrationJob[]>([])
  const [started, setStarted] = useState(false)

  // Drain statuses for all pods
  const [drainStatuses, setDrainStatuses] = useState<Record<string, DrainStatusEntry>>({})

  // Fetch worker pods with drain statuses
  const fetchData = async () => {
    try {
      const [podsRes, drainRes] = await Promise.all([
        fetch(`/api/clusters/${namespace}/${name}/pods`),
        fetch(`/api/clusters/${namespace}/${name}/drain-status`),
      ])
      const podsData = await podsRes.json()
      const drainData = await drainRes.json()

      const allPods: PodInfo[] = podsData.pods || []
      const workers = allPods.filter(p => p.metadata.name.includes('-worker-') && p.status.phase === 'Running')

      setWorkerPods(workers)
      if (drainData.statuses) setDrainStatuses(drainData.statuses)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { fetchData() }, [namespace, name])

  // Auto-refresh drain statuses during migration
  useEffect(() => {
    if (!started || migrationJobs.length === 0) return
    const interval = setInterval(async () => {
      try {
        const drainRes = await fetch(`/api/clusters/${namespace}/${name}/drain-status`)
        const drainData = await drainRes.json()
        if (drainData.statuses) setDrainStatuses(drainData.statuses)
      } catch {
        // ignore
      }
    }, 3000)
    return () => clearInterval(interval)
  }, [started, migrationJobs.length, namespace, name])

  // Update migrationJobs when drainStatuses changes
  useEffect(() => {
    if (!started) return
    setMigrationJobs(prev => prev.map(job => {
      const ds = drainStatuses[job.podIP]
      if (!ds) return job
      return {
        ...job,
        status: ds.status,
        migratedBytes: ds.migratedBytes || 0,
        succeededUnits: ds.succeededUnits || 0,
        failedUnits: ds.failedUnits || 0,
        message: ds.message || '',
        error: ds.error,
      }
    }))
  }, [drainStatuses, started])

  const handleSourceToggle = (ip: string) => {
    setSelectedSources(prev => {
      const next = new Set(prev)
      if (next.has(ip)) next.delete(ip)
      else next.add(ip)
      return next
    })
    // Clear target if it overlaps with new source
    setSelectedTargets(prev => {
      if (prev.has(ip)) {
        const next = new Set(prev)
        next.delete(ip)
        return next
      }
      return prev
    })
  }

  const handleTargetToggle = (ip: string) => {
    if (selectedSources.has(ip)) return
    setSelectedTargets(prev => {
      const next = new Set(prev)
      if (next.has(ip)) next.delete(ip)
      else next.add(ip)
      return next
    })
  }

  const handleSelectAllSources = () => {
    const all = new Set(workerPods.map(p => p.status.podIP).filter(ip => ip))
    // Exclude pods that are already draining
    for (const ip of all) {
      const ds = drainStatuses[ip]
      if (ds && ['CREATED', 'PLANNING', 'RUNNING'].includes(ds.status)) {
        all.delete(ip)
      }
    }
    setSelectedSources(all)
    // Remove overlapping targets
    setSelectedTargets(prev => {
      const next = new Set(prev)
      for (const ip of all) next.delete(ip)
      return next
    })
  }

  const handleSelectAllTargets = () => {
    const all = new Set(workerPods.map(p => p.status.podIP).filter(ip => ip))
    for (const ip of selectedSources) all.delete(ip)
    setSelectedTargets(all)
  }

  const handleStartMigration = async () => {
    if (selectedSources.size === 0) return
    if (!confirm(`Migrate data of ${selectedSources.size} source worker(s) to ${selectedTargets.size > 0 ? selectedTargets.size + ' target node(s)' : 'target nodes selected by Master'}?`)) return

    setMigrating(true)
    setStarted(true)
    const targetIPs = selectedTargets.size > 0 ? Array.from(selectedTargets) : undefined
    const jobs: MigrationJob[] = []

    for (const srcIP of selectedSources) {
      const pod = workerPods.find(p => p.status.podIP === srcIP)
      try {
        const res = await fetch(`/api/clusters/${namespace}/${name}/drain-worker`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            podIP: srcIP,
            maxConcurrency,
            bandwidthMBPS,
            targetIPs,
          }),
        })
        const data = await res.json()
        if (!res.ok) throw new Error(data.error || 'Failed to start migration')
        jobs.push({
          podIP: srcIP,
          podName: pod?.metadata.name || srcIP,
          jobId: data.jobId,
          status: 'CREATED',
          migratedBytes: 0,
          succeededUnits: 0,
          failedUnits: 0,
          message: '',
        })
      } catch (e: any) {
        jobs.push({
          podIP: srcIP,
          podName: pod?.metadata.name || srcIP,
          jobId: '',
          status: 'FAILED',
          migratedBytes: 0,
          succeededUnits: 0,
          failedUnits: 0,
          message: '',
          error: e.message,
        })
      }
    }

    setMigrationJobs(jobs)
    setMigrating(false)
  }

  const handleCancelAll = async () => {
    for (const job of migrationJobs) {
      if (!job.jobId || ['SUCCEEDED', 'FAILED', 'CANCELED'].includes(job.status)) continue
      try {
        await fetch(`/api/clusters/${namespace}/${name}/cancel-drain`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ jobId: job.jobId }),
        })
      } catch { /* ignore */ }
    }
  }

  const completedCount = migrationJobs.filter(j =>
    ['SUCCEEDED', 'FAILED', 'CANCELED'].includes(j.status)
  ).length
  const succeededCount = migrationJobs.filter(j => j.status === 'SUCCEEDED').length
  const totalCount = migrationJobs.length
  const hasActiveJobs = migrationJobs.some(j =>
    ['CREATED', 'PLANNING', 'RUNNING'].includes(j.status)
  )

  const isPodDraining = (ip: string) => {
    const ds = drainStatuses[ip]
    return ds && ['CREATED', 'PLANNING', 'RUNNING'].includes(ds.status)
  }

  // ---------- Loading state ----------
  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <div className="text-gray-500">Loading cluster info...</div>
      </div>
    )
  }

  // ---------- Error state ----------
  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
        Error loading cluster: {error}
        <button onClick={fetchData} className="ml-3 underline text-red-600 hover:text-red-800">Retry</button>
      </div>
    )
  }

  // ---------- Empty state ----------
  if (workerPods.length === 0) {
    return (
      <div>
        <a href={`/clusters/${namespace}/${name}`} className="text-sm text-indigo-600 hover:text-indigo-800">&larr; Back to Cluster</a>
        <h1 className="text-2xl font-semibold text-gray-900 mt-1">Data Migration - {name}</h1>
        <p className="text-sm text-gray-500 mt-1">Namespace: {namespace}</p>
        <div className="mt-8 bg-yellow-50 border border-yellow-200 text-yellow-700 px-4 py-3 rounded">
          No running Worker Pods in this cluster. Migration cannot proceed.
        </div>
      </div>
    )
  }

  return (
    <div>
      {/* Header */}
      <a href={`/clusters/${namespace}/${name}`} className="text-sm text-indigo-600 hover:text-indigo-800">&larr; Back to Cluster</a>
      <h1 className="text-2xl font-semibold text-gray-900 mt-1">Data Migration - {name}</h1>
      <p className="text-sm text-gray-500 mt-1">Namespace: {namespace} | Workers: {workerPods.length}</p>

      {/* ---------- Configuration area (hidden after migration starts) ---------- */}
      {!started && (
        <div className="mt-6 space-y-6">
          {/* Source workers */}
          <div className="bg-white shadow sm:rounded-lg">
            <div className="px-4 py-5 sm:px-6 flex justify-between items-center">
              <div>
                <h3 className="text-lg leading-6 font-medium text-gray-900">Source Workers</h3>
                <p className="mt-1 text-sm text-gray-500">Select workers to drain data from</p>
              </div>
              <button
                type="button"
                onClick={handleSelectAllSources}
                className="text-sm text-indigo-600 hover:text-indigo-800"
              >
                Select All
              </button>
            </div>
            <div className="border-t border-gray-200">
              {workerPods.map(p => {
                const ip = p.status.podIP
                if (!ip) return null
                const draining = isPodDraining(ip)
                const checked = selectedSources.has(ip)
                return (
                  <label
                    key={p.metadata.uid}
                    className={`flex items-center px-4 py-3 hover:bg-gray-50 cursor-pointer ${checked ? 'bg-indigo-50' : ''} ${draining ? 'opacity-50 cursor-not-allowed' : ''}`}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      disabled={draining}
                      onChange={() => handleSourceToggle(ip)}
                      className="focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300 rounded"
                    />
                    <div className="ml-3 flex-1">
                      <span className="text-sm font-medium text-gray-900">{p.metadata.name}</span>
                      <span className="ml-2 text-xs text-gray-500">IP: {ip}</span>
                      <span className="ml-2 text-xs text-gray-400">Node: {p.spec.nodeName}</span>
                    </div>
                    {draining && (
                      <DrainStatusBadge status={drainStatuses[ip]?.status || 'RUNNING'} />
                    )}
                  </label>
                )
              })}
              {selectedSources.size === 0 && (
                <p className="px-4 py-3 text-sm text-gray-400">Select at least one source worker</p>
              )}
            </div>
          </div>

          {/* Target workers */}
          <div className="bg-white shadow sm:rounded-lg">
            <div className="px-4 py-5 sm:px-6 flex justify-between items-center">
              <div>
                <h3 className="text-lg leading-6 font-medium text-gray-900">Target Workers</h3>
                <p className="mt-1 text-sm text-gray-500">Select target nodes for migration (leave empty for auto-selection by Master)</p>
              </div>
              <button
                type="button"
                onClick={handleSelectAllTargets}
                className="text-sm text-indigo-600 hover:text-indigo-800"
              >
                Select All
              </button>
            </div>
            <div className="border-t border-gray-200">
              {workerPods.map(p => {
                const ip = p.status.podIP
                if (!ip) return null
                const isSource = selectedSources.has(ip)
                const checked = selectedTargets.has(ip)
                return (
                  <label
                    key={p.metadata.uid}
                    className={`flex items-center px-4 py-3 hover:bg-gray-50 cursor-pointer ${checked ? 'bg-indigo-50' : ''} ${isSource ? 'opacity-40 cursor-not-allowed' : ''}`}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      disabled={isSource}
                      onChange={() => handleTargetToggle(ip)}
                      className="focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300 rounded"
                    />
                    <div className="ml-3 flex-1">
                      <span className="text-sm font-medium text-gray-900">{p.metadata.name}</span>
                      <span className="ml-2 text-xs text-gray-500">IP: {ip}</span>
                      <span className="ml-2 text-xs text-gray-400">Node: {p.spec.nodeName}</span>
                    </div>
                    {isSource && <span className="text-xs text-gray-400">(selected as source)</span>}
                  </label>
                )
              })}
              {selectedTargets.size === 0 && (
                <p className="px-4 py-3 text-sm text-gray-400">No targets selected. Master will auto-select.</p>
              )}
            </div>
          </div>

          {/* Migration config */}
          <div className="bg-white shadow sm:rounded-lg">
            <div className="px-4 py-5 sm:px-6">
              <h3 className="text-lg leading-6 font-medium text-gray-900">Migration Parameters</h3>
            </div>
            <div className="border-t border-gray-200 px-4 py-5 sm:p-6">
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label htmlFor="maxConcurrency" className="block text-sm font-medium text-gray-700">Max Concurrency</label>
                  <input
                    type="number"
                    id="maxConcurrency"
                    value={maxConcurrency}
                    onChange={e => setMaxConcurrency(parseInt(e.target.value) || 4)}
                    className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
                    min={1} max={64}
                  />
                  <p className="mt-1 text-xs text-gray-400">Objects transferred concurrently (1-64)</p>
                </div>
                <div>
                  <label htmlFor="bandwidthMBPS" className="block text-sm font-medium text-gray-700">Bandwidth Limit (Mbps)</label>
                  <input
                    type="number"
                    id="bandwidthMBPS"
                    value={bandwidthMBPS}
                    onChange={e => setBandwidthMBPS(parseInt(e.target.value) || 0)}
                    className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
                    min={0}
                  />
                  <p className="mt-1 text-xs text-gray-400">0 = unlimited</p>
                </div>
                <div>
                  <label htmlFor="timeoutSeconds" className="block text-sm font-medium text-gray-700">Timeout (s)</label>
                  <input
                    type="number"
                    id="timeoutSeconds"
                    value={timeoutSeconds}
                    onChange={e => setTimeoutSeconds(parseInt(e.target.value) || 300)}
                    className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
                    min={30}
                  />
                  <p className="mt-1 text-xs text-gray-400">Timeout per drain job</p>
                </div>
              </div>
            </div>
          </div>

          {/* Start button */}
          <div className="flex justify-end space-x-3">
            <a
              href={`/clusters/${namespace}/${name}`}
              className="bg-white py-2 px-4 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
              Cancel
            </a>
            <button
              type="button"
              onClick={handleStartMigration}
              disabled={selectedSources.size === 0 || migrating}
              className="inline-flex justify-center py-2 px-6 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {migrating ? 'Starting...' : 'Start Migration'}
            </button>
          </div>
        </div>
      )}

      {/* ---------- Progress area (shown after migration starts) ---------- */}
      {started && (
        <div className="mt-6 space-y-4">
          {/* Summary */}
          <div className="bg-white shadow sm:rounded-lg p-6">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-lg font-medium text-gray-900">Migration Progress</h3>
                <p className="text-sm text-gray-500 mt-1">
                  {succeededCount} / {totalCount} workers completed
                </p>
              </div>
              <div className="flex items-center space-x-4">
                {hasActiveJobs && (
                  <button
                    type="button"
                    onClick={handleCancelAll}
                    className="inline-flex items-center px-3 py-1.5 border border-red-300 text-sm font-medium rounded-md text-red-700 bg-white hover:bg-red-50"
                  >
                    Cancel All
                  </button>
                )}
                {!hasActiveJobs && (
                  <button
                    type="button"
                    onClick={() => { setStarted(false); setMigrationJobs([]); setSelectedSources(new Set()); setSelectedTargets(new Set()) }}
                    className="inline-flex items-center px-3 py-1.5 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50"
                  >
                    Back to Config
                  </button>
                )}
              </div>
            </div>

            {/* Progress bar */}
            {totalCount > 0 && (
              <div className="mt-4 bg-gray-200 rounded-full h-3">
                <div
                  className="bg-indigo-600 h-3 rounded-full transition-all duration-500"
                  style={{ width: `${(completedCount / totalCount) * 100}%` }}
                />
              </div>
            )}
          </div>

          {/* Per-worker status */}
          <div className="bg-white shadow sm:rounded-lg">
            <div className="border-t border-gray-200">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Worker</th>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">IP</th>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Data Migrated</th>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Objects</th>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Details</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {migrationJobs.map(job => (
                    <tr key={job.podIP} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{job.podName}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{job.podIP}</td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <DrainStatusBadge status={job.status} />
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {formatBytes(job.migratedBytes)}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        <span className="text-green-600">{job.succeededUnits}</span>
                        {job.failedUnits > 0 && (
                          <span className="text-red-600 ml-1">/ {job.failedUnits} failed</span>
                        )}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {job.error && <span className="text-red-500" title={job.error}>Error: {job.error}</span>}
                        {job.message && <span className="text-gray-400">{job.message}</span>}
                        {!job.error && !job.message && job.jobId && (
                          <span className="text-xs text-gray-400">Job: {job.jobId.substring(0, 12)}...</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Config summary */}
          <div className="text-xs text-gray-400 space-x-4">
            <span>Max Concurrency: {maxConcurrency}</span>
            <span>Bandwidth: {bandwidthMBPS > 0 ? `${bandwidthMBPS} Mbps` : 'unlimited'}</span>
            <span>Timeout: {timeoutSeconds}s</span>
            {selectedTargets.size > 0 && (
              <span>Targets: {Array.from(selectedTargets).join(', ')}</span>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
