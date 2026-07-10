'use client'

import { useEffect, useState } from 'react'

interface BenchmarkResult {
  test: string
  protocol: string
  loss_pct: string
  workload: string
  obj_size: number
  duration: number
  ops: number
  errors: number
  throughput_mbps: number
  ops_per_sec: number
  avg_latency_ms: number
}

export default function BenchmarkPage({
  params,
}: {
  params: { namespace: string; name: string }
}) {
  const { namespace, name } = params

  // Form state
  const [workload, setWorkload] = useState('put-only')
  const [objSize, setObjSize] = useState(4096)
  const [duration, setDuration] = useState(60)
  const [protocol, setProtocol] = useState('rdma')
  const [label, setLabel] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Job state
  const [jobName, setJobName] = useState<string | null>(null)
  const [jobStatus, setJobStatus] = useState<'idle' | 'Pending' | 'Running' | 'Succeeded' | 'Failed' | 'Expired'>('idle')
  const [logs, setLogs] = useState('')
  const [result, setResult] = useState<BenchmarkResult | null>(null)
  const [activeBenchmarks, setActiveBenchmarks] = useState<Array<{ name: string; status: string; created: string; completed?: string }>>([])

  // Fetch active benchmarks for this cluster
  useEffect(() => {
    const fetchBenchmarks = async () => {
      try {
        const res = await fetch(`/api/clusters/${namespace}/${name}/benchmarks`)
        const data = await res.json()
        if (data.benchmarks) setActiveBenchmarks(data.benchmarks)
      } catch (e) { /* ignore */ }
    }
    fetchBenchmarks()
    const interval = setInterval(fetchBenchmarks, 15000)
    return () => clearInterval(interval)
  }, [namespace, name])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setSubmitting(true)
    setError(null)
    setJobStatus('Pending')
    setLogs('')
    setJobName(null)
    setResult(null)

    try {
      const res = await fetch(`/api/clusters/${namespace}/${name}/benchmark`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ workload, objSize, duration, protocol, label: label || undefined }),
      })
      const contentType = res.headers.get('content-type') || ''
      if (!contentType.includes('application/json')) {
        // Server returned non-JSON (e.g., auth redirect or HTML error page)
        const text = await res.text()
        throw new Error(`Server returned ${res.status} ${res.statusText} (expected JSON, got ${contentType}). ${text.substring(0, 200)}`)
      }
      let data
      try {
        data = await res.json()
      } catch (parseErr: any) {
        // Empty body or invalid JSON — try to get the raw text for debugging
        const text = await res.text().catch(() => '')
        throw new Error(`Invalid JSON response (${res.status}): ${parseErr.message}. Raw body (${text.length} bytes): ${text.substring(0, 200)}`)
      }
      if (!res.ok) throw new Error(data.error || `Request failed: ${res.status}`)
      setJobName(data.jobName)
      if (!label) setLabel(data.label)
      setJobStatus('Pending')
    } catch (err: any) {
      // Provide a user-friendly hint for auth/session issues
      let msg = err.message
      if (msg.includes('Unexpected end of JSON input') || msg.includes('Unexpected token')) {
        msg += ' (Possible causes: session expired — try refreshing the page, or the cluster namespace/name may be incorrect in the URL)'
      }
      setError(msg)
      setJobStatus('idle')
    } finally {
      setSubmitting(false)
    }
  }

  const handleAbort = async () => {
    if (!jobName) return
    if (!confirm('Abort benchmark job "' + jobName + '"?')) return
    try {
      await fetch(`/api/clusters/${namespace}/${name}/benchmark/${jobName}`, { method: 'DELETE' })
      setJobStatus('idle')
      setJobName(null)
    } catch (e: any) {
      setError('Failed to abort: ' + e.message)
    }
  }

  // Poll job status — faster during Running for snappier result display
  useEffect(() => {
    if (!jobName) return
    if (jobStatus === 'Succeeded' || jobStatus === 'Failed' || jobStatus === 'Expired') return

    const pollInterval = jobStatus === 'Running' ? 1000 : 2000
    const interval = setInterval(async () => {
      try {
        const res = await fetch(`/api/clusters/${namespace}/${name}/benchmark/${jobName}`)
        const data = await res.json()
        if (data.logs) setLogs(data.logs)
        if (data.status) setJobStatus(data.status)
        if (data.result) setResult(data.result)
      } catch (e) { /* ignore transient poll failures */ }
    }, pollInterval)

    return () => clearInterval(interval)
  }, [jobName, jobStatus, namespace, name])

  const statusColor: Record<string, string> = {
    idle: '',
    Pending: 'text-yellow-700 bg-yellow-50 border-yellow-200',
    Running: 'text-blue-700 bg-blue-50 border-blue-200',
    Succeeded: 'text-green-700 bg-green-50 border-green-200',
    Failed: 'text-red-700 bg-red-50 border-red-200',
    Expired: 'text-gray-700 bg-gray-50 border-gray-200',
  }

  const statusLabel: Record<string, string> = {
    idle: '',
    Pending: 'Job created, waiting for pod to start...',
    Running: 'Benchmark running...',
    Succeeded: 'Benchmark completed successfully!',
    Failed: 'Benchmark failed',
    Expired: 'Job has expired',
  }

  const isActive = jobStatus === 'Running' || jobStatus === 'Pending'

  return (
    <div>
      <a href={`/clusters/${namespace}/${name}`} className="text-sm text-indigo-600 hover:text-indigo-800">
        &larr; Back to Cluster
      </a>
      <h1 className="text-2xl font-semibold text-gray-900 mt-2">Benchmark {name}</h1>
      <p className="text-sm text-gray-500 mt-1">Namespace: {namespace}</p>

      {error && (
        <div className="mt-4 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">{error}</div>
      )}

      {/* Configuration Form */}
      <form className="mt-6 space-y-6 bg-white shadow sm:rounded-lg p-6" onSubmit={handleSubmit}>
        {/* Workload Type */}
        <fieldset disabled={isActive}>
          <legend className="text-sm font-medium text-gray-700">Workload Type</legend>
          <div className="mt-2 space-y-2">
            <label className="inline-flex items-center">
              <input type="radio" value="put-only" checked={workload === 'put-only'} onChange={e => setWorkload(e.target.value)}
                className="focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300" disabled={isActive} />
              <span className="ml-2 text-sm text-gray-700">PUT-only (test max write bandwidth)</span>
            </label>
            <br />
            <label className="inline-flex items-center">
              <input type="radio" value="full-cycle" checked={workload === 'full-cycle'} onChange={e => setWorkload(e.target.value)}
                className="focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300" disabled={isActive} />
              <span className="ml-2 text-sm text-gray-700">Full Cycle (PUT + GET + REMOVE, simulate KV cache access)</span>
            </label>
          </div>
        </fieldset>

        {/* Parameters */}
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label htmlFor="objSize" className="block text-sm font-medium text-gray-700">Object Size (bytes)</label>
            <input type="number" id="objSize" value={objSize} onChange={e => setObjSize(parseInt(e.target.value) || 4096)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
              min={1} max={104857600} disabled={isActive} />
          </div>
          <div>
            <label htmlFor="duration" className="block text-sm font-medium text-gray-700">Duration (seconds)</label>
            <input type="number" id="duration" value={duration} onChange={e => setDuration(parseInt(e.target.value) || 60)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
              min={5} max={3600} disabled={isActive} />
          </div>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label htmlFor="protocol" className="block text-sm font-medium text-gray-700">Protocol</label>
            <select id="protocol" value={protocol} onChange={e => setProtocol(e.target.value)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
              disabled={isActive}>
              <option value="rdma">RDMA</option>
              <option value="tcp">TCP</option>
            </select>
          </div>
          <div>
            <label htmlFor="label" className="block text-sm font-medium text-gray-700">Test Label</label>
            <input type="text" id="label" value={label} onChange={e => setLabel(e.target.value)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
              placeholder={'benchmark-' + name + '-<timestamp>'} disabled={isActive} />
            <p className="mt-1 text-xs text-gray-500">Used as instance label in Prometheus metrics. Auto-generated if empty.</p>
          </div>
        </div>

        {/* Action buttons */}
        <div className="flex justify-end space-x-3">
          <a href={`/clusters/${namespace}/${name}`}
            className="bg-white py-2 px-4 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 hover:bg-gray-50">
            Cancel
          </a>
          {isActive ? (
            <button type="button" onClick={handleAbort}
              className="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-red-600 hover:bg-red-700">
              Abort
            </button>
          ) : (
            <button type="submit" disabled={submitting}
              className="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed">
              {submitting ? 'Starting...' : 'Start Benchmark'}
            </button>
          )}
        </div>
      </form>

      {/* Status banner */}
      {jobStatus !== 'idle' && (
        <div className={'mt-4 border px-4 py-3 rounded ' + (statusColor[jobStatus] || '')}>
          <div className="flex items-center">
            {jobStatus === 'Running' && (
              <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-blue-600" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
            )}
            <span className="font-medium">{statusLabel[jobStatus]}</span>
          </div>
        </div>
      )}

      {/* Results Summary (when completed) */}
      {result && (
        <div className="mt-4 bg-white shadow sm:rounded-lg">
          <div className="px-4 py-5 sm:px-6">
            <h3 className="text-lg leading-6 font-medium text-gray-900">Results</h3>
          </div>
          <div className="border-t border-gray-200 px-4 py-5 sm:p-0">
            <dl className="sm:divide-y sm:divide-gray-200">
              <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                <dt className="text-sm font-medium text-gray-500">Workload</dt>
                <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                  {result.workload === 'full-cycle' ? 'Full Cycle (PUT+GET+REMOVE)' : 'PUT-only'}
                </dd>
              </div>
              <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                <dt className="text-sm font-medium text-gray-500">Throughput</dt>
                <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2 font-semibold">{result.throughput_mbps} MB/s</dd>
              </div>
              <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                <dt className="text-sm font-medium text-gray-500">Operations</dt>
                <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{result.ops_per_sec} ops/s (total: {result.ops})</dd>
              </div>
              <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                <dt className="text-sm font-medium text-gray-500">Latency (avg)</dt>
                <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{result.avg_latency_ms} ms</dd>
              </div>
              <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                <dt className="text-sm font-medium text-gray-500">Errors</dt>
                <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                  <span className={result.errors > 0 ? 'text-red-600 font-semibold' : 'text-gray-900'}>
                    {result.errors}
                  </span>
                </dd>
              </div>
              <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                <dt className="text-sm font-medium text-gray-500">Duration</dt>
                <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{result.duration}s</dd>
              </div>
              <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                <dt className="text-sm font-medium text-gray-500">Object Size</dt>
                <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{result.obj_size} bytes</dd>
              </div>
              <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:px-6">
                <dt className="text-sm font-medium text-gray-500">Protocol</dt>
                <dd className="mt-1 text-sm text-gray-900 sm:mt-0 sm:col-span-2">{result.protocol.toUpperCase()}</dd>
              </div>
            </dl>
          </div>
        </div>
      )}

      {/* Logs */}
      {logs && (
        <div className="mt-4">
          <h3 className="text-sm font-medium text-gray-700 mb-1">Benchmark Logs</h3>
          <pre className="bg-gray-900 text-green-400 p-4 rounded text-xs font-mono overflow-auto max-h-96 whitespace-pre-wrap">
            {logs}
          </pre>
        </div>
      )}

      {/* Job info */}
      {jobName && (
        <div className="mt-4 text-xs text-gray-400">
          Job: {jobName} | Namespace: {namespace}
        </div>
      )}

      {/* Recent benchmarks list */}
      {activeBenchmarks.length > 0 && (
        <div className="mt-8 bg-white shadow sm:rounded-lg">
          <div className="px-4 py-5 sm:px-6">
            <h3 className="text-lg leading-6 font-medium text-gray-900">Recent Benchmarks</h3>
          </div>
          <div className="border-t border-gray-200">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Job</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Created</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Completed</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {activeBenchmarks.map(job => (
                  <tr key={job.name} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{job.name}</td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className={'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ' + (
                        job.status === 'Succeeded' ? 'bg-green-100 text-green-800' :
                        job.status === 'Failed' ? 'bg-red-100 text-red-800' :
                        job.status === 'Running' ? 'bg-blue-100 text-blue-800' :
                        'bg-yellow-100 text-yellow-800'
                      )}>
                        {job.status}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{new Date(job.created).toLocaleString()}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {job.completed ? new Date(job.completed).toLocaleString() : '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
