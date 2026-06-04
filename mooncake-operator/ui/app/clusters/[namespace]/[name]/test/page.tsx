'use client'

import { useEffect, useState } from 'react'

export default function TestClusterPage({
  params,
}: {
  params: { namespace: string; name: string }
}) {
  const { namespace, name } = params
  const [dataSize, setDataSize] = useState('4MB')
  const [replicaNum, setReplicaNum] = useState(3)
  const [repeatCount, setRepeatCount] = useState(16)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [jobName, setJobName] = useState<string | null>(null)
  const [jobStatus, setJobStatus] = useState<'idle' | 'Pending' | 'Running' | 'Succeeded' | 'Failed' | 'Expired'>('idle')
  const [logs, setLogs] = useState('')

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setSubmitting(true)
    setError(null)
    setJobStatus('Pending')
    setLogs('')
    setJobName(null)

    try {
      const res = await fetch(`/api/clusters/${namespace}/${name}/test`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dataSize, replicaNum, repeatCount }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.error || `Request failed: ${res.status}`)
      setJobName(data.jobName)
      setJobStatus('Pending')
    } catch (err: any) {
      setError(err.message)
      setJobStatus('idle')
    } finally {
      setSubmitting(false)
    }
  }

  // Poll job status
  useEffect(() => {
    if (!jobName) return
    if (jobStatus === 'Succeeded' || jobStatus === 'Failed' || jobStatus === 'Expired') return

    const interval = setInterval(async () => {
      try {
        const res = await fetch(`/api/clusters/${namespace}/${name}/test/${jobName}`)
        const data = await res.json()
        if (data.logs) setLogs(data.logs)
        if (data.status) setJobStatus(data.status)
      } catch (e) {
        // Ignore transient poll failures
      }
    }, 3000)

    return () => clearInterval(interval)
  }, [jobName, jobStatus, namespace, name])

  const statusColor = {
    idle: '',
    Pending: 'text-yellow-700 bg-yellow-50 border-yellow-200',
    Running: 'text-blue-700 bg-blue-50 border-blue-200',
    Succeeded: 'text-green-700 bg-green-50 border-green-200',
    Failed: 'text-red-700 bg-red-50 border-red-200',
    Expired: 'text-gray-700 bg-gray-50 border-gray-200',
  }

  const statusLabel = {
    idle: '',
    Pending: 'Job created, waiting for pod to start...',
    Running: 'Running...',
    Succeeded: 'Completed successfully!',
    Failed: 'Failed',
    Expired: 'Job has expired',
  }

  return (
    <div>
      <a href={`/clusters/${namespace}/${name}`} className="text-sm text-indigo-600 hover:text-indigo-800">
        &larr; Back to Cluster
      </a>
      <h1 className="text-2xl font-semibold text-gray-900 mt-2">Test Cluster {name}</h1>
      <p className="text-sm text-gray-500 mt-1">Namespace: {namespace}</p>

      {error && (
        <div className="mt-4 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">{error}</div>
      )}

      <form className="mt-6 space-y-6 bg-white shadow sm:rounded-lg p-6" onSubmit={handleSubmit}>
        <div>
          <label htmlFor="dataSize" className="block text-sm font-medium text-gray-700">Data Size</label>
          <input type="text" id="dataSize" value={dataSize} onChange={e => setDataSize(e.target.value)}
            className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
            placeholder="e.g. 1KB, 1MB, 512MB, 1GB" disabled={submitting || jobStatus === 'Running' || jobStatus === 'Pending'} />
          <p className="mt-1 text-xs text-gray-500">e.g. 1KB, 1MB, 512MB, 1GB. Cannot exceed cluster segment size.</p>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label htmlFor="replicaNum" className="block text-sm font-medium text-gray-700">Replica Count</label>
            <input type="number" id="replicaNum" value={replicaNum} onChange={e => setReplicaNum(parseInt(e.target.value) || 1)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
              min={1} disabled={submitting || jobStatus === 'Running' || jobStatus === 'Pending'} />
          </div>
          <div>
            <label htmlFor="repeatCount" className="block text-sm font-medium text-gray-700">Repeat Count</label>
            <input type="number" id="repeatCount" value={repeatCount} onChange={e => setRepeatCount(parseInt(e.target.value) || 1)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
              min={1} disabled={submitting || jobStatus === 'Running' || jobStatus === 'Pending'} />
          </div>
        </div>

        <div className="flex justify-end space-x-3">
          <a href={`/clusters/${namespace}/${name}`}
            className="bg-white py-2 px-4 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 hover:bg-gray-50">
            Cancel
          </a>
          <button type="submit" disabled={submitting || jobStatus === 'Running' || jobStatus === 'Pending'}
            className="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed">
            {submitting ? 'Submitting...' : 'Start Test'}
          </button>
        </div>
      </form>

      {/* Status banner */}
      {jobStatus !== 'idle' && (
        <div className={`mt-4 border px-4 py-3 rounded ${statusColor[jobStatus]}`}>
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

      {/* Logs */}
      {logs && (
        <div className="mt-4">
          <pre className="bg-gray-900 text-green-400 p-4 rounded text-xs font-mono overflow-auto max-h-96 whitespace-pre-wrap">
            {logs}
          </pre>
        </div>
      )}

      {/* Job info */}
      {jobName && (
        <div className="mt-4 text-xs text-gray-400">
          Job: {jobName}
        </div>
      )}
    </div>
  )
}
