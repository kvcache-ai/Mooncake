'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'

interface KeyEntry {
  key: string
  size: number
  transport_endpoint: string
}

interface WorkerKeysResponse {
  segment: string
  keys: KeyEntry[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  truncated: boolean
  scannedCount: number
  totalKeyCount: number
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  return `${(bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0)} ${units[i]}`
}

function formatKey(key: string, maxLen: number = 64): string {
  if (key.length <= maxLen) return key
  return key.substring(0, maxLen - 3) + '...'
}

function Spinner() {
  return (
    <div className="flex justify-center py-12">
      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-600"></div>
    </div>
  )
}

export default function WorkerDetailPage({
  params,
  searchParams,
}: {
  params: { namespace: string; name: string; podName: string }
  searchParams: { [key: string]: string | string[] | undefined }
}) {
  const { namespace, name, podName } = params
  const router = useRouter()

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<WorkerKeysResponse | null>(null)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(50)
  const [toastMessage, setToastMessage] = useState<string | null>(null)
  const [clearing, setClearing] = useState(false)

  // Resolve page/pageSize from URL search params on mount
  useEffect(() => {
    const sp = searchParams as Record<string, string> | undefined
    if (sp?.page) {
      const p = parseInt(sp.page)
      if (p >= 1) setPage(p)
    }
    if (sp?.pageSize) {
      const ps = parseInt(sp.pageSize)
      if (ps >= 1 && ps <= 200) setPageSize(ps)
    }
  }, [])

  // Fetch pod info to get the segment name
  const [podIP, setPodIP] = useState<string>('')
  const [segmentName, setSegmentName] = useState<string>('')
  const [nodeName, setNodeName] = useState<string>('')
  const [phase, setPhase] = useState<string>('')
  const [deletionTimestamp, setDeletionTimestamp] = useState<string>('')

  useEffect(() => {
    // Fetch pod info first to resolve the segment
    fetch(`/api/clusters/${namespace}/${name}/pods`)
      .then(r => r.json())
      .then(podData => {
        const pods = podData.pods || []
        const pod = pods.find((p: any) => p.metadata?.name === podName)
        if (pod) {
          const ip = pod.status?.podIP || ''
          const node = pod.spec?.nodeName || ''
          const ph = pod.status?.phase || ''
          const delTs = pod.metadata?.deletionTimestamp || ''
          setPodIP(ip)
          setNodeName(node)
          setPhase(ph)
          setDeletionTimestamp(delTs)

          // Derive segment name from the worker config: IP + client min port
          // Default min port is 13006 (MC_STORE_CLIENT_MIN_PORT)
          const port = '13006'
          const segName = ip ? `${ip}:${port}` : ''
          setSegmentName(segName)
        }
      })
      .catch(e => setError(e.message))
  }, [namespace, name, podName])

  // Fetch keys when segment is resolved
  const fetchKeys = (p: number, ps: number) => {
    if (!segmentName) return
    setLoading(true)
    setError(null)

    fetch(`/api/clusters/${namespace}/${name}/worker-keys?segment=${encodeURIComponent(segmentName)}&page=${p}&pageSize=${ps}`)
      .then(r => r.json())
      .then(d => {
        if (d.error) {
          setError(d.error)
        } else {
          setData(d)
        }
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    if (segmentName) {
      fetchKeys(page, pageSize)
    }
  }, [segmentName, page, pageSize])

  const handlePageChange = (newPage: number) => {
    setPage(newPage)
  }

  const handlePageSizeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const newSize = parseInt(e.target.value)
    setPageSize(newSize)
    setPage(1)
  }

  const totalKeys = data?.total ?? 0
  const startIndex = totalKeys > 0 ? (page - 1) * pageSize + 1 : 0
  const endIndex = Math.min(page * pageSize, totalKeys)

  // Build page number list
  const totalPages = data?.totalPages ?? 1
  const pageNumbers: number[] = []
  const maxVisible = 7
  if (totalPages <= maxVisible) {
    for (let i = 1; i <= totalPages; i++) pageNumbers.push(i)
  } else {
    pageNumbers.push(1)
    let left = Math.max(2, page - 1)
    let right = Math.min(totalPages - 1, page + 1)
    // Expand range to fill maxVisible
    while (right - left + 1 < maxVisible - 2) {
      if (left > 2) left--
      if (right < totalPages - 1) right++
    }
    if (left > 2) pageNumbers.push(-1) // ellipsis
    for (let i = left; i <= right; i++) pageNumbers.push(i)
    if (right < totalPages - 1) pageNumbers.push(-2) // ellipsis
    pageNumbers.push(totalPages)
  }

  return (
    <div>
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <a
            href={`/clusters/${namespace}/${name}`}
            className="text-sm text-indigo-600 hover:text-indigo-800"
          >
            &larr; Back to cluster {name}
          </a>
          <h1 className="text-2xl font-semibold text-gray-900 mt-1">
            Worker: {podName}
          </h1>
          <p className="text-sm text-gray-500">
            Namespace: {namespace} &middot; Cluster: {name}
          </p>
        </div>
        <div>
          {data && (
            <button
              onClick={async () => {
                const keyCount = data?.total || 0
                if (!confirm(keyCount > 0
                  ? `Are you sure you want to clear ALL ${keyCount.toLocaleString()} KV cache keys from worker "${podName}"?\n\nThis action cannot be undone.`
                  : `No keys found for worker "${podName}". Clear anyway?`)) return
                setError(null)
                setClearing(true)
                setToastMessage('Clearing data...')
                try {
                  const res = await fetch(`/api/clusters/${namespace}/${name}/clear-worker-data`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ segment: segmentName }),
                  })
                  const result = await res.json()
                  if (!res.ok || result.error) throw new Error(result.error || 'Failed to clear data')
                  setToastMessage(`Cleared ${result.removed || 0} keys${result.failed ? ` (${result.failed} failed)` : ''}`)
                  setTimeout(() => setToastMessage(null), 5000)
                  // Refresh keys
                  fetchKeys(page, pageSize)
                } catch (e: any) {
                  setError(e.message)
                  setToastMessage(null)
                } finally {
                  setClearing(false)
                }
              }}
              disabled={clearing}
              className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white ${clearing ? 'bg-red-400 cursor-not-allowed' : 'bg-red-600 hover:bg-red-700'}`}
            >
              Clear Data
            </button>
          )}
        </div>
      </div>

      {/* Toast */}
      {toastMessage && (
        <div className="mt-4 bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded text-sm">
          {toastMessage}
        </div>
      )}

      {/* Worker info card */}
      <div className="mt-6 bg-white overflow-hidden shadow rounded-lg">
        <div className="px-4 py-5 sm:px-6">
          <h3 className="text-lg leading-6 font-medium text-gray-900">Worker Info</h3>
        </div>
        <div className="border-t border-gray-200 px-4 py-5 sm:px-6">
          <dl className="grid grid-cols-1 gap-x-4 gap-y-4 sm:grid-cols-3">
            <div>
              <dt className="text-sm font-medium text-gray-500">Node</dt>
              <dd className="mt-1 text-sm text-gray-900">{nodeName || '-'}</dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500">Pod IP</dt>
              <dd className="mt-1 text-sm text-gray-900 font-mono">{podIP || '-'}</dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500">Status</dt>
              <dd className="mt-1 text-sm text-gray-900">
                {deletionTimestamp ? (
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-orange-100 text-orange-800">
                    Terminating
                  </span>
                ) : (
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                    phase === 'Running' ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-800'
                  }`}>
                    {phase || 'Unknown'}
                  </span>
                )}
              </dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500">Segment</dt>
              <dd className="mt-1 text-sm text-gray-900 font-mono">{segmentName || '...'}</dd>
            </div>
            {data && (
              <div>
                <dt className="text-sm font-medium text-gray-500">Total Keys</dt>
                <dd className="mt-1 text-sm text-gray-900 font-semibold">{data.total.toLocaleString()}</dd>
              </div>
            )}
          </dl>
        </div>
      </div>

      {/* Keys table */}
      <div className="mt-6 bg-white shadow sm:rounded-lg">
        <div className="px-4 py-5 sm:px-6 flex justify-between items-center flex-wrap gap-2">
          <div>
            <h3 className="text-lg leading-6 font-medium text-gray-900">KV Cache Keys</h3>
            {data && (
              <p className="mt-1 text-sm text-gray-500">
                Showing {startIndex}–{endIndex} of {totalKeys.toLocaleString()} keys
                {data.truncated && (
                  <span className="ml-2 text-amber-600 font-medium" title={`Scanned first ${data.scannedCount} of ${data.totalKeyCount} total keys. For large clusters, consider using a master-side segment-key filter in the future.`}>
                    (scanned {data.scannedCount.toLocaleString()} / {data.totalKeyCount.toLocaleString()})
                  </span>
                )}
              </p>
            )}
          </div>
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-500">Page size:</label>
            <select
              value={pageSize}
              onChange={handlePageSizeChange}
              className="border border-gray-300 rounded px-2 py-1 text-sm"
            >
              <option value={20}>20</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
              <option value={200}>200</option>
            </select>
          </div>
        </div>

        <div className="border-t border-gray-200 overflow-x-auto">
          {loading ? (
            <Spinner />
          ) : error ? (
            <div className="px-6 py-12 text-center">
              <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded inline-block">
                Error: {error}
              </div>
            </div>
          ) : !data || data.keys.length === 0 ? (
            <div className="px-6 py-12 text-center text-sm text-gray-500">
              No KV cache keys found for this worker.
            </div>
          ) : (
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider w-12">#</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Key</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider w-32">Size</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {data.keys.map((entry, idx) => (
                  <tr key={entry.key} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-400">
                      {startIndex + idx}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-900 font-mono break-all">
                      <span title={entry.key}>{formatKey(entry.key, 80)}</span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {formatBytes(entry.size)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Pagination */}
        {data && data.totalPages > 1 && (
          <div className="px-4 py-3 flex items-center justify-between border-t border-gray-200 sm:px-6">
            <div className="flex-1 flex justify-between sm:hidden">
              <button
                onClick={() => handlePageChange(page - 1)}
                disabled={page <= 1}
                className="relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Previous
              </button>
              <button
                onClick={() => handlePageChange(page + 1)}
                disabled={page >= totalPages}
                className="ml-3 relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Next
              </button>
            </div>
            <div className="hidden sm:flex-1 sm:flex sm:items-center sm:justify-between">
              <div>
                <p className="text-sm text-gray-700">
                  Page <span className="font-medium">{page}</span> of{' '}
                  <span className="font-medium">{totalPages}</span>
                </p>
              </div>
              <div>
                <nav className="relative z-0 inline-flex rounded-md shadow-sm -space-x-px" aria-label="Pagination">
                  <button
                    onClick={() => handlePageChange(page - 1)}
                    disabled={page <= 1}
                    className="relative inline-flex items-center px-2 py-2 rounded-l-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <svg className="h-5 w-5" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M12.707 5.293a1 1 0 010 1.414L9.414 10l3.293 3.293a1 1 0 01-1.414 1.414l-4-4a1 1 0 010-1.414l4-4a1 1 0 011.414 0z" clipRule="evenodd" />
                    </svg>
                  </button>
                  {pageNumbers.map((pn, i) => {
                    if (pn < 0) {
                      return (
                        <span
                          key={`ellipsis-${i}`}
                          className="relative inline-flex items-center px-4 py-2 border border-gray-300 bg-white text-sm font-medium text-gray-500"
                        >
                          ...
                        </span>
                      )
                    }
                    const isActive = pn === page
                    return (
                      <button
                        key={pn}
                        onClick={() => handlePageChange(pn)}
                        className={`relative inline-flex items-center px-4 py-2 border text-sm font-medium ${
                          isActive
                            ? 'z-10 bg-indigo-50 border-indigo-500 text-indigo-600'
                            : 'bg-white border-gray-300 text-gray-500 hover:bg-gray-50'
                        }`}
                      >
                        {pn}
                      </button>
                    )
                  })}
                  <button
                    onClick={() => handlePageChange(page + 1)}
                    disabled={page >= totalPages}
                    className="relative inline-flex items-center px-2 py-2 rounded-r-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <svg className="h-5 w-5" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z" clipRule="evenodd" />
                    </svg>
                  </button>
                </nav>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
