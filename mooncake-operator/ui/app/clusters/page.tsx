'use client'

import { useEffect, useState } from 'react'

interface Cluster {
  metadata: { name: string; namespace: string; creationTimestamp: string }
  spec: {
    image?: string
    master?: { replicas?: number }
    workers?: { replicas?: number; rdmaEnabled?: boolean }
    ha?: { type?: string }
  }
  status?: {
    phase?: string
    masterReady?: number
    workerReady?: number
  }
}

function PhaseBadge({ phase }: { phase?: string }) {
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

export default function ClustersPage() {
  const [clusters, setClusters] = useState<Cluster[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchClusters = () => {
    setLoading(true)
    setError(null)
    fetch('/api/clusters')
      .then(r => r.json())
      .then(d => {
        if (d.error) {
          setError(d.error)
          setClusters([])
        } else {
          setClusters(d.clusters || [])
        }
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => { fetchClusters() }, [])

  const handleDelete = async (namespace: string, name: string) => {
    if (!confirm(`Delete cluster "${name}" in namespace "${namespace}"?`)) return
    try {
      await fetch(`/api/clusters/${namespace}/${name}`, { method: 'DELETE' })
      fetchClusters()
    } catch (e: any) {
      alert('Failed to delete: ' + e.message)
    }
  }

  return (
    <div>
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold text-gray-900">Clusters</h1>
        <a
          href="/clusters/new"
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-indigo-600 hover:bg-indigo-700"
        >
          Create Cluster
        </a>
      </div>

      {error && (
        <div className="mt-4 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
          Error loading clusters: {error}
        </div>
      )}

      <div className="mt-6">
        <div className="bg-white shadow overflow-hidden sm:rounded-lg">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Namespace</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Phase</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Masters</th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Workers</th>
                <th scope="col" className="relative px-6 py-3">
                  <span className="sr-only">Actions</span>
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {loading ? (
                <tr><td colSpan={6} className="px-6 py-12 text-center text-sm text-gray-500">Loading...</td></tr>
              ) : clusters.length === 0 ? (
                <tr><td colSpan={6} className="px-6 py-12 text-center text-sm text-gray-500">No clusters found. Create your first MooncakeCluster.</td></tr>
              ) : (
                clusters.map(c => (
                  <tr key={`${c.metadata.namespace}/${c.metadata.name}`} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-indigo-600">
                      <a href={`/clusters/${c.metadata.namespace}/${c.metadata.name}`}>{c.metadata.name}</a>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{c.metadata.namespace}</td>
                    <td className="px-6 py-4 whitespace-nowrap"><PhaseBadge phase={c.status?.phase} /></td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {c.status?.masterReady ?? 0}/{c.spec?.master?.replicas ?? 1}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {c.status?.workerReady ?? 0}/{c.spec?.workers?.replicas ?? 0}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                      <button onClick={() => handleDelete(c.metadata.namespace, c.metadata.name)} className="text-red-600 hover:text-red-900">Delete</button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
