'use client'

import { useEffect, useState } from 'react'

interface ClusterSpec {
  image?: string
  master?: { replicas?: number; configOverrides?: Record<string, string> }
  workers?: {
    replicas?: number; rdmaEnabled?: boolean; gpuEnabled?: boolean
    segmentSize?: string; rdmaPortRange?: string; configOverrides?: Record<string, string>
  }
  ha?: { type?: string; connectionString?: string; etcdEndpoints?: string }
}

export default function EditClusterPage({
  params,
}: {
  params: { namespace: string; name: string }
}) {
  const { namespace, name } = params
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState(false)

  const [image, setImage] = useState('mooncake-store:latest')
  const [masterReplicas, setMasterReplicas] = useState(1)
  const [workerReplicas, setWorkerReplicas] = useState(2)
  const [segmentSize, setSegmentSize] = useState('4Gi')
  const [haType, setHaType] = useState('')
  const [rdmaEnabled, setRdmaEnabled] = useState(true)
  const [gpuEnabled, setGpuEnabled] = useState(false)
  const [configOverrides, setConfigOverrides] = useState('')

  useEffect(() => {
    fetch(`/api/clusters/${namespace}/${name}`)
      .then(r => r.json())
      .then(data => {
        const spec: ClusterSpec = data.cluster?.spec || {}
        setImage(spec.image || 'mooncake-store:latest')
        setMasterReplicas(spec.master?.replicas ?? 1)
        setWorkerReplicas(spec.workers?.replicas ?? 0)
        setSegmentSize(spec.workers?.segmentSize || '4Gi')
        setHaType(spec.ha?.type || '')
        setRdmaEnabled(spec.workers?.rdmaEnabled ?? true)
        setGpuEnabled(spec.workers?.gpuEnabled ?? false)
        const ov = spec.master?.configOverrides
        setConfigOverrides(ov && Object.keys(ov).length > 0 ? JSON.stringify(ov, null, 2) : '')
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [namespace, name])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setSaving(true)
    setError(null)
    setSuccess(false)

    let parsedOverrides: Record<string, string> | undefined
    if (configOverrides.trim()) {
      try {
        parsedOverrides = JSON.parse(configOverrides)
      } catch {
        setError('configOverrides 不是合法的 JSON')
        setSaving(false)
        return
      }
    }

    const spec: any = {
      image: image.trim() || 'mooncake-store:latest',
      master: {
        replicas: masterReplicas,
      },
      workers: {
        replicas: workerReplicas,
        rdmaEnabled,
        gpuEnabled,
        segmentSize: segmentSize || '4Gi',
      },
    }
    if (haType) spec.ha = { type: haType }
    if (parsedOverrides) spec.master.configOverrides = parsedOverrides

    try {
      const res = await fetch(`/api/clusters/${namespace}/${name}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ spec }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.error || `Request failed: ${res.status}`)
      setSuccess(true)
    } catch (err: any) {
      setError(err.message || '保存失败')
    } finally {
      setSaving(false)
    }
  }

  if (loading) {
    return <div className="text-gray-500 py-12 text-center">加载中...</div>
  }

  return (
    <div>
      <h1 className="text-2xl font-semibold text-gray-900">编辑集群 {name}</h1>
      <p className="text-sm text-gray-500 mt-1">命名空间: {namespace}</p>

      {error && (
        <div className="mt-4 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">{error}</div>
      )}
      {success && (
        <div className="mt-4 bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded">
          保存成功！
          <a href={`/clusters/${namespace}/${name}`} className="ml-3 underline text-green-600 hover:text-green-800">返回详情</a>
        </div>
      )}

      <form className="mt-6 space-y-6 bg-white shadow sm:rounded-lg p-6" onSubmit={handleSubmit}>
        <div>
          <label htmlFor="image" className="block text-sm font-medium text-gray-700">镜像</label>
          <input type="text" id="image" value={image} onChange={e => setImage(e.target.value)}
            className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md" />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label htmlFor="masterReplicas" className="block text-sm font-medium text-gray-700">Master 副本数</label>
            <input type="number" id="masterReplicas" value={masterReplicas} onChange={e => setMasterReplicas(parseInt(e.target.value) || 1)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md" min={1} />
          </div>
          <div>
            <label htmlFor="workerReplicas" className="block text-sm font-medium text-gray-700">Worker 副本数</label>
            <input type="number" id="workerReplicas" value={workerReplicas} onChange={e => setWorkerReplicas(parseInt(e.target.value) || 0)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md" min={0} />
          </div>
        </div>

        <div>
          <label htmlFor="segmentSize" className="block text-sm font-medium text-gray-700">Segment 大小</label>
          <input type="text" id="segmentSize" value={segmentSize} onChange={e => setSegmentSize(e.target.value)}
            className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md" />
        </div>

        <div>
          <label htmlFor="haType" className="block text-sm font-medium text-gray-700">HA 类型</label>
          <select id="haType" value={haType} onChange={e => setHaType(e.target.value)}
            className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md">
            <option value="">无</option>
            <option value="k8s">Kubernetes</option>
            <option value="etcd">etcd</option>
            <option value="redis">Redis</option>
          </select>
        </div>

        <div className="flex items-center space-x-4">
          <div className="flex items-center">
            <input type="checkbox" id="rdmaEnabled" checked={rdmaEnabled} onChange={e => setRdmaEnabled(e.target.checked)}
              className="focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300 rounded" />
            <label htmlFor="rdmaEnabled" className="ml-2 block text-sm text-gray-900">启用 RDMA</label>
          </div>
          <div className="flex items-center">
            <input type="checkbox" id="gpuEnabled" checked={gpuEnabled} onChange={e => setGpuEnabled(e.target.checked)}
              className="focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300 rounded" />
            <label htmlFor="gpuEnabled" className="ml-2 block text-sm text-gray-900">启用 GPU</label>
          </div>
        </div>

        <div>
          <label htmlFor="configOverrides" className="block text-sm font-medium text-gray-700">Config Overrides (JSON)</label>
          <textarea id="configOverrides" rows={4} value={configOverrides} onChange={e => setConfigOverrides(e.target.value)}
            className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md font-mono" />
        </div>

        <div className="flex justify-end space-x-3">
          <a href={`/clusters/${namespace}/${name}`}
            className="bg-white py-2 px-4 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 hover:bg-gray-50">
            取消
          </a>
          <button type="submit" disabled={saving}
            className="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed">
            {saving ? '保存中...' : '保存'}
          </button>
        </div>
      </form>
    </div>
  )
}
