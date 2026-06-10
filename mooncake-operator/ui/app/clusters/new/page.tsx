'use client'

import { useEffect, useState } from 'react'

const ADJECTIVES = [
  'bold', 'calm', 'swift', 'keen', 'neat', 'pure', 'rare', 'safe',
  'cool', 'fast', 'glad', 'kind', 'lucky', 'proud', 'bright', 'quiet',
  'sharp', 'sunny', 'warm', 'wise', 'eager', 'gentle', 'happy', 'jolly',
  'lively', 'noble', 'polite', 'sweet', 'zesty', 'brisk',
]

const NOUNS = [
  'panda', 'tiger', 'eagle', 'lion', 'wolf', 'bear', 'hawk', 'deer',
  'owl', 'fox', 'kite', 'orca', 'koala', 'otter', 'robin', 'swan',
  'heron', 'elm', 'oak', 'pine', 'maple', 'cedar', 'willow', 'birch',
]

function randomName(): string {
  const adj = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)]
  const noun = NOUNS[Math.floor(Math.random() * NOUNS.length)]
  const num = Math.floor(Math.random() * 100)
  return `${adj}-${noun}-${num}`
}

export default function NewClusterPage() {
  const [name, setName] = useState('')
  const [namespace, setNamespace] = useState('default')
  const [images, setImages] = useState<string[]>([])
  const [image, setImage] = useState('')
  const [imagesLoading, setImagesLoading] = useState(true)
  const [masterReplicas, setMasterReplicas] = useState(1)
  const [workerReplicas, setWorkerReplicas] = useState(2)
  const [segmentSize, setSegmentSize] = useState('2Gi')
  const [haType, setHaType] = useState('k8s')
  const [rdmaEnabled, setRdmaEnabled] = useState(false)
  const [gpuEnabled, setGpuEnabled] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setName(randomName())
    fetch('/api/images')
      .then(r => r.json())
      .then(data => {
        const imgs = data.images || []
        setImages(imgs)
        if (imgs.length > 0) setImage(imgs[0])
        setImagesLoading(false)
      })
      .catch(() => setImagesLoading(false))
  }, [])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!name.trim()) {
      setError('Cluster name is required')
      return
    }
    setSubmitting(true)
    setError(null)

    const cr = {
      apiVersion: 'mooncake.io/v1alpha1',
      kind: 'MooncakeCluster',
      metadata: {
        name: name.trim(),
        namespace: namespace.trim() || 'default',
      },
      spec: {
        image: image.trim() || 'mooncake-store:latest',
        master: {
          replicas: masterReplicas,
        },
        workers: {
          replicas: workerReplicas,
          rdmaEnabled,
          gpuEnabled,
          segmentSize,
        },
        ha: {
          type: haType,
        },
      },
    }

    try {
      const res = await fetch('/api/clusters', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cr),
      })
      const data = await res.json()
      if (!res.ok) {
        throw new Error(data.error || `Request failed: ${res.status}`)
      }
      window.location.href = '/clusters'
    } catch (err: any) {
      setError(err.message || 'Failed to create cluster')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div>
      <h1 className="text-2xl font-semibold text-gray-900">Create MooncakeCluster</h1>
      {error && (
        <div className="mt-4 bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
          {error}
        </div>
      )}
      <form className="mt-6 space-y-6 bg-white shadow sm:rounded-lg p-6" onSubmit={handleSubmit}>
        <div>
          <label htmlFor="name" className="block text-sm font-medium text-gray-700">
            Cluster Name <span className="text-red-500">*</span>
          </label>
          <input
            type="text"
            name="name"
            id="name"
            value={name}
            onChange={e => setName(e.target.value)}
            className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
            placeholder="my-cluster"
            required
          />
        </div>

        <div>
          <label htmlFor="namespace" className="block text-sm font-medium text-gray-700">
            Namespace
          </label>
          <input
            type="text"
            name="namespace"
            id="namespace"
            value={namespace}
            onChange={e => setNamespace(e.target.value)}
            className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
            placeholder="default"
          />
        </div>

        <div>
          <label htmlFor="image" className="block text-sm font-medium text-gray-700">
            Image <span className="text-red-500">*</span>
          </label>
          <select
            name="image"
            id="image"
            value={image}
            onChange={e => setImage(e.target.value)}
            className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
            disabled={imagesLoading}
          >
            {imagesLoading ? (
              <option>Loading images...</option>
            ) : images.length === 0 ? (
              <option value="">No mooncake-store images found on nodes</option>
            ) : (
              images.map(img => (
                <option key={img} value={img}>{img}</option>
              ))
            )}
          </select>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label htmlFor="masterReplicas" className="block text-sm font-medium text-gray-700">
              Master Replicas
            </label>
            <input
              type="number"
              name="masterReplicas"
              id="masterReplicas"
              value={masterReplicas}
              onChange={e => setMasterReplicas(parseInt(e.target.value) || 1)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
              min={1}
            />
          </div>

          <div>
            <label htmlFor="workerReplicas" className="block text-sm font-medium text-gray-700">
              Worker Replicas
            </label>
            <input
              type="number"
              name="workerReplicas"
              id="workerReplicas"
              value={workerReplicas}
              onChange={e => setWorkerReplicas(parseInt(e.target.value) || 1)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
              min={1}
            />
          </div>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label htmlFor="segmentSize" className="block text-sm font-medium text-gray-700">
              Segment Size
            </label>
            <input
              type="text"
              name="segmentSize"
              id="segmentSize"
              value={segmentSize}
              onChange={e => setSegmentSize(e.target.value)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
            />
          </div>

          <div>
            <label htmlFor="haType" className="block text-sm font-medium text-gray-700">
              HA Backend
            </label>
            <select
              name="haType"
              id="haType"
              value={haType}
              onChange={e => setHaType(e.target.value)}
              className="mt-1 focus:ring-indigo-500 focus:border-indigo-500 block w-full shadow-sm sm:text-sm border-gray-300 rounded-md"
            >
              <option value="k8s">Kubernetes</option>
              <option value="etcd">etcd</option>
              <option value="redis">Redis</option>
            </select>
          </div>
        </div>

        <div className="flex items-center space-x-4">
          <div className="flex items-center">
            <input
              type="checkbox"
              name="rdmaEnabled"
              id="rdmaEnabled"
              checked={rdmaEnabled}
              onChange={e => setRdmaEnabled(e.target.checked)}
              className="focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300 rounded"
            />
            <label htmlFor="rdmaEnabled" className="ml-2 block text-sm text-gray-900">
              Enable RDMA
            </label>
          </div>

          <div className="flex items-center">
            <input
              type="checkbox"
              name="gpuEnabled"
              id="gpuEnabled"
              checked={gpuEnabled}
              onChange={e => setGpuEnabled(e.target.checked)}
              className="focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300 rounded"
            />
            <label htmlFor="gpuEnabled" className="ml-2 block text-sm text-gray-900">
              Enable GPU
            </label>
          </div>
        </div>

        <div className="flex justify-end space-x-3">
          <a
            href="/clusters"
            className="bg-white py-2 px-4 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 hover:bg-gray-50"
          >
            Cancel
          </a>
          <button
            type="submit"
            disabled={submitting}
            className="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {submitting ? 'Creating...' : 'Create'}
          </button>
        </div>
      </form>
    </div>
  )
}
