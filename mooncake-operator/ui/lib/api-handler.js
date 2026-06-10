const https = require('https')
const http = require('http')
const fs = require('fs')
const path = require('path')
const os = require('os')

let k8sCaData = ''
let k8sServer = ''
let k8sClientCert = ''   // PEM cert from kubeconfig
let k8sClientKey = ''    // PEM key from kubeconfig
let k8sToken = ''         // static token from kubeconfig (when not using SA token file)

// Try in-cluster SA credentials first, then fall back to kubeconfig
try {
  const caPath = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
  if (fs.existsSync(caPath)) {
    const caCert = fs.readFileSync(caPath)
    k8sCaData = caCert.toString('base64')
    const host = process.env.KUBERNETES_SERVICE_HOST || 'kubernetes.default.svc'
    const port = process.env.KUBERNETES_SERVICE_PORT || '443'
    k8sServer = `https://${host}:${port}`
    console.log('[api-handler] Loaded in-cluster K8s credentials')
  }
} catch (e) {
  console.warn('[api-handler] Failed to load in-cluster credentials:', e.message)
}

// Kubeconfig fallback
if (!k8sServer) {
  try {
    const yaml = require('js-yaml')
    const kubeconfigPath = process.env.KUBECONFIG || path.join(os.homedir(), '.kube', 'config')
    if (fs.existsSync(kubeconfigPath)) {
      const kc = yaml.load(fs.readFileSync(kubeconfigPath, 'utf8'))
      const currentCtx = kc['current-context']
      const ctx = (kc.contexts || []).find(c => c.name === currentCtx)
      if (ctx) {
        const cluster = (kc.clusters || []).find(c => c.name === ctx.context.cluster)
        const user = (kc.users || []).find(u => u.name === ctx.context.user)
        if (cluster && user) {
          k8sServer = cluster.cluster.server
          if (cluster.cluster['certificate-authority-data']) {
            k8sCaData = cluster.cluster['certificate-authority-data']
          } else if (cluster.cluster['certificate-authority']) {
            k8sCaData = fs.readFileSync(
              cluster.cluster['certificate-authority'].replace(/^~/, os.homedir()),
              'base64'
            )
          }
          if (user.user['client-certificate-data']) {
            k8sClientCert = Buffer.from(user.user['client-certificate-data'], 'base64').toString('utf8')
          } else if (user.user['client-certificate']) {
            k8sClientCert = fs.readFileSync(
              user.user['client-certificate'].replace(/^~/, os.homedir()),
              'utf8'
            )
          }
          if (user.user['client-key-data']) {
            k8sClientKey = Buffer.from(user.user['client-key-data'], 'base64').toString('utf8')
          } else if (user.user['client-key']) {
            k8sClientKey = fs.readFileSync(
              user.user['client-key'].replace(/^~/, os.homedir()),
              'utf8'
            )
          }
          if (user.user.token) {
            k8sToken = user.user.token
          }
          console.log('[api-handler] Loaded K8s credentials from kubeconfig:', kubeconfigPath, '->', k8sServer)
        }
      }
    }
  } catch (e) {
    console.warn('[api-handler] Failed to load kubeconfig:', e.message)
  }
}

function getK8sToken() {
  if (k8sToken) return k8sToken
  try {
    const tokenPath = '/var/run/secrets/kubernetes.io/serviceaccount/token'
    return fs.readFileSync(tokenPath, 'utf8').trim()
  } catch (e) {
    return ''
  }
}

function k8sRequest(apiPath, method = 'GET', body) {
  return new Promise((resolve, reject) => {
    if (!k8sServer) {
      reject(new Error('K8s API server not available — not running in-cluster and no kubeconfig found'))
      return
    }
    const url = new URL(apiPath, k8sServer)
    const ca = k8sCaData ? Buffer.from(k8sCaData, 'base64') : undefined
    const headers = {
      'Accept': 'application/json',
    }
    // Use client cert auth when available; otherwise fall back to Bearer token
    if (k8sClientCert && k8sClientKey) {
      // client cert auth — don't add Authorization header
    } else {
      headers['Authorization'] = `Bearer ${getK8sToken()}`
    }
    if (method === 'POST' || method === 'PUT') headers['Content-Type'] = 'application/json'
    if (method === 'PATCH') headers['Content-Type'] = 'application/json-patch+json'
    const options = {
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname + url.search,
      method,
      headers,
      ca,
      rejectUnauthorized: !!ca,
      cert: k8sClientCert || undefined,
      key: k8sClientKey || undefined,
    }
    const req = https.request(options, (res) => {
      let data = ''
      res.on('data', (chunk) => data += chunk)
      res.on('end', () => {
        try {
          const body = JSON.parse(data)
          if (res.statusCode < 200 || res.statusCode >= 300) {
            const msg = body?.message || body?.error || `K8s API returned ${res.statusCode}`
            reject(new Error(msg))
          } else {
            resolve(body)
          }
        } catch (e) {
          if (e instanceof SyntaxError) {
            if (res.statusCode < 200 || res.statusCode >= 300) {
              reject(new Error(`K8s API returned ${res.statusCode}: ${data}`))
            } else {
              resolve(data)
            }
          } else {
            reject(e)
          }
        }
      })
    })
    req.on('error', reject)
    if (body) req.write(typeof body === 'string' ? body : JSON.stringify(body))
    req.end()
  })
}

function jsonReply(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' })
  res.end(JSON.stringify(data))
}

// Recursive deep merge: merges src into target for plain objects, replaces for scalars/arrays
function deepMerge(target, src) {
  for (const key of Object.keys(src)) {
    if (src[key] && typeof src[key] === 'object' && !Array.isArray(src[key])) {
      if (!target[key] || typeof target[key] !== 'object') target[key] = {}
      deepMerge(target[key], src[key])
    } else {
      target[key] = src[key]
    }
  }
}

// In-memory drain job tracking (module-level, persists across requests)
// key: `${namespace}/${name}/${podIP}` -> { jobId, lastStatus, migratedBytes, succeededUnits, failedUnits, createdAt }
const drainJobs = new Map()
const drainStatusEnum = ['CREATED', 'PLANNING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED']

// Helper: find the leader master pod by querying each master's /role endpoint via K8s API proxy
async function findLeaderPod(namespace, name) {
  try {
    const podResult = await k8sRequest(
      `/api/v1/namespaces/${namespace}/pods?labelSelector=app=mooncake-master,cluster=${name}`
    )
    const pods = podResult?.items || []
    const containerPort = 9003 // metrics port, used for admin HTTP API

    for (const pod of pods) {
      const podName = pod.metadata.name
      try {
        const proxyPrefix = `/api/v1/namespaces/${namespace}/pods/${podName}:${containerPort}/proxy`
        const roleText = await k8sRequest(`${proxyPrefix}/role`)
        const role = typeof roleText === 'string' ? roleText.trim() : ''
        if (role === 'leader') {
          console.log(`[findLeaderPod] Leader found: ${podName}`)
          return podName
        }
      } catch (e) {
        console.warn(`[findLeaderPod] Failed to query role for ${podName}: ${e.message}`)
      }
    }
  } catch (e) {
    console.warn(`[findLeaderPod] Failed to list master pods: ${e.message}`)
  }
  // Fallback: master-0 is typically the default leader
  console.log(`[findLeaderPod] No leader detected via role query, using ${name}-master-0`)
  return name + '-master-0'
}

async function handleApiRequest(req, res) {
  try {
    const url = new URL(req.url, 'http://localhost')

  // GET /api/operator - get operator HA status (pods + leader)
  if (url.pathname === '/api/operator' && req.method === 'GET') {
    try {
      const namespace = 'mooncake-operator-system'

      // Get operator pods
      const podResult = await k8sRequest(
        `/api/v1/namespaces/${namespace}/pods?labelSelector=control-plane%3Dmooncake-operator`
      )
      const pods = (podResult?.items || []).map(pod => {
        const readyCondition = (pod.status?.conditions || []).find(c => c.type === 'Ready')
        return {
          name: pod.metadata?.name || '',
          phase: pod.status?.phase || 'Unknown',
          node: pod.spec?.nodeName || '',
          ready: readyCondition?.status === 'True',
        }
      })

      // Get leader lease
      let leader = null
      try {
        const leaseResult = await k8sRequest(
          `/apis/coordination.k8s.io/v1/namespaces/${namespace}/leases/mooncake-operator-leader`
        )
        if (leaseResult?.spec) {
          leader = {
            holder: leaseResult.spec.holderIdentity || '',
            leaseDuration: leaseResult.spec.leaseDurationSeconds || 0,
            acquireTime: leaseResult.spec.acquireTime || '',
            renewTime: leaseResult.spec.renewTime || '',
            leaderTransitions: leaseResult.spec.leaderTransitions || 0,
          }
        }
      } catch (e) {
        // Lease may not exist yet
        console.warn('[api-handler] Failed to get operator lease:', e.message)
      }

      return jsonReply(res, 200, {
        pods,
        readyCount: pods.filter(p => p.ready).length,
        totalCount: pods.length,
        leader,
      })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // PUT /api/operator/scale - scale the operator deployment replicas
  if (url.pathname === '/api/operator/scale' && req.method === 'PUT') {
    let body = ''
    for await (const chunk of req) body += chunk
    try {
      const { replicas } = JSON.parse(body)
      if (typeof replicas !== 'number' || replicas < 1 || !Number.isInteger(replicas)) {
        return jsonReply(res, 400, { error: 'replicas must be an integer >= 1' })
      }
      const namespace = 'mooncake-operator-system'
      const depName = 'mooncake-operator-controller-manager'

      // Patch the deployment's replicas
      const patch = [{ op: 'replace', path: '/spec/replicas', value: replicas }]
      const result = await k8sRequest(
        `/apis/apps/v1/namespaces/${namespace}/deployments/${depName}`,
        'PATCH',
        patch
      )
      return jsonReply(res, 200, { replicas, success: true })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // GET /api/debug
  if (url.pathname === '/api/debug') {
    const info = {
      k8sServer,
      hasToken: !!k8sServer,
      tokenLength: getK8sToken().length,
      hasCaData: !!k8sCaData,
      nodeVersion: process.version,
      pid: process.pid,
    }
    try {
      const result = await k8sRequest('/apis/mooncake.io/v1alpha1/mooncakeclusters')
      info.apiSuccess = true
      info.itemsCount = result?.items?.length ?? 0
      info.firstItemName = result?.items?.[0]?.metadata?.name || 'none'
    } catch (e) {
      info.apiError = e.message
    }
    return jsonReply(res, 200, info)
  }

  // GET /api/images — list available mooncake-store images from all nodes
  if (url.pathname === '/api/images' && req.method === 'GET') {
    try {
      const nodesResult = await k8sRequest('/api/v1/nodes')
      const images = new Set()
      for (const node of (nodesResult?.items || [])) {
        for (const image of (node.status?.images || [])) {
          for (const name of (image.names || [])) {
            // match both docker.io/mooncake/mooncake-store:* and mooncake-store:* etc.
            if (name.includes('mooncake-store') || name.includes('mooncake/store')) {
              // strip leading docker.io/ if present
              const normalized = name.replace(/^docker\.io\//, '')
              images.add(normalized)
            }
          }
        }
      }
      const sorted = Array.from(images).sort()
      return jsonReply(res, 200, { images: sorted })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // GET /api/clusters
  if (url.pathname === '/api/clusters' && req.method === 'GET') {
    try {
      const namespace = url.searchParams.get('namespace')
      const apiPath = namespace
        ? `/apis/mooncake.io/v1alpha1/namespaces/${namespace}/mooncakeclusters`
        : '/apis/mooncake.io/v1alpha1/mooncakeclusters'
      const result = await k8sRequest(apiPath)
      return jsonReply(res, 200, { clusters: result?.items || [] })
    } catch (e) {
      // Cluster-scoped query failed (likely RBAC). Try the pod's own namespace.
      try {
        const ns = process.env.NAMESPACE || process.env.POD_NAMESPACE || 'default'
        const fallbackPath = `/apis/mooncake.io/v1alpha1/namespaces/${ns}/mooncakeclusters`
        console.warn(`[api-handler] Cluster-scoped listing failed, falling back to namespace "${ns}": ${e.message}`)
        const result = await k8sRequest(fallbackPath)
        return jsonReply(res, 200, { clusters: result?.items || [] })
      } catch (fallbackErr) {
        return jsonReply(res, 500, { error: `Cluster-level listing failed: ${e.message}; namespace fallback also failed: ${fallbackErr.message}` })
      }
    }
  }

  // GET /api/clusters/:namespace/:name/pods - list all pods for a cluster
  const podsMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/pods$/)
  if (podsMatch && req.method === 'GET') {
    try {
      const [, namespace, name] = podsMatch
      const result = await k8sRequest(
        `/api/v1/namespaces/${namespace}/pods?labelSelector=cluster=${name}`
      )
      return jsonReply(res, 200, { pods: result?.items || [] })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // GET /api/clusters/:namespace/:name/pod-metrics - pod CPU/memory usage via Metrics API
  const podMetricsMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/pod-metrics$/)
  if (podMetricsMatch && req.method === 'GET') {
    try {
      const [, namespace, name] = podMetricsMatch
      const result = await k8sRequest(
        `/apis/metrics.k8s.io/v1beta1/namespaces/${namespace}/pods?labelSelector=cluster=${name}`
      )
      return jsonReply(res, 200, { metrics: result?.items || [] })
    } catch (e) {
      // Metrics API may not be available; return empty
      return jsonReply(res, 200, { metrics: [], error: e.message })
    }
  }

  // GET /api/clusters/:namespace/:name/master-stats - proxy to each master pod's admin endpoints
  const masterStatsMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/master-stats$/)
  if (masterStatsMatch && req.method === 'GET') {
    try {
      const [, namespace, name] = masterStatsMatch
      const podResult = await k8sRequest(
        `/api/v1/namespaces/${namespace}/pods?labelSelector=app=mooncake-master,cluster=${name}`
      )
      const pods = podResult?.items || []

      const stats = await Promise.all(pods.map(async (pod) => {
        const podName = pod.metadata.name
        const podIP = pod.status.podIP || ''
        const nodeName = pod.spec.nodeName || ''
        const phase = pod.status.phase || ''
        const containerPort = pod.spec.containers?.[0]?.ports?.find(p => p.name === 'metrics')?.containerPort || 9003
        const proxyPrefix = `/api/v1/namespaces/${namespace}/pods/${podName}:${containerPort}/proxy`

        let health = null, summary = null, role = null, leader = null
        const tasks = [
          k8sRequest(`${proxyPrefix}/health`).then(r => { health = typeof r === 'string' ? JSON.parse(r) : r }).catch(() => {}),
          k8sRequest(`${proxyPrefix}/metrics/summary`).then(r => { summary = typeof r === 'string' ? r : JSON.stringify(r) }).catch(() => {}),
          k8sRequest(`${proxyPrefix}/role`).then(r => { role = typeof r === 'string' ? r.trim() : '' }).catch(() => {}),
          k8sRequest(`${proxyPrefix}/leader`).then(r => { leader = typeof r === 'string' ? JSON.parse(r) : r }).catch(() => {}),
        ]
        await Promise.all(tasks)

        return { name: podName, node: nodeName, phase, podIP, health, summary, role, leader }
      }))

      return jsonReply(res, 200, { stats })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // GET /api/clusters/:namespace/:name/worker-segments — query each worker's segment usage via master admin API
  const workerSegmentsMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/worker-segments$/)
  if (workerSegmentsMatch && req.method === 'GET') {
    try {
      const [, namespace, name] = workerSegmentsMatch
      const podResult = await k8sRequest(
        `/api/v1/namespaces/${namespace}/pods?labelSelector=app=mooncake-master,cluster=${name}`
      )
      const pods = podResult?.items || []
      if (pods.length === 0) {
        return jsonReply(res, 200, { segments: {} })
      }

      // Use first master pod to query segment info
      const masterPod = pods[0]
      const containerPort = masterPod.spec.containers?.[0]?.ports?.find(p => p.name === 'metrics')?.containerPort || 9003
      const proxyPrefix = `/api/v1/namespaces/${namespace}/pods/${masterPod.metadata.name}:${containerPort}/proxy`

      // Get all segment names
      const allSegmentsText = await k8sRequest(`${proxyPrefix}/get_all_segments`)
      const segmentNames = (typeof allSegmentsText === 'string' ? allSegmentsText : String(allSegmentsText))
        .split('\n')
        .map(s => s.trim())
        .filter(s => s.length > 0)

      // Query each segment's usage (limit concurrency to 5)
      const segments = {}
      const batchSize = 5
      for (let i = 0; i < segmentNames.length; i += batchSize) {
        const batch = segmentNames.slice(i, i + batchSize)
        const results = await Promise.all(batch.map(async (segName) => {
          try {
            const text = await k8sRequest(`${proxyPrefix}/query_segment?segment=${segName}`)
            const body = typeof text === 'string' ? text : String(text)
            const usedMatch = body.match(/Used\(bytes\):\s*(\d+)/)
            const capMatch = body.match(/Capacity\(bytes\)\s*:\s*(\d+)/)
            if (usedMatch && capMatch) {
              return { name: segName, used: parseInt(usedMatch[1]), capacity: parseInt(capMatch[1]) }
            }
          } catch (e) {
            // Skip segments that can't be queried
          }
          return null
        }))
        for (const r of results) {
          if (!r) continue
          // Extract IP from segment name: "10.244.1.5:12468" -> "10.244.1.5"
          const ip = r.name.split(':')[0]
          if (ip) segments[ip] = { used: r.used, capacity: r.capacity }
        }
      }

      return jsonReply(res, 200, { segments })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // GET /api/clusters/:namespace/:name
  const clusterMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)$/)
  if (clusterMatch && req.method === 'GET') {
    try {
      const [, namespace, name] = clusterMatch
      const result = await k8sRequest(
        `/apis/mooncake.io/v1alpha1/namespaces/${namespace}/mooncakeclusters/${name}`
      )
      return jsonReply(res, 200, { cluster: result })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // PUT /api/clusters/:namespace/:name — update cluster
  if (clusterMatch && req.method === 'PUT') {
    console.log('[api-handler] PUT /api/clusters starting')
    let body = ''
    for await (const chunk of req) body += chunk
    console.log('[api-handler] PUT body length:', body.length)
    try {
      const [, namespace, name] = clusterMatch
      const patch = JSON.parse(body)
      // First fetch current CR to get resourceVersion for optimistic concurrency
      const current = await k8sRequest(
        `/apis/mooncake.io/v1alpha1/namespaces/${namespace}/mooncakeclusters/${name}`
      )
      // Deep-merge patch.spec into existing spec so omitted fields are preserved
      const mergedSpec = JSON.parse(JSON.stringify(current.spec))
      if (patch.spec) {
        deepMerge(mergedSpec, patch.spec)
      }
      const updated = {
        apiVersion: 'mooncake.io/v1alpha1',
        kind: 'MooncakeCluster',
        metadata: {
          name,
          namespace,
          resourceVersion: current.metadata.resourceVersion,
        },
        spec: mergedSpec,
      }
      const result = await k8sRequest(
        `/apis/mooncake.io/v1alpha1/namespaces/${namespace}/mooncakeclusters/${name}`,
        'PUT',
        updated
      )
      console.log('[api-handler] Cluster updated successfully')
      return jsonReply(res, 200, { cluster: result })
    } catch (e) {
      console.log('[api-handler] PUT error:', e.message)
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // DELETE /api/clusters/:namespace/:name
  if (clusterMatch && req.method === 'DELETE') {
    try {
      const [, namespace, name] = clusterMatch
      await k8sRequest(
        `/apis/mooncake.io/v1alpha1/namespaces/${namespace}/mooncakeclusters/${name}`,
        'DELETE'
      )
      return jsonReply(res, 200, { success: true })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // POST /api/clusters
  if (url.pathname === '/api/clusters' && req.method === 'POST') {
    console.log('[api-handler] POST /api/clusters starting')
    let body = ''
    for await (const chunk of req) body += chunk
    console.log('[api-handler] POST body length:', body.length)
    try {
      const cr = JSON.parse(body)
      const namespace = cr.metadata?.namespace || 'default'
      console.log('[api-handler] Creating cluster:', cr.metadata?.name, 'in namespace:', namespace)
      const result = await k8sRequest(
        `/apis/mooncake.io/v1alpha1/namespaces/${namespace}/mooncakeclusters`,
        'POST',
        cr
      )
      console.log('[api-handler] Cluster created successfully')
      return jsonReply(res, 201, { cluster: result })
    } catch (e) {
      console.log('[api-handler] POST error:', e.message)
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // Helper: parse data size string (e.g. "1MB", "512KB", "2GiB") to bytes
  function parseDataSize(str) {
    if (!str || typeof str !== 'string') return null
    const s = str.trim().toUpperCase()
    const m = s.match(/^(\d+(?:\.\d+)?)\s*(?:B)?\s*(KB|MB|GB|TB|KIB|MIB|GIB|TIB|K|M|G|T)?$/)
    if (!m) return null
    const val = parseFloat(m[1])
    const unit = m[2] || 'B'
    const multipliers = { B: 1, K: 1024, KB: 1024, KIB: 1024, M: 1024 * 1024, MB: 1024 * 1024, MIB: 1024 * 1024, G: 1024 * 1024 * 1024, GB: 1024 * 1024 * 1024, GIB: 1024 * 1024 * 1024, T: 1024 * 1024 * 1024 * 1024, TB: 1024 * 1024 * 1024 * 1024, TIB: 1024 * 1024 * 1024 * 1024 }
    return Math.floor(val * (multipliers[unit] || 1))
  }

  const testCreateMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/test$/)
  if (testCreateMatch && req.method === 'POST') {
    let body = ''
    for await (const chunk of req) body += chunk
    try {
      const [, namespace, name] = testCreateMatch
      const { dataSize, replicaNum, repeatCount } = JSON.parse(body)

      // Validate inputs
      const dataSizeBytes = parseDataSize(dataSize)
      if (!dataSizeBytes || dataSizeBytes < 1) {
        return jsonReply(res, 400, { error: 'Invalid dataSize. Use format like "1KB", "1MB", "512MB", "1GB"' })
      }
      if (dataSizeBytes > 10 * 1024 * 1024 * 1024) {
        return jsonReply(res, 400, { error: 'dataSize exceeds maximum of 10GB' })
      }
      const repNum = parseInt(replicaNum) || 3
      if (repNum < 1) {
        return jsonReply(res, 400, { error: 'replicaNum must be >= 1' })
      }
      const repCount = parseInt(repeatCount) || 16
      if (repCount < 1) {
        return jsonReply(res, 400, { error: 'repeatCount must be >= 1' })
      }

      // Fetch cluster CR to get image and default ports
      const cluster = await k8sRequest(
        `/apis/mooncake.io/v1alpha1/namespaces/${namespace}/mooncakeclusters/${name}`
      )
      const spec = cluster.spec || {}
      const image = spec.image || 'mooncake/mooncake-store:latest'
      const rpcPort = spec.master?.rpcPort || 50051
      const metadataPort = spec.master?.httpMetadataServerPort || 8080
      const segmentSize = spec.workers?.segmentSize || '4Gi'

      // Convert segmentSize to bytes for the Python script
      const segmentBytes = parseDataSize(segmentSize) || 4 * 1024 * 1024 * 1024

      // Validate dataSize doesn't exceed segmentSize
      if (dataSizeBytes > segmentBytes) {
        return jsonReply(res, 400, { error: `dataSize (${dataSize}) exceeds segmentSize (${segmentSize})` })
      }

      const masterAddr = name + '-master-headless.' + namespace + ':' + rpcPort
      // Use leader master's pod DNS for metadata server to avoid per-master metadata partition
      const leaderPod = await findLeaderPod(namespace, name)
      const metadataServer = 'http://' + leaderPod + '.' + name + '-master-headless.' + namespace + '.svc:' + metadataPort + '/metadata'

      const timestamp = Date.now()
      const jobName = name + '-test-' + timestamp

      // Python test script — embedded here
      const pythonScript = `import os, time, sys, uuid
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

master_addr = os.environ['MC_MASTER_ADDR']
metadata_server = os.environ['MC_METADATA_SERVER']
segment_size = int(os.environ['MC_SEGMENT_SIZE'])
local_buffer_size = int(os.environ.get('MC_LOCAL_BUFFER_SIZE', str(512 * 1024 * 1024)))
protocol = os.environ.get('MC_PROTOCOL', 'tcp')
data_size = int(os.environ['TEST_DATA_SIZE'])
replica_num = int(os.environ['TEST_REPLICA_NUM'])
repeat_count = int(os.environ['TEST_REPEAT_COUNT'])
key_prefix = os.environ.get('TEST_KEY_PREFIX', 'mooncake-test-')

hostname = os.environ['POD_IP']

store = MooncakeDistributedStore()
retcode = store.setup(hostname, metadata_server, segment_size, local_buffer_size, protocol, "", master_addr)
if retcode != 0:
    print(f"FATAL: store.setup returned {retcode}", flush=True)
    sys.exit(1)
print(f"Store setup OK. master={master_addr} metadata={metadata_server} segment={segment_size}", flush=True)

config = ReplicateConfig()
config.replica_num = replica_num

success = 0
failure = 0
total_bytes = 0
start_time = time.time()

for i in range(repeat_count):
    key = key_prefix + uuid.uuid4().hex
    value = os.urandom(data_size)
    t0 = time.time()
    try:
        ret = store.put(key=key, value=value, config=config)
        elapsed = time.time() - t0
        if ret == 0:
            success += 1
            total_bytes += data_size
            print(f"OK  [{i+1}/{repeat_count}] key={key} size={data_size}B replica={replica_num} {elapsed*1000:.1f}ms", flush=True)
        else:
            failure += 1
            print(f"FAIL[{i+1}/{repeat_count}] key={key} ret={ret} {elapsed*1000:.1f}ms", flush=True)
    except Exception as e:
        failure += 1
        print(f"FAIL[{i+1}/{repeat_count}] key={key} exception={e}", flush=True)

elapsed_total = time.time() - start_time
throughput = (total_bytes / elapsed_total) / (1024 * 1024) if elapsed_total > 0 else 0
print(f"\\n=== RESULTS ===", flush=True)
print(f"Success: {success}, Failed: {failure}, Total: {repeat_count}", flush=True)
print(f"Total data written: {total_bytes} bytes ({total_bytes / (1024 * 1024):.1f} MB)", flush=True)
print(f"Total time: {elapsed_total:.1f}s", flush=True)
print(f"Throughput: {throughput:.1f} MB/s", flush=True)

sys.exit(0 if failure == 0 else 1)`

      // Create the K8s Job with script inlined via bash heredoc (no ConfigMap needed)
      const bashCommand = `python3 << 'PYEOF'
${pythonScript}
PYEOF`

      const job = {
        apiVersion: 'batch/v1',
        kind: 'Job',
        metadata: { name: jobName, namespace },
        spec: {
          backoffLimit: 0,
          ttlSecondsAfterFinished: 3600,
          template: {
            spec: {
              restartPolicy: 'Never',
              containers: [{
                name: 'store-test',
                image: image,
                imagePullPolicy: 'IfNotPresent',
                command: ['bash', '-c', bashCommand],
                env: [
                  { name: 'MC_MASTER_ADDR', value: masterAddr },
                  { name: 'MC_METADATA_SERVER', value: metadataServer },
                  { name: 'MC_SEGMENT_SIZE', value: String(segmentBytes) },
                  { name: 'MC_LOCAL_BUFFER_SIZE', value: String(512 * 1024 * 1024) },
                  { name: 'MC_PROTOCOL', value: 'tcp' },
                  { name: 'POD_IP', valueFrom: { fieldRef: { fieldPath: 'status.podIP' } } },
                  { name: 'TEST_DATA_SIZE', value: String(dataSizeBytes) },
                  { name: 'TEST_REPLICA_NUM', value: String(repNum) },
                  { name: 'TEST_REPEAT_COUNT', value: String(repCount) },
                  { name: 'LD_LIBRARY_PATH', value: '/usr/local/lib/mooncake' },
                ],
              }],
            },
          },
        },
      }
      await k8sRequest(`/apis/batch/v1/namespaces/${namespace}/jobs`, 'POST', job)

      console.log(`[api-handler] Test job created: ${jobName} in namespace ${namespace}`)
      return jsonReply(res, 201, { jobName, namespace })
    } catch (e) {
      console.log('[api-handler] POST test error:', e.message)
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // GET /api/clusters/:namespace/:name/test/:jobname — get test job status and logs
  const testStatusMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/test\/([^/]+)$/)
  if (testStatusMatch && req.method === 'GET') {
    try {
      const [, namespace, name, jobName] = testStatusMatch

      // Fetch Job status
      let jobStatus = 'Pending'
      let succeeded = 0, failed = 0, active = 0
      try {
        const jobResult = await k8sRequest(`/apis/batch/v1/namespaces/${namespace}/jobs/${jobName}`)
        const st = jobResult.status || {}
        succeeded = st.succeeded || 0
        failed = st.failed || 0
        active = st.active || 0
        if (succeeded > 0) jobStatus = 'Succeeded'
        else if (failed > 0) jobStatus = 'Failed'
        else if (active > 0) jobStatus = 'Running'
      } catch (e) {
        // Job not found (may have been TTL-deleted)
        return jsonReply(res, 200, { status: 'Expired', logs: 'Job has expired (TTL 1h).', jobName, podName: null })
      }

      // Find associated pod
      let podName = null
      let logs = ''
      try {
        const podResult = await k8sRequest(`/api/v1/namespaces/${namespace}/pods?labelSelector=job-name=${jobName}`)
        const pods = podResult?.items || []
        if (pods.length > 0) {
          podName = pods[0].metadata.name
          // Fetch logs — k8sRequest returns raw text when JSON.parse fails (existing fallback at line 62-67)
          const logResult = await k8sRequest(`/api/v1/namespaces/${namespace}/pods/${podName}/log?tailLines=500`)
          logs = typeof logResult === 'string' ? logResult : JSON.stringify(logResult)
        }
      } catch (e) {
        // Pod may not exist yet or logs not available
        logs = ''
      }

      return jsonReply(res, 200, { status: jobStatus, logs, jobName, podName })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

// Helper: call master HTTP API directly via ClusterIP service (avoids K8s pod proxy POST body corruption)
async function callMasterAPI(namespace, name, method, apiPath, body) {
  let metricsPort = 9003
  try {
    const clusterCR = await k8sRequest(
      `/apis/mooncake.io/v1alpha1/namespaces/${namespace}/mooncakeclusters/${name}`
    )
    if (clusterCR.spec?.master?.metricsPort) metricsPort = clusterCR.spec.master.metricsPort
  } catch (e) {
    console.warn(`[api-handler] Failed to fetch cluster CR, using metricsPort=${metricsPort}: ${e.message}`)
  }

  return new Promise((resolve, reject) => {
    const options = {
      hostname: `${name}-master.${namespace}.svc`,
      port: metricsPort,
      path: apiPath,
      method,
      headers: { 'Accept': 'application/json' },
    }
    if (body) {
      const bodyStr = typeof body === 'string' ? body : JSON.stringify(body)
      options.headers['Content-Type'] = 'application/json'
      options.headers['Content-Length'] = Buffer.byteLength(bodyStr)
    }
    const req = http.request(options, (res) => {
      let data = ''
      res.on('data', (chunk) => data += chunk)
      res.on('end', () => {
        try {
          resolve(JSON.parse(data))
        } catch (e) {
          resolve(data)
        }
      })
    })
    req.on('error', reject)
    if (body) req.write(typeof body === 'string' ? body : JSON.stringify(body))
    req.end()
  })
}

// POST /api/clusters/:namespace/:name/delete-pod — delete a pod via K8s API
const deletePodMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/delete-pod$/)
if (deletePodMatch && req.method === 'POST') {
  let body = ''
  for await (const chunk of req) body += chunk
  try {
    const [, namespace] = deletePodMatch
    const { podName } = JSON.parse(body)
    if (!podName) return jsonReply(res, 400, { error: 'podName is required' })

    await k8sRequest(`/api/v1/namespaces/${namespace}/pods/${podName}`, 'DELETE')
    return jsonReply(res, 200, { success: true })
  } catch (e) {
    return jsonReply(res, 500, { error: e.message })
  }
}

// GET /api/clusters/:namespace/:name/drain-status
const drainStatusMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/drain-status$/)
if (drainStatusMatch && req.method === 'GET') {
  try {
    const [, namespace, name] = drainStatusMatch
    const clusterKey = `${namespace}/${name}`

    const podResult = await k8sRequest(
      `/api/v1/namespaces/${namespace}/pods?labelSelector=cluster=${name}`
    )
    const allPods = podResult?.items || []
    const workerPods = allPods.filter(p => p.metadata.name.includes('-worker-'))

    const statuses = {}

    for (const pod of workerPods) {
      const podIP = pod.status.podIP
      if (!podIP) continue

      const entryKey = `${clusterKey}/${podIP}`
      const tracked = drainJobs.get(entryKey)

      if (tracked) {
        try {
          const result = await callMasterAPI(namespace, name, 'GET', `/api/v1/drain_jobs/query?job_id=${tracked.jobId}`)
          const statusIdx = parseInt(result.status)
          const statusStr = drainStatusEnum[statusIdx] || 'UNKNOWN'
          statuses[podIP] = {
            status: statusStr,
            jobId: tracked.jobId,
            migratedBytes: result.migrated_bytes || 0,
            speedMbps: parseFloat(result.speed_mbps) || 0,
            succeededUnits: result.succeeded_units || 0,
            failedUnits: result.failed_units || 0,
            message: result.message || '',
          }
          tracked.lastStatus = statusStr
          tracked.migratedBytes = result.migrated_bytes || 0
          tracked.succeededUnits = result.succeeded_units || 0
          tracked.failedUnits = result.failed_units || 0
        } catch (e) {
          statuses[podIP] = {
            status: tracked.lastStatus || 'UNKNOWN',
            jobId: tracked.jobId,
            error: e.message,
          }
        }
      } else {
        statuses[podIP] = { status: 'N/A' }
      }
    }

    return jsonReply(res, 200, { statuses })
  } catch (e) {
    return jsonReply(res, 500, { error: e.message })
  }
}

// POST /api/clusters/:namespace/:name/drain-worker
const drainWorkerMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/drain-worker$/)
if (drainWorkerMatch && req.method === 'POST') {
  let body = ''
  for await (const chunk of req) body += chunk
  try {
    const [, namespace, name] = drainWorkerMatch
    const { podIP, podName, maxConcurrency, bandwidthMBPS, targetIPs } = JSON.parse(body)
    if (!podIP) return jsonReply(res, 400, { error: 'podIP is required' })

    const clusterKey = `${namespace}/${name}`
    const entryKey = `${clusterKey}/${podIP}`

    // Reject if already has an active drain job
    const existing = drainJobs.get(entryKey)
    if (existing && ['CREATED', 'PLANNING', 'RUNNING'].includes(existing.lastStatus)) {
      return jsonReply(res, 409, { error: 'Worker already has an active drain job', jobId: existing.jobId })
    }

    const mc = Math.min(Math.max(parseInt(maxConcurrency) || 4, 1), 64)
    const bw = Math.max(parseInt(bandwidthMBPS) || 0, 0)

    // Resolve pod IP from K8s API by podName (handles dynamic IP changes after restart)
    let resolvedPodIP = podIP
    if (podName) {
      try {
        const podDetail = await k8sRequest(`/api/v1/namespaces/${namespace}/pods/${podName}`)
        if (podDetail?.status?.podIP) {
          resolvedPodIP = podDetail.status.podIP
          console.log(`[drain-worker] Resolved ${podName} -> ${resolvedPodIP} (provided IP was ${podIP})`)
        }
      } catch (e) {
        console.warn(`[drain-worker] Failed to look up pod ${podName}, using provided IP ${podIP}: ${e.message}`)
      }
    }

    // Get segment names from master's authoritative segment list (not Prometheus /metrics)
    const segmentsText = await callMasterAPI(namespace, name, 'GET', '/get_all_segments')
    const allSegments = (typeof segmentsText === 'string' ? segmentsText : String(segmentsText))
      .split('\n')
      .map(s => s.trim())
      .filter(s => s.length > 0)

    const segmentName = allSegments.find(s => s.startsWith(`${resolvedPodIP}:`))
    if (!segmentName) {
      return jsonReply(res, 404, { error: `No segment found for pod ${podName || podIP} (IP: ${resolvedPodIP}) on master` })
    }

    const targetSegments = Array.isArray(targetIPs) && targetIPs.length > 0
      ? targetIPs.map(ip => allSegments.find(s => s.startsWith(`${ip}:`))).filter(Boolean)
      : undefined

    const drainBody = {
      segments: [segmentName],
      max_concurrency: mc,
      bandwidth_mbps: bw,
    }
    if (targetSegments) drainBody.target_segments = targetSegments

    const result = await callMasterAPI(namespace, name, 'POST', '/api/v1/drain_jobs', drainBody)

    const jobId = result.id || result.job_id
    drainJobs.set(entryKey, {
      jobId,
      podIP,
      lastStatus: 'CREATED',
      migratedBytes: 0,
      succeededUnits: 0,
      failedUnits: 0,
      createdAt: Date.now(),
    })

    return jsonReply(res, 200, { jobId, status: 'CREATED' })
  } catch (e) {
    return jsonReply(res, 500, { error: e.message })
  }
}

// POST /api/clusters/:namespace/:name/cancel-drain
const cancelDrainMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/cancel-drain$/)
if (cancelDrainMatch && req.method === 'POST') {
  let body = ''
  for await (const chunk of req) body += chunk
  try {
    const [, namespace, name] = cancelDrainMatch
    const { jobId } = JSON.parse(body)
    if (!jobId) return jsonReply(res, 400, { error: 'jobId is required' })

    await callMasterAPI(namespace, name, 'POST', '/api/v1/drain_jobs/cancel', { job_id: jobId })

    // Remove tracking for all entries with this jobId
    for (const [key, val] of drainJobs) {
      if (val.jobId === jobId) drainJobs.delete(key)
    }

    return jsonReply(res, 200, { success: true })
  } catch (e) {
    return jsonReply(res, 500, { error: e.message })
  }
}

      return false // not handled
  } catch (e) {
    console.error('[api-handler] Unhandled error:', e.message)
    return jsonReply(res, 500, { error: e.message })
  }
}

module.exports = { handleApiRequest }
