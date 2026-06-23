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
  // Ensure we never send an empty body — JSON.stringify(undefined) returns undefined,
  // which makes res.end() send an empty body and causes "Unexpected end of JSON input" on the client.
  const body = data !== undefined ? JSON.stringify(data) : '{}'
  res.writeHead(status, { 'Content-Type': 'application/json' })
  res.end(body)
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
    // Normalize: KiB → KIB, MiB → MIB, GiB → GIB, TiB → TIB
    let s = str.trim()
    s = s.replace(/([KMGTP])i$/i, '$1IB')
    s = s.replace(/([KMGTP])i([bB])/i, '$1I$2')
    s = s.toUpperCase()
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
      const rdmaEnabled = spec.workers?.rdmaEnabled || false
      const protocol = rdmaEnabled ? 'rdma' : 'tcp'

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
rdma_devices = os.environ.get('MC_RDMA_DEVICES', '')

store = MooncakeDistributedStore()
retcode = store.setup(hostname, metadata_server, segment_size, local_buffer_size, protocol, rdma_devices, master_addr)
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

store.close()
sys.exit(0 if failure == 0 else 1)`

      // Create the K8s Job with script inlined via bash heredoc (no ConfigMap needed)
      const bashCommand = `python3 << 'PYEOF'
${pythonScript}
PYEOF`

      const testContainer = {
        name: 'store-test',
        image: image,
        imagePullPolicy: 'IfNotPresent',
        command: ['bash', '-c', bashCommand],
        env: [
          { name: 'MC_MASTER_ADDR', value: masterAddr },
          { name: 'MC_METADATA_SERVER', value: metadataServer },
          { name: 'MC_SEGMENT_SIZE', value: String(segmentBytes) },
          { name: 'MC_LOCAL_BUFFER_SIZE', value: String(512 * 1024 * 1024) },
          { name: 'MC_PROTOCOL', value: protocol },
          { name: 'POD_IP', valueFrom: { fieldRef: { fieldPath: 'status.podIP' } } },
          { name: 'TEST_DATA_SIZE', value: String(dataSizeBytes) },
          { name: 'TEST_REPLICA_NUM', value: String(repNum) },
          { name: 'TEST_REPEAT_COUNT', value: String(repCount) },
          { name: 'LD_LIBRARY_PATH', value: '/usr/local/lib/mooncake' },
        ],
      }

      const podSpec = {
        restartPolicy: 'Never',
        containers: [testContainer],
      }

      // When RDMA is enabled, grant the test pod privileged access to /dev/infiniband
      if (rdmaEnabled) {
        testContainer.securityContext = { privileged: true }
        testContainer.env.push({ name: 'MC_RDMA_DEVICES', value: 'rxe-eth0' })
        podSpec.volumes = [{
          name: 'rdma-dev',
          volumeSource: { hostPath: { path: '/dev/infiniband', type: 'DirectoryOrCreate' } },
        }]
        testContainer.volumeMounts = [{
          name: 'rdma-dev',
          mountPath: '/dev/infiniband',
        }]
      }

      const job = {
        apiVersion: 'batch/v1',
        kind: 'Job',
        metadata: { name: jobName, namespace },
        spec: {
          backoffLimit: 0,
          ttlSecondsAfterFinished: 3600,
          template: {
            spec: podSpec,
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
          const podPhase = pods[0].status?.phase || ''
          const containerStatuses = pods[0].status?.containerStatuses || []

          // Early detection: check pod container state directly — don't wait for Job controller
          if (jobStatus === 'Pending' || jobStatus === 'Running') {
            for (const cs of containerStatuses) {
              if (cs.state?.terminated) {
                const exitCode = cs.state.terminated.exitCode
                if (exitCode === 0) jobStatus = 'Succeeded'
                else jobStatus = 'Failed'
                break
              }
              if (cs.state?.waiting?.reason === 'CrashLoopBackOff' || cs.state?.waiting?.reason === 'Error') {
                jobStatus = 'Failed'
                break
              }
            }
            if (jobStatus === 'Pending' && (podPhase === 'Succeeded' || podPhase === 'Failed')) {
              jobStatus = podPhase === 'Succeeded' ? 'Succeeded' : 'Failed'
            }
          }

          // Fetch logs
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

  // ── Benchmark Endpoints ────────────────────────────────────────────────

  // POST /api/clusters/:namespace/:name/benchmark — create a benchmark job
  const createBenchmarkMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/benchmark$/)
  if (createBenchmarkMatch && req.method === 'POST') {
    let body = ''
    for await (const chunk of req) body += chunk
    try {
      const [, namespace, name] = createBenchmarkMatch
      const { workload, objSize, duration, protocol, label } = JSON.parse(body)

      // Validate inputs
      const validWorkloads = ['put-only', 'full-cycle']
      if (!validWorkloads.includes(workload)) {
        return jsonReply(res, 400, { error: `workload must be one of: ${validWorkloads.join(', ')}` })
      }
      const objSz = parseInt(objSize) || 4096
      if (objSz < 1 || objSz > 100 * 1024 * 1024) {
        return jsonReply(res, 400, { error: 'objSize must be between 1 and 104857600 (100MB)' })
      }
      const dur = parseInt(duration) || 60
      if (dur < 5 || dur > 3600) {
        return jsonReply(res, 400, { error: 'duration must be between 5 and 3600 seconds' })
      }
      const validProtocols = ['rdma', 'tcp']
      if (!validProtocols.includes(protocol)) {
        return jsonReply(res, 400, { error: `protocol must be one of: ${validProtocols.join(', ')}` })
      }

      // Fetch cluster CR to get master / metadata server config
      const cluster = await k8sRequest(
        `/apis/mooncake.io/v1alpha1/namespaces/${namespace}/mooncakeclusters/${name}`
      )
      const spec = cluster.spec || {}
      const image = spec.image || 'mooncake/mooncake-store:latest'
      const rpcPort = spec.master?.rpcPort || 50051
      const metadataPort = spec.master?.httpMetadataServerPort || 8080
      const segmentSize = spec.workers?.segmentSize || '4Gi'
      const rdmaEnabled = protocol === 'rdma' ? (spec.workers?.rdmaEnabled !== false) : false

      // Parse data size helpers (reuse from test logic)
      function parseDataSize(str) {
        if (!str || typeof str !== 'string') return null
        let s = str.trim()
        s = s.replace(/([KMGTP])i$/i, '$1IB')
        s = s.replace(/([KMGTP])i([bB])/i, '$1I$2')
        s = s.toUpperCase()
        const m = s.match(/^(\d+(?:\.\d+)?)\s*(?:B)?\s*(KB|MB|GB|TB|KIB|MIB|GIB|TIB|K|M|G|T)?$/)
        if (!m) return null
        const val = parseFloat(m[1])
        const unit = m[2] || 'B'
        const multipliers = { B: 1, K: 1024, KB: 1024, KIB: 1024, M: 1024 * 1024, MB: 1024 * 1024, MIB: 1024 * 1024, G: 1024 * 1024 * 1024, GB: 1024 * 1024 * 1024, GIB: 1024 * 1024 * 1024, T: 1024 * 1024 * 1024 * 1024, TB: 1024 * 1024 * 1024 * 1024, TIB: 1024 * 1024 * 1024 * 1024 }
        return Math.floor(val * (multipliers[unit] || 1))
      }
      const segmentBytes = parseDataSize(segmentSize) || 4 * 1024 * 1024 * 1024

      const masterAddr = name + '-master-headless.' + namespace + ':' + rpcPort
      const leaderPod = await findLeaderPod(namespace, name)
      const metadataServer = 'http://' + leaderPod + '.' + name + '-master-headless.' + namespace + '.svc:' + metadataPort + '/metadata'

      const timestamp = Date.now()
      const benchLabel = label || ('benchmark-' + name + '-' + timestamp)
      const jobName = name + '-benchmark-' + timestamp

      // The benchmark Python script — inlined via heredoc
      const pythonScript = `import os, sys, time, json, signal, statistics, urllib.request

os.environ["LD_LIBRARY_PATH"] = "/usr/local/lib/mooncake:/usr/local/lib/python3.12/dist-packages/mooncake:" + os.environ.get("LD_LIBRARY_PATH", "")
sys.path.insert(0, "/usr/local/lib/python3.12/dist-packages")
from mooncake import store

PUSHGATEWAY = os.environ.get("PUSHGATEWAY", "pushgateway:9091")
TEST_LABEL = os.environ.get("TEST_LABEL", "benchmark")
OBJ_SIZE = int(os.environ.get("OBJ_SIZE", "4096"))
DURATION = int(os.environ.get("DURATION", "60"))
PROTOCOL = os.environ.get("PROTOCOL", "rdma")
WORKLOAD = os.environ.get("WORKLOAD", "put-only")
LOSS_PCT = os.environ.get("LOSS_PCT", "0")
MASTER_ADDR = os.environ.get("MASTER_ADDR", "")
METADATA_SERVER = os.environ.get("METADATA_SERVER", "")
SEGMENT_SIZE = int(os.environ.get("SEGMENT_SIZE", "536870912"))
LOCAL_BUFFER_SIZE = int(os.environ.get("LOCAL_BUFFER_SIZE", "268435456"))
NETWORK_IF = os.environ.get("NETWORK_IF", "")

running = True
def _handler(sig, frame):
    global running
    running = False
signal.signal(signal.SIGINT, _handler)
signal.signal(signal.SIGTERM, _handler)

def push_metrics(metrics):
    lines = []
    for name, value, labels in metrics:
        label_str = ",".join(k + '="' + v + '"' for k, v in labels.items())
        lines.append(name + "{" + label_str + "} " + str(value))
    data = "\\n".join(lines) + "\\n"
    try:
        req = urllib.request.Request(
            "http://" + PUSHGATEWAY + "/metrics/job/mooncake/instance/" + TEST_LABEL,
            data=data.encode(),
            method="PUT"
        )
        urllib.request.urlopen(req, timeout=3)
    except Exception:
        pass

hostname = os.environ.get("POD_IP", "0.0.0.0")
if not MASTER_ADDR or not METADATA_SERVER:
    print("FATAL: MASTER_ADDR and METADATA_SERVER must be set", flush=True)
    sys.exit(1)

s = store.MooncakeDistributedStore()
rc = s.setup(hostname, METADATA_SERVER, SEGMENT_SIZE, LOCAL_BUFFER_SIZE, PROTOCOL, NETWORK_IF, MASTER_ADDR)
if rc != 0:
    print("FATAL: setup failed rc=" + str(rc), flush=True)
    sys.exit(1)
print("[SETUP OK] hostname=" + hostname + " master=" + MASTER_ADDR + " protocol=" + PROTOCOL + " workload=" + WORKLOAD, flush=True)

d = os.urandom(OBJ_SIZE)
start = time.time()
end_time = start + DURATION

total_ops = 0
total_errors = 0
all_latencies = []
interval_latencies = []

push_interval = 1.0
last_push = start
prev_ops = 0
prev_errors = 0
last_print = start

print("[BENCH] Running " + WORKLOAD + " for " + str(DURATION) + "s...", flush=True)
while running and time.time() < end_time:
    key = TEST_LABEL + "_" + str(total_ops)
    try:
        t0 = time.time()
        if WORKLOAD == "full-cycle":
            try:
                rc1 = s.put(key, d)
                _val = s.get(key)
                rc3 = s.remove(key)
                op_ok = True
            except Exception as _e:
                print("[FULLCYCLE_ERROR] " + str(_e), flush=True)
                op_ok = False
        else:
            rc1 = s.put(key, d)
            op_ok = (rc1 == 0)

        t1 = time.time()
        if op_ok:
            lat = (t1 - t0) * 1000
            all_latencies.append(lat)
            interval_latencies.append(lat)
            total_ops += 1
        else:
            total_errors += 1
    except Exception:
        total_errors += 1

    now = time.time()
    if now - last_push >= push_interval:
        elapsed = now - start
        if elapsed > 0:
            labels = {"test_label": TEST_LABEL, "protocol": PROTOCOL, "loss_pct": LOSS_PCT}

            lats_sorted = sorted(interval_latencies)
            p50 = statistics.median(interval_latencies) if interval_latencies else 0
            p99 = lats_sorted[min(int(len(lats_sorted)*0.99), len(lats_sorted)-1)] if len(lats_sorted) > 10 else (interval_latencies[-1] if interval_latencies else 0)
            avg_lat = statistics.mean(interval_latencies) if interval_latencies else 0

            interval_elapsed = now - last_push
            interval_ops = total_ops - prev_ops
            ops_per_s = interval_ops / interval_elapsed if interval_elapsed > 0 else 0
            mb_per_s = interval_ops * OBJ_SIZE / interval_elapsed / 1024 / 1024 if interval_elapsed > 0 else 0

            push_metrics([
                ("mooncake_ops_total", total_ops, labels),
                ("mooncake_bytes_total", total_ops * OBJ_SIZE, labels),
                ("mooncake_errors_total", total_errors, labels),
                ("mooncake_avg_latency_ms", avg_lat, labels),
                ("mooncake_p50_latency_ms", p50, labels),
                ("mooncake_p99_latency_ms", p99, labels),
                ("mooncake_throughput_mbps", mb_per_s, labels),
                ("mooncake_ops_per_sec", ops_per_s, labels),
            ])

            prev_ops = total_ops
            prev_errors = total_errors
            interval_latencies = []

        last_push = now

    if now - last_print >= 5:
        elapsed = now - start
        cur_mbps = total_ops * OBJ_SIZE / elapsed / 1024 / 1024 if elapsed > 0 else 0
        print("  [" + str(int(elapsed)) + "s] " + str(total_ops) + " ops, " + str(round(total_ops/elapsed, 1)) + " ops/s, " + str(round(cur_mbps, 2)) + " MB/s, " + str(total_errors) + " err", flush=True)
        last_print = now

elapsed = time.time() - start
if elapsed > 0:
    mbps = total_ops * OBJ_SIZE / elapsed / 1024 / 1024
    avg_lat = statistics.mean(all_latencies) if all_latencies else 0
    print("[DONE] " + str(total_ops) + " ops in " + str(round(elapsed, 1)) + "s = " + str(round(mbps, 2)) + " MB/s, " + str(round(total_ops/elapsed, 1)) + " ops/s, " + str(total_errors) + " err", flush=True)
    print("[LATENCY] avg=" + str(round(avg_lat, 3)) + "ms over " + str(len(all_latencies)) + " samples", flush=True)
else:
    print("[DONE] 0 ops", flush=True)

result = {
    "test": TEST_LABEL,
    "protocol": PROTOCOL,
    "loss_pct": LOSS_PCT,
    "workload": WORKLOAD,
    "obj_size": OBJ_SIZE,
    "duration": round(elapsed, 2),
    "ops": total_ops,
    "errors": total_errors,
    "throughput_mbps": round(mbps, 2) if elapsed > 0 else 0,
    "ops_per_sec": round(total_ops/elapsed, 1) if elapsed > 0 else 0,
    "avg_latency_ms": round(statistics.mean(all_latencies), 3) if all_latencies else 0,
}
os.makedirs("/tmp/monitor-results", exist_ok=True)
with open("/tmp/monitor-results/" + TEST_LABEL + ".json", "w") as f:
    json.dump(result, f, indent=2)
print("[RESULT] " + json.dumps(result), flush=True)

try:
    s.close()
except:
    pass`

      const bashCommand = 'python3 << \'PYEOF\'\n' + pythonScript + '\nPYEOF'

      const env = [
        { name: 'POD_IP', valueFrom: { fieldRef: { fieldPath: 'status.podIP' } } },
        { name: 'LD_LIBRARY_PATH', value: '/usr/local/lib/mooncake' },
        { name: 'MASTER_ADDR', value: masterAddr },
        { name: 'METADATA_SERVER', value: metadataServer },
        { name: 'SEGMENT_SIZE', value: String(segmentBytes) },
        { name: 'LOCAL_BUFFER_SIZE', value: String(256 * 1024 * 1024) },
        { name: 'PROTOCOL', value: protocol },
        { name: 'WORKLOAD', value: workload },
        { name: 'PUSHGATEWAY', value: process.env.PUSHGATEWAY || 'pushgateway:9091' },
        { name: 'TEST_LABEL', value: benchLabel },
        { name: 'OBJ_SIZE', value: String(objSz) },
        { name: 'DURATION', value: String(dur) },
      ]

      // When RDMA is enabled, set network interface and add privileged access
      if (rdmaEnabled) {
        env.push({ name: 'NETWORK_IF', value: 'rxe-eth0' })
      }

      const benchmarkContainer = {
        name: 'benchmark',
        image: image,
        imagePullPolicy: 'IfNotPresent',
        command: ['bash', '-c', bashCommand],
        env: env,
      }

      const podSpec = {
        restartPolicy: 'Never',
        containers: [benchmarkContainer],
      }

      if (rdmaEnabled) {
        benchmarkContainer.securityContext = { privileged: true }
        podSpec.volumes = [{
          name: 'rdma-dev',
          volumeSource: { hostPath: { path: '/dev/infiniband', type: 'DirectoryOrCreate' } },
        }]
        benchmarkContainer.volumeMounts = [{
          name: 'rdma-dev',
          mountPath: '/dev/infiniband',
        }]
      }

      const job = {
        apiVersion: 'batch/v1',
        kind: 'Job',
        metadata: {
          name: jobName,
          namespace,
          labels: { app: 'mooncake-benchmark', cluster: name },
        },
        spec: {
          backoffLimit: 0,
          ttlSecondsAfterFinished: 3600,
          template: { spec: podSpec },
        },
      }

      await k8sRequest('/apis/batch/v1/namespaces/' + namespace + '/jobs', 'POST', job)

      console.log('[api-handler] Benchmark job created: ' + jobName + ' in namespace ' + namespace)
      return jsonReply(res, 201, { jobName, namespace, label: benchLabel })
    } catch (e) {
      console.log('[api-handler] POST benchmark error:', e.message)
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // GET /api/clusters/:namespace/:name/benchmark/:jobname — get benchmark job status and logs
  const benchmarkStatusMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/benchmark\/([^/]+)$/)
  if (benchmarkStatusMatch && req.method === 'GET') {
    try {
      const [, namespace, name, jobName] = benchmarkStatusMatch

      // Fetch Job status
      let jobStatus = 'Pending'
      let succeeded = 0, failed = 0, active = 0
      try {
        const jobResult = await k8sRequest('/apis/batch/v1/namespaces/' + namespace + '/jobs/' + jobName)
        const st = jobResult.status || {}
        succeeded = st.succeeded || 0
        failed = st.failed || 0
        active = st.active || 0
        if (succeeded > 0) jobStatus = 'Succeeded'
        else if (failed > 0) jobStatus = 'Failed'
        else if (active > 0) jobStatus = 'Running'
      } catch (e) {
        return jsonReply(res, 200, { status: 'Expired', logs: 'Job has expired (TTL 1h).' })
      }

      // Find associated pod and fetch logs
      let podName = null
      let logs = ''
      let result = null
      try {
        const podResult = await k8sRequest('/api/v1/namespaces/' + namespace + '/pods?labelSelector=job-name=' + jobName)
        const pods = podResult?.items || []
        if (pods.length > 0) {
          podName = pods[0].metadata.name
          const podPhase = pods[0].status?.phase || ''
          const containerStatuses = pods[0].status?.containerStatuses || []

          // Early detection: check pod container state directly — don't wait for Job controller
          if (jobStatus === 'Pending' || jobStatus === 'Running') {
            for (const cs of containerStatuses) {
              if (cs.state?.terminated) {
                const exitCode = cs.state.terminated.exitCode
                if (exitCode === 0) {
                  jobStatus = 'Succeeded'
                } else {
                  jobStatus = 'Failed'
                }
                break
              }
              // Container waiting with CrashLoopBackOff or error
              if (cs.state?.waiting?.reason === 'CrashLoopBackOff' || cs.state?.waiting?.reason === 'Error') {
                jobStatus = 'Failed'
                break
              }
            }
            // Also use pod phase as signal (some runtimes set phase=Succeeded before job status updates)
            if (jobStatus === 'Pending' && (podPhase === 'Succeeded' || podPhase === 'Failed')) {
              jobStatus = podPhase === 'Succeeded' ? 'Succeeded' : 'Failed'
            }
          }

          const logResult = await k8sRequest('/api/v1/namespaces/' + namespace + '/pods/' + podName + '/log?tailLines=500')
          logs = typeof logResult === 'string' ? logResult : JSON.stringify(logResult)

          // Try to parse result JSON from the last line
          if (jobStatus === 'Succeeded' || jobStatus === 'Failed') {
            const lines = logs.split('\n').filter(l => l.trim())
            for (let i = lines.length - 1; i >= 0; i--) {
              const line = lines[i].trim()
              if (line.startsWith('[RESULT] ')) {
                try {
                  result = JSON.parse(line.substring(9))
                } catch (e) { /* ignore parse error */ }
                break
              }
            }
          }
        }
      } catch (e) {
        logs = ''
      }

      return jsonReply(res, 200, { status: jobStatus, logs, jobName, podName, result })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // GET /api/clusters/:namespace/:name/benchmarks — list all benchmark jobs for a cluster
  const listBenchmarksMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/benchmarks$/)
  if (listBenchmarksMatch && req.method === 'GET') {
    try {
      const [, namespace, name] = listBenchmarksMatch
      const result = await k8sRequest(
        '/apis/batch/v1/namespaces/' + namespace + '/jobs?labelSelector=app%3Dmooncake-benchmark%2Ccluster%3D' + name
      )
      const items = (result?.items || []).map(job => {
        const st = job.status || {}
        let status = 'Pending'
        if (st.succeeded > 0) status = 'Succeeded'
        else if (st.failed > 0) status = 'Failed'
        else if (st.active > 0) status = 'Running'
        return {
          name: job.metadata.name,
          status,
          created: job.metadata.creationTimestamp,
          completed: st.completionTime || null,
        }
      })
      // Sort by creation time descending
      items.sort((a, b) => new Date(b.created).getTime() - new Date(a.created).getTime())
      return jsonReply(res, 200, { benchmarks: items })
    } catch (e) {
      return jsonReply(res, 500, { error: e.message })
    }
  }

  // DELETE /api/clusters/:namespace/:name/benchmark/:jobname — delete a benchmark job
  const deleteBenchmarkMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/benchmark\/([^/]+)$/)
  if (deleteBenchmarkMatch && req.method === 'DELETE') {
    try {
      const [, namespace, , jobName] = deleteBenchmarkMatch
      await k8sRequest('/apis/batch/v1/namespaces/' + namespace + '/jobs/' + jobName, 'DELETE')
      return jsonReply(res, 200, { success: true })
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

      // GET /api/clusters/:namespace/:name/rdma-status
      // Returns per-worker RDMA availability status by checking each worker pod's
      // RDMA device allocation and health.
      const rdmaStatusMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/rdma-status$/)
      if (rdmaStatusMatch && req.method === 'GET') {
        try {
          const [, namespace, name] = rdmaStatusMatch

          // Check cluster CR for rdmaEnabled (covers Soft-RoCE / hostPath RDMA)
          let crRdmaEnabled = false
          try {
            const clusterCR = await k8sRequest(
              `/apis/mooncake.io/v1alpha1/namespaces/${namespace}/mooncakeclusters/${name}`
            )
            crRdmaEnabled = !!clusterCR?.spec?.workers?.rdmaEnabled
          } catch (e) {
            // CR not found; proceed with pod-level checks only
          }

          // Get all worker pods
          const podResult = await k8sRequest(
            `/api/v1/namespaces/${namespace}/pods?labelSelector=app=mooncake-worker,cluster=${name}`
          )
          const pods = podResult?.items || []

          // Get all nodes to check RDMA device allocatable
          const nodesResult = await k8sRequest('/api/v1/nodes')
          const nodeRdma = new Map()
          for (const node of (nodesResult?.items || [])) {
            const allocatable = node.status?.allocatable || {}
            const rdmaDevices = allocatable['rdma/hca_shared_devices_a']
            nodeRdma.set(node.metadata.name, rdmaDevices ? parseInt(rdmaDevices) : 0)
          }

          const workers = []
          for (const pod of pods) {
            const podName = pod.metadata?.name || ''
            const podIP = pod.status?.podIP || ''
            const nodeName = pod.spec?.nodeName || ''
            const phase = pod.status?.phase || ''
            const ready = (pod.status?.conditions || []).some(
              c => c.type === 'Ready' && c.status === 'True'
            )

            // Check if RDMA resources were allocated to this container
            let rdmaRequested = false
            let rdmaAllocated = 0
            let softRoce = false
            for (const container of (pod.spec?.containers || [])) {
              const limits = container.resources?.limits || {}
              if (limits['rdma/hca_shared_devices_a']) {
                rdmaRequested = true
                rdmaAllocated = parseInt(limits['rdma/hca_shared_devices_a'])
              }
              // Detect Soft-RoCE via /dev/infiniband hostPath mount
              const mounts = container.volumeMounts || []
              for (const m of mounts) {
                if (m.mountPath === '/dev/infiniband') softRoce = true
              }
            }

            const nodeRdmaCount = nodeRdma.get(nodeName) || 0

            // Also try to query the worker pod's transport health endpoint
            let rdmaAvailable = false
            let transportHealth = {}
            if (ready && podIP) {
              try {
                const containerPort = pod.spec?.containers?.[0]?.ports?.find(
                  p => p.name === 'http'
                )?.containerPort || 9300
                const proxyPrefix = `/api/v1/namespaces/${namespace}/pods/${podName}:${containerPort}/proxy`
                const healthText = await k8sRequest(`${proxyPrefix}/transport_health`)
                if (typeof healthText === 'string') {
                  const health = JSON.parse(healthText)
                  transportHealth = health
                  // Check if RDMA transport is healthy from runtime data
                  if (health.rdma) {
                    rdmaAvailable = health.rdma.healthy
                  } else if (health.mock_rdma) {
                    rdmaAvailable = health.mock_rdma.healthy
                  }
                }
              } catch (e) {
                // Transport health endpoint not available; fall back to resource/CR check
              }
            }

            // RDMA is available if: health says so, OR device plugin allocated, OR Soft-RoCE hostPath, OR CR says rdmaEnabled
            if (!rdmaAvailable) {
              rdmaAvailable = rdmaRequested && rdmaAllocated > 0
            }
            if (!rdmaAvailable) {
              rdmaAvailable = softRoce && crRdmaEnabled
            }
            if (!rdmaAvailable) {
              rdmaAvailable = crRdmaEnabled && ready
            }

            workers.push({
              podName,
              podIP,
              nodeName,
              phase,
              ready,
              rdmaAvailable,
              rdmaRequested: rdmaRequested || softRoce,
              rdmaAllocated: rdmaAllocated || (softRoce ? 1 : 0),
              softRoce,
              nodeRdmaDevices: nodeRdmaCount,
              transportHealth,
            })
          }

          return jsonReply(res, 200, { workers, crRdmaEnabled })
        } catch (e) {
          return jsonReply(res, 500, { error: e.message })
        }
      }


      // GET /api/clusters/:namespace/:name/worker-keys
      // Paginated list of KV cache keys owned by a specific worker segment.
      // Query params:
      //   segment  - the segment name, e.g. "10.244.1.132:13006" (required)
      //   page     - page number, 1-based (default 1)
      //   pageSize - keys per page (default 50, max 200)
      // Performance: batches replica queries (100 keys/batch, 5 concurrent) to
      // filter keys by segment. For clusters with >10k keys, only the first
      // 10k are scanned; a future master-side endpoint can improve this.
      const workerKeysMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/worker-keys$/)
      if (workerKeysMatch && req.method === 'GET') {
        try {
          const [, namespace, name] = workerKeysMatch
          const segment = url.searchParams.get('segment')
          const pageNum = Math.max(1, parseInt(url.searchParams.get('page')) || 1)
          const pageSize = Math.min(Math.max(1, parseInt(url.searchParams.get('pageSize')) || 50), 200)

          if (!segment) {
            return jsonReply(res, 400, { error: 'segment query parameter is required (e.g. ?segment=10.244.1.132:13006)' })
          }

          // 1. Get all keys from master
          let allKeysText = ''
          try {
            allKeysText = await callMasterAPI(namespace, name, 'GET', '/get_all_keys')
          } catch (e) {
            return jsonReply(res, 502, { error: 'Failed to fetch keys from master: ' + e.message })
          }

          const allKeys = (typeof allKeysText === 'string' ? allKeysText : String(allKeysText))
            .split('\n')
            .map(s => s.trim())
            .filter(s => s.length > 0)

          const MAX_SCAN = 10000
          const scannedKeys = allKeys.length > MAX_SCAN ? allKeys.slice(0, MAX_SCAN) : allKeys
          const truncated = allKeys.length > MAX_SCAN

          // 2. Batch query replicas to find keys belonging to target segment
          const BATCH_SIZE = 100
          const CONCURRENCY = 5
          const matchingKeys = []

          for (let i = 0; i < scannedKeys.length; i += BATCH_SIZE * CONCURRENCY) {
            const chunkBatches = []
            for (let j = 0; j < CONCURRENCY; j++) {
              const batchStart = i + j * BATCH_SIZE
              if (batchStart >= scannedKeys.length) break
              const batch = scannedKeys.slice(batchStart, batchStart + BATCH_SIZE)
              chunkBatches.push(batch)
            }

            const results = await Promise.all(chunkBatches.map(async (batch) => {
              try {
                const keysParam = batch.map(encodeURIComponent).join(',')
                const resp = await callMasterAPI(namespace, name, 'GET', '/batch_query_keys?keys=' + keysParam)
                // Returns: {"success":true,"data":{"key1":{"ok":true,"values":[{...}]},...}}
                if (resp && resp.success && resp.data) {
                  const batchMatches = []
                  for (const [key, info] of Object.entries(resp.data)) {
                    if (info.ok && info.values) {
                      for (const val of info.values) {
                        // Master API returns underscores in field names: "transport_endpoint_", "size_"
                        const ep = val.transport_endpoint_ || val.transport_endpoint || ''
                        if (ep === segment) {
                          batchMatches.push({ key, size: val.size_ || val.size || 0, transport_endpoint: ep })
                          break // key matched this segment, no need to check other replicas
                        }
                      }
                    }
                  }
                  return batchMatches
                }
              } catch (e) {
                // skip batch errors
              }
              return null
            }))

            for (const r of results) {
              if (r && r.length) matchingKeys.push(...r)
            }
          }

          // 3. Paginate
          const total = matchingKeys.length
          const totalPages = Math.max(1, Math.ceil(total / pageSize))
          const start = (pageNum - 1) * pageSize
          const page = matchingKeys.slice(start, start + pageSize)

          return jsonReply(res, 200, {
            segment,
            keys: page,
            total,
            page: pageNum,
            pageSize,
            totalPages,
            truncated,
            scannedCount: scannedKeys.length,
            totalKeyCount: allKeys.length,
          })
        } catch (e) {
          return jsonReply(res, 500, { error: e.message })
        }
      }


      // POST /api/clusters/:namespace/:name/clear-worker-data
      // Clear all KV cache data from a specific worker segment.
      // Body: { segment: "10.244.1.132:13006" }
      // Returns: { success: true, removed: N, failed: M, totalKeys: T }
      const clearWorkerDataMatch = url.pathname.match(/^\/api\/clusters\/([^/]+)\/([^/]+)\/clear-worker-data$/)
      if (clearWorkerDataMatch && req.method === 'POST') {
        let body = ''
        try {
          for await (const chunk of req) body += chunk
        } catch (e) {
          return jsonReply(res, 400, { error: 'Failed to read request body' })
        }
        let segment
        try {
          segment = JSON.parse(body).segment
        } catch (e) {
          return jsonReply(res, 400, { error: 'Invalid JSON body' })
        }
        if (!segment) {
          return jsonReply(res, 400, { error: 'segment is required' })
        }

        const [, namespace, name] = clearWorkerDataMatch

        try {
          // 1. Get all keys from master
          let allKeysText = ''
          try {
            allKeysText = await callMasterAPI(namespace, name, 'GET', '/get_all_keys')
          } catch (e) {
            return jsonReply(res, 502, { error: 'Failed to fetch keys from master: ' + e.message })
          }

          const allKeys = (typeof allKeysText === 'string' ? allKeysText : String(allKeysText))
            .split('\n')
            .map(s => s.trim())
            .filter(s => s.length > 0)

          const MAX_SCAN = 10000
          const MAX_REMOVE = 5000
          const scannedKeys = allKeys.length > MAX_SCAN ? allKeys.slice(0, MAX_SCAN) : allKeys

          // 2. Batch query replicas to find keys belonging to target segment
          const BATCH_SIZE = 100
          const CONCURRENCY = 5
          const matchingKeys = []

          for (let i = 0; i < scannedKeys.length; i += BATCH_SIZE * CONCURRENCY) {
            const chunkBatches = []
            for (let j = 0; j < CONCURRENCY; j++) {
              const batchStart = i + j * BATCH_SIZE
              if (batchStart >= scannedKeys.length) break
              const batch = scannedKeys.slice(batchStart, batchStart + BATCH_SIZE)
              chunkBatches.push(batch)
            }

            const results = await Promise.all(chunkBatches.map(async (batch) => {
              try {
                const keysParam = batch.map(encodeURIComponent).join(',')
                const resp = await callMasterAPI(namespace, name, 'GET', '/batch_query_keys?keys=' + keysParam)
                if (resp && resp.success && resp.data) {
                  const batchMatches = []
                  for (const [key, info] of Object.entries(resp.data)) {
                    if (info.ok && info.values) {
                      for (const val of info.values) {
                        const ep = val.transport_endpoint_ || val.transport_endpoint || ''
                        if (ep === segment) {
                          batchMatches.push(key)
                          break
                        }
                      }
                    }
                  }
                  return batchMatches
                }
              } catch (e) {
                // skip batch errors
              }
              return null
            }))

            for (const r of results) {
              if (r && r.length) matchingKeys.push(...r)
            }
          }

          if (matchingKeys.length === 0) {
            return jsonReply(res, 200, {
              success: true,
              segment,
              removed: 0,
              failed: 0,
              totalKeys: allKeys.length,
              scannedKeys: scannedKeys.length,
              message: 'No keys found for this segment',
            })
          }

          if (matchingKeys.length > MAX_REMOVE) {
            return jsonReply(res, 400, {
              error: `Too many keys (${matchingKeys.length}) to clear. Maximum ${MAX_REMOVE} keys allowed for safety.`,
            })
          }

          // 3. Remove all matching keys via the master's remove API
          let removeResult
          try {
            removeResult = await callMasterAPI(namespace, name, 'POST', '/api/v1/objects/remove', {
              keys: matchingKeys,
              force: true,
            })
          } catch (e) {
            return jsonReply(res, 502, { error: 'Failed to remove keys from master: ' + e.message })
          }

          return jsonReply(res, 200, {
            success: removeResult.success !== false,
            segment,
            removed: removeResult.removed || 0,
            failed: removeResult.failed || 0,
            total: matchingKeys.length,
            totalKeys: allKeys.length,
            scannedKeys: scannedKeys.length,
          })
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
