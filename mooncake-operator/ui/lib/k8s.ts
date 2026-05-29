import * as k8s from '@kubernetes/client-node'
import * as fs from 'fs'

interface K8sCredentials {
  token: string
  caData: string
  server: string
}

function loadKubeConfig(): k8s.KubeConfig {
  const kc = new k8s.KubeConfig()

  if (process.env.KUBECONFIG) {
    kc.loadFromFile(process.env.KUBECONFIG)
    return kc
  }

  // Try 1: credentials from globalThis (set by custom server.js before app.prepare())
  const globalCreds = (globalThis as any).__K8S_CREDENTIALS__
  if (globalCreds?.token && globalCreds?.caData) {
    kc.loadFromClusterAndUser(
      { name: 'inCluster', server: globalCreds.server, caData: globalCreds.caData, skipTLSVerify: false },
      { name: 'inClusterUser', token: globalCreds.token }
    )
    return kc
  }

  // Try 2: credentials from env vars
  if (process.env.K8S_SA_TOKEN && process.env.K8S_SA_CA) {
    const host = process.env.KUBERNETES_SERVICE_HOST || 'kubernetes.default.svc'
    const port = process.env.KUBERNETES_SERVICE_PORT || '443'
    kc.loadFromClusterAndUser(
      { name: 'inCluster', server: `https://${host}:${port}`, caData: process.env.K8S_SA_CA, skipTLSVerify: false },
      { name: 'inClusterUser', token: process.env.K8S_SA_TOKEN }
    )
    return kc
  }

  // Try 3: credentials file written by server.js
  for (const credFile of ['/app/k8s-credentials.json', '/tmp/k8s-credentials.json']) {
    try {
      if (fs.existsSync(credFile)) {
        const creds: K8sCredentials = JSON.parse(fs.readFileSync(credFile, 'utf8'))
        kc.loadFromClusterAndUser(
          { name: 'inCluster', server: creds.server, caData: creds.caData, skipTLSVerify: false },
          { name: 'inClusterUser', token: creds.token }
        )
        return kc
      }
    } catch {}
  }

  // Try 4: direct filesystem (works when not in isolated namespace)
  const tokenPath = '/var/run/secrets/kubernetes.io/serviceaccount/token'
  const caPath = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
  if (fs.existsSync(tokenPath)) {
    const host = process.env.KUBERNETES_SERVICE_HOST || 'kubernetes.default.svc'
    const port = process.env.KUBERNETES_SERVICE_PORT || '443'
    const token = fs.readFileSync(tokenPath, 'utf8').trim()
    const caData = fs.readFileSync(caPath).toString('base64')
    kc.loadFromClusterAndUser(
      { name: 'inCluster', server: `https://${host}:${port}`, caData, skipTLSVerify: false },
      { name: 'inClusterUser', token }
    )
    return kc
  }

  kc.loadFromDefault()
  return kc
}

const kc = loadKubeConfig()
const k8sApi = kc.makeApiClient(k8s.CustomObjectsApi)

export interface MooncakeCluster {
  apiVersion: string
  kind: string
  metadata: {
    name: string
    namespace: string
    creationTimestamp: string
  }
  spec: {
    image: string
    master: {
      replicas: number
      rpcPort: number
      metricsPort: number
    }
    workers: {
      replicas: number
      segmentSize: string
      rdmaEnabled: boolean
      gpuEnabled: boolean
    }
    ha: {
      type: string
    }
  }
  status?: {
    phase?: string
    masterReady?: number
    workerReady?: number
    leaderNode?: string
  }
}

export async function listMooncakeClusters(namespace?: string): Promise<MooncakeCluster[]> {
  try {
    const resp = await k8sApi.listClusterCustomObject(
      'mooncake.io',
      'v1alpha1',
      'mooncakeclusters',
      undefined,
      undefined,
      undefined,
      undefined,
      namespace ? `metadata.namespace=${namespace}` : undefined
    )
    return (resp.body as any).items || []
  } catch (err) {
    console.error('Failed to list MooncakeClusters:', err)
    return []
  }
}

export async function getMooncakeCluster(namespace: string, name: string): Promise<MooncakeCluster | null> {
  try {
    const resp = await k8sApi.getClusterCustomObject(
      'mooncake.io',
      'v1alpha1',
      'mooncakeclusters',
      name,
    )
    return resp.body as MooncakeCluster
  } catch (err) {
    console.error('Failed to get MooncakeCluster:', err)
    return null
  }
}

export async function deleteMooncakeCluster(namespace: string, name: string): Promise<boolean> {
  try {
    await k8sApi.deleteClusterCustomObject(
      'mooncake.io',
      'v1alpha1',
      'mooncakeclusters',
      name,
    )
    return true
  } catch (err) {
    console.error('Failed to delete MooncakeCluster:', err)
    return false
  }
}
