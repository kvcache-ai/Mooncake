import { NextResponse } from 'next/server'
import * as k8s from '@kubernetes/client-node'
import * as fs from 'fs'

export async function GET() {
  const info: any = {}

  // Check globalThis credentials (set by custom server.js)
  const globalCreds = (globalThis as any).__K8S_CREDENTIALS__
  info.globalThisCreds = globalCreds ? { tokenLen: globalCreds.token?.length, server: globalCreds.server, hasCaData: !!globalCreds.caData } : 'not set'

  // Check env vars
  info.KUBERNETES_SERVICE_HOST = process.env.KUBERNETES_SERVICE_HOST || 'not set'
  info.KUBERNETES_SERVICE_PORT = process.env.KUBERNETES_SERVICE_PORT || 'not set'
  info.KUBECONFIG = process.env.KUBECONFIG || 'not set'
  info.NODE_ENV = process.env.NODE_ENV || 'not set'
  info.K8S_SA_TOKEN = process.env.K8S_SA_TOKEN ? 'SET (len=' + process.env.K8S_SA_TOKEN.length + ')' : 'not set'
  info.K8S_SA_CA = process.env.K8S_SA_CA ? 'SET' : 'not set'

  // Check credential files
  for (const credPath of ['/app/k8s-credentials.json', '/tmp/k8s-credentials.json']) {
    try {
      const creds = JSON.parse(fs.readFileSync(credPath, 'utf8'))
      info['credsFrom_' + credPath] = { readable: true, tokenLen: creds.token?.length, server: creds.server }
      break
    } catch (e: any) {
      info['credsFrom_' + credPath] = { readable: false, error: e?.message }
    }
  }

  // Check service account files
  const tokenPath = '/var/run/secrets/kubernetes.io/serviceaccount/token'
  const caPath = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
  info.tokenFileExists = fs.existsSync(tokenPath)
  info.caFileExists = fs.existsSync(caPath)
  info.cwd = process.cwd()
  info.nodeVersion = process.version
  info.pid = process.ppid || process.pid
  try {
    const dirEntries = fs.readdirSync('/var/run/secrets/kubernetes.io/serviceaccount/')
    info.serviceAccountDir = dirEntries
  } catch (e: any) {
    info.serviceAccountDirError = e?.message
  }

  try {
    const kc = new k8s.KubeConfig()
    if (info.tokenFileExists) {
      const host = process.env.KUBERNETES_SERVICE_HOST || 'kubernetes.default.svc'
      const port = process.env.KUBERNETES_SERVICE_PORT || '443'
      const token = fs.readFileSync(tokenPath, 'utf8').trim()
      const caData = fs.readFileSync(caPath).toString('base64')
      kc.loadFromClusterAndUser(
        { name: 'inCluster', server: `https://${host}:${port}`, caData, skipTLSVerify: false },
        { name: 'inClusterUser', token }
      )
      info.authMethod = 'in-cluster-serviceaccount'
    } else {
      kc.loadFromDefault()
      info.authMethod = 'loadFromDefault'
    }
    info.kubeConfigLoaded = true
    info.currentCluster = kc.getCurrentCluster()?.server || 'none'

    const k8sApi = kc.makeApiClient(k8s.CustomObjectsApi)
    const resp = await k8sApi.listClusterCustomObject(
      'mooncake.io', 'v1alpha1', 'mooncakeclusters'
    )
    info.apiSuccess = true
    info.itemsCount = (resp.body as any)?.items?.length ?? 0
    info.firstItemName = (resp.body as any)?.items?.[0]?.metadata?.name || 'none'
  } catch (err: any) {
    info.apiError = err?.message || String(err)
    info.apiErrorBody = err?.body ? JSON.stringify(err.body).substring(0, 500) : 'none'
  }

  return NextResponse.json(info)
}
