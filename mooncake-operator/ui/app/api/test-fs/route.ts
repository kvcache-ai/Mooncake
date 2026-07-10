import { NextResponse } from 'next/server'
import { execSync } from 'child_process'
import * as fs from 'fs'

export async function GET() {
  const info: any = {}

  // Check if we can access known files
  info.packageJsonExists = fs.existsSync('/app/package.json')
  info.serverJsExists = fs.existsSync('/app/server.js')

  // Check service account
  info.saTokenExists = fs.existsSync('/var/run/secrets/kubernetes.io/serviceaccount/token')
  info.saCaExists = fs.existsSync('/var/run/secrets/kubernetes.io/serviceaccount/ca.crt')

  // Try to read the service account directory via ls
  try {
    info.lsOutput = execSync('ls -la /var/run/secrets/kubernetes.io/serviceaccount/').toString()
  } catch (e: any) {
    info.lsError = e?.stderr?.toString() || e?.message
  }

  // Try reading the token directly
  try {
    const token = fs.readFileSync('/var/run/secrets/kubernetes.io/serviceaccount/token', 'utf8')
    info.tokenLength = token.length
    info.tokenPrefix = token.substring(0, 20) + '...'
  } catch (e: any) {
    info.tokenReadError = e?.message
  }

  // Check if /var/run is a symlink and if it resolves
  try {
    info.varRunSymlink = fs.readlinkSync('/var/run')
  } catch (e: any) {
    info.varRunError = e?.message
  }

  // Check the mount point directly
  try {
    info.runSecretsExists = fs.existsSync('/run/secrets/kubernetes.io/serviceaccount/token')
  } catch (e: any) {
    info.runSecretsError = e?.message
  }

  // User info
  try {
    info.whoami = execSync('whoami').toString().trim()
    info.id = execSync('id').toString().trim()
  } catch (e: any) {
    info.whoamiError = e?.message
  }

  // Check /proc/self/mountinfo for serviceaccount
  try {
    const mountinfo = fs.readFileSync('/proc/self/mountinfo', 'utf8')
    info.mountEntries = mountinfo.split('\n').filter(l => l.includes('serviceaccount') || l.includes('/run/secrets'))
  } catch (e: any) {
    info.mountInfoError = e?.message
  }

  // Check what's visible in /app
  try {
    info.appDirContents = fs.readdirSync('/app/')
  } catch (e: any) {
    info.appDirError = e?.message
  }

  // Check /proc/self namespace info
  try {
    info.pidNamespace = execSync('readlink /proc/self/ns/mnt').toString().trim()
    info.mntNamespace = execSync('readlink /proc/1/ns/mnt').toString().trim()
    info.sameNamespace = info.pidNamespace === info.mntNamespace
  } catch (e: any) {
    info.namespaceError = e?.message
  }

  // Check /proc/1/root
  try {
    info.proc1Root = fs.readdirSync('/proc/1/root/var/run/secrets/kubernetes.io/serviceaccount/')
  } catch (e: any) {
    info.proc1RootError = e?.message
  }

  // Try reading token via /proc/1/root
  try {
    const token = fs.readFileSync('/proc/1/root/var/run/secrets/kubernetes.io/serviceaccount/token', 'utf8')
    info.proc1TokenLength = token.length
  } catch (e: any) {
    info.proc1TokenError = e?.message
  }

  // Check PID of this process
  try {
    info.nodePid = execSync('cat /proc/self/stat').toString().split(' ')[0]
    info.ppid = execSync('cat /proc/self/stat').toString().split(' ')[3]
    info.parentPid = process.ppid
    info.processPid = process.pid
  } catch (e: any) {
    info.pidError = e?.message
  }

  return NextResponse.json(info)
}
