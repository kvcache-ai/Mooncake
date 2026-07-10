import { NextResponse } from 'next/server'
import { getOperatorStatus } from '@/lib/k8s'

export async function GET() {
  try {
    const status = await getOperatorStatus()
    return NextResponse.json(status)
  } catch (err: any) {
    console.error('API /operator error:', err?.message || err)
    return NextResponse.json(
      { pods: [], readyCount: 0, totalCount: 0, leader: null, error: err?.message || String(err) },
      { status: 500 }
    )
  }
}
