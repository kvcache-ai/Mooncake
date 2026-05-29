import { NextResponse } from 'next/server'
import { listMooncakeClusters } from '@/lib/k8s'

export async function GET() {
  try {
    const clusters = await listMooncakeClusters()
    return NextResponse.json({ clusters })
  } catch (err: any) {
    console.error('API /clusters error:', err?.message || err)
    return NextResponse.json({ error: err?.message || String(err), clusters: [] }, { status: 500 })
  }
}
