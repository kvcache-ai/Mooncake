import { NextResponse } from 'next/server'
import { scaleOperatorDeployment } from '@/lib/k8s'

export async function PUT(request: Request) {
  try {
    const { replicas } = await request.json()
    if (typeof replicas !== 'number' || replicas < 1 || !Number.isInteger(replicas)) {
      return NextResponse.json(
        { error: 'replicas must be an integer >= 1' },
        { status: 400 }
      )
    }
    const result = await scaleOperatorDeployment(replicas)
    return NextResponse.json(result)
  } catch (err: any) {
    console.error('API /operator/scale error:', err?.message || err)
    return NextResponse.json(
      { error: err?.message || String(err) },
      { status: 500 }
    )
  }
}
