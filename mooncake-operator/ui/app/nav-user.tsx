'use client'

import { useRouter } from 'next/navigation'

export default function NavUser() {
  const router = useRouter()

  const handleLogout = async () => {
    try {
      await fetch('/api/auth/logout', { method: 'POST' })
    } catch {
      // ignore
    }
    router.push('/login')
  }

  return (
    <div className="flex items-center">
      <button
        onClick={handleLogout}
        className="text-sm text-gray-500 hover:text-gray-700 px-3 py-2 rounded-md hover:bg-gray-100"
      >
        Sign out
      </button>
    </div>
  )
}
