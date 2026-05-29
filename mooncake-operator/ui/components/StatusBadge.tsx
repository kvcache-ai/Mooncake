export function StatusBadge({ phase }: { phase?: string }) {
  const colors: Record<string, string> = {
    Running: 'bg-green-100 text-green-800',
    Creating: 'bg-blue-100 text-blue-800',
    Updating: 'bg-yellow-100 text-yellow-800',
    Failed: 'bg-red-100 text-red-800',
    Deleting: 'bg-gray-100 text-gray-800',
  }

  const color = colors[phase || ''] || 'bg-gray-100 text-gray-800'

  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${color}`}>
      {phase || 'Unknown'}
    </span>
  )
}
