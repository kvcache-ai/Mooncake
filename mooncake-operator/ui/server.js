const path = require('path')
const fs = require('fs')
const http = require('http')
const { parse } = require('url')
const next = require('next')
const { handleApiRequest } = require('./lib/api-handler')

const dir = path.join(__dirname)

process.env.NODE_ENV = 'production'
process.chdir(__dirname)

const app = next({ dir, dev: false })
const handle = app.getRequestHandler()

const currentPort = parseInt(process.env.PORT, 10) || 3000
const listenHost = '0.0.0.0'

app.prepare().then(() => {
  const server = http.createServer(async (req, res) => {
    const parsedUrl = parse(req.url, true)

    // Intercept /api/* routes — handle directly in main process
    if (parsedUrl.pathname.startsWith('/api/')) {
      console.log('[server] api request:', req.method, parsedUrl.pathname)
      try {
        const handled = await handleApiRequest(req, res)
        console.log('[server] handleApiRequest returned:', handled)
        if (handled !== false) return
      } catch (e) {
        console.error('[api-handler] Error:', e.message)
      }
    }

    handle(req, res, parsedUrl)
  })

  server.listen(currentPort, listenHost, (err) => {
    if (err) throw err
    console.log(`> Ready on http://${listenHost}:${currentPort}`)
  })
})
