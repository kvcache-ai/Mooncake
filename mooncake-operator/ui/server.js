const path = require('path')
const fs = require('fs')
const http = require('http')
const crypto = require('crypto')
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

// --- Session management ---
const SESSION_TTL_MS = 24 * 60 * 60 * 1000 // 24 hours
const sessions = new Map() // token -> { createdAt }

const VALID_USERNAME = process.env.UI_USERNAME || 'admin'
const VALID_PASSWORD = process.env.UI_PASSWORD || 'admin123'

function createSession() {
  const token = crypto.randomBytes(32).toString('hex')
  sessions.set(token, { createdAt: Date.now() })
  return token
}

function isValidSession(token) {
  const entry = sessions.get(token)
  if (!entry) return false
  if (Date.now() - entry.createdAt > SESSION_TTL_MS) {
    sessions.delete(token)
    return false
  }
  return true
}

function destroySession(token) {
  sessions.delete(token)
}

function parseCookies(cookieHeader) {
  const cookies = {}
  if (!cookieHeader) return cookies
  for (const part of cookieHeader.split(';')) {
    const [key, ...rest] = part.split('=')
    if (key) cookies[key.trim()] = rest.join('=').trim()
  }
  return cookies
}

function isPublicPath(pathname) {
  if (pathname === '/login') return true
  if (pathname.startsWith('/api/auth/')) return true
  if (pathname.startsWith('/_next/')) return true
  if (pathname === '/favicon.ico') return true
  return false
}

app.prepare().then(() => {
  const server = http.createServer(async (req, res) => {
    const parsedUrl = parse(req.url, true)

    // --- Auth check ---
    if (!isPublicPath(parsedUrl.pathname)) {
      const cookies = parseCookies(req.headers.cookie)
      if (!cookies.mc_session || !isValidSession(cookies.mc_session)) {
        res.writeHead(302, { Location: '/login' })
        res.end()
        return
      }
    }

    // If logged in and visiting /login, redirect to /
    if (parsedUrl.pathname === '/login') {
      const cookies = parseCookies(req.headers.cookie)
      if (cookies.mc_session && isValidSession(cookies.mc_session)) {
        res.writeHead(302, { Location: '/' })
        res.end()
        return
      }
    }

    // --- Auth API routes ---
    if (parsedUrl.pathname === '/api/auth/login' && req.method === 'POST') {
      let body = ''
      for await (const chunk of req) body += chunk
      try {
        const { username, password } = JSON.parse(body)
        if (username === VALID_USERNAME && password === VALID_PASSWORD) {
          const token = createSession()
          res.writeHead(200, {
            'Content-Type': 'application/json',
            'Set-Cookie': `mc_session=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${SESSION_TTL_MS / 1000}`,
          })
          res.end(JSON.stringify({ success: true }))
        } else {
          res.writeHead(401, { 'Content-Type': 'application/json' })
          res.end(JSON.stringify({ error: 'Invalid username or password' }))
        }
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ error: 'Bad request' }))
      }
      return
    }

    if (parsedUrl.pathname === '/api/auth/logout' && req.method === 'POST') {
      const cookies = parseCookies(req.headers.cookie)
      if (cookies.mc_session) destroySession(cookies.mc_session)
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Set-Cookie': 'mc_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0',
      })
      res.end(JSON.stringify({ success: true }))
      return
    }

    if (parsedUrl.pathname === '/api/auth/status' && req.method === 'GET') {
      const cookies = parseCookies(req.headers.cookie)
      const authenticated = !!(cookies.mc_session && isValidSession(cookies.mc_session))
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ authenticated }))
      return
    }

    // Intercept /api/* routes — handle directly in main process
    if (parsedUrl.pathname.startsWith('/api/')) {
      console.log('[server] api request:', req.method, parsedUrl.pathname)
      try {
        const handled = await handleApiRequest(req, res)
        console.log('[server] handleApiRequest returned:', handled)
        if (handled !== false) return
      } catch (e) {
        console.error('[api-handler] Error:', e.message)
        // Always try to send a JSON error response, even if headers were already sent.
        // res.end() is safe to call multiple times in Node.js and will not throw after
        // the response has been closed — it prevents "Unexpected end of JSON input" on
        // the client when the response headers were partially sent before the error.
        if (!res.headersSent) {
          res.writeHead(500, { 'Content-Type': 'application/json' })
        }
        try { res.end(JSON.stringify({ error: e.message })) } catch (_) { /* ignore */ }
        return
      }
    }

    handle(req, res, parsedUrl)
  })

  server.listen(currentPort, listenHost, (err) => {
    if (err) throw err
    console.log(`> Ready on http://${listenHost}:${currentPort}`)
  })
})
