import { serve } from "https://deno.land/std@0.224.0/http/server.ts"

export interface StoredRequest {
  request: Request
  body: unknown
  response: unknown
}

export interface TestServer {
  port: number
  storedEvents: Map<string, unknown>
  start: () => Promise<void>
  stop: () => Promise<void>
  reset: () => void
  failNextRequests: (count: number) => void
  getRequestCount: () => number
}

export function createTestServer(port = 8000): TestServer {
  let controller: AbortController | undefined
  let serverPromise: Promise<void> | undefined
  const storedEvents: Map<string, unknown> = new Map()
  let failureCount = 0
  let requestCount = 0

  return {
    port,
    storedEvents,
    reset() {
      storedEvents.clear()
      failureCount = 0
      requestCount = 0
    },
    failNextRequests(count: number) {
      failureCount = count
    },
    getRequestCount() {
      return requestCount
    },
    async start() {
      if (controller || serverPromise) {
        return
      }

      controller = new AbortController()
      const signal = controller.signal

      let startResolve: () => void
      const startPromise = new Promise<void>((resolve) => {
        startResolve = resolve
      })

      serverPromise = serve(async (req: Request) => {
        console.log("Received request:", req.url)
        requestCount++

        // Debug: Print all headers
        console.log("Request headers:")
        for (const [key, value] of req.headers.entries()) {
          console.log(`  ${key}: ${value}`)
        }

        // Handle authentication
        const apiKey = req.headers.get("x-api-key")
        if (apiKey === "invalid-api-key") {
          return new Response(JSON.stringify({ error: "Invalid API key" }), {
            status: 401,
            headers: { "Content-Type": "application/json" },
          })
        }

        // Handle forced failures for retry testing
        if (failureCount > 0) {
          failureCount--
          return new Response(JSON.stringify({ error: "Internal Server Error" }), {
            status: 500,
            headers: { "Content-Type": "application/json" },
          })
        }

        // Clone the request before consuming its body
        const clonedReq = req.clone()
        const body = await req.json()

        // Parse the URL to get event details
        const url = new URL(req.url)
        const pathParts = url.pathname.split("/")
        const eventId = crypto.randomUUID()

        console.log("Path parts:", pathParts)

        // Store the request data for test verification
        storedEvents.set(eventId, {
          url: req.url,
          method: req.method,
          headers: Object.fromEntries(req.headers.entries()),
          body,
          requestCount,
        })

        // Return a simple success response with eventId
        const responseData = {
          success: true,
          eventId,
        }

        return new Response(JSON.stringify(responseData), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      }, {
        port,
        signal,
        onListen: () => {
          startResolve()
        },
      })

      // Wait for the server to start
      await startPromise
    },
    async stop() {
      if (controller && serverPromise) {
        controller.abort()
        await serverPromise
        controller = undefined
        serverPromise = undefined
      }
    },
  }
}
