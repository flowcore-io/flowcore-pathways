import type { ClusterSocket, ClusterTransport } from "./types.ts"

/**
 * Creates a cluster transport for Node.js/Bun using the `ws` package.
 * The `ws` import is deferred to `startServer()` to avoid loading it in Deno.
 */
export function createNodeTransport(): ClusterTransport {
  return {
    async startServer(port, onConnection) {
      // deno-lint-ignore no-explicit-any
      const ws = await import("ws") as any
      const WebSocketServer = ws.WebSocketServer ?? ws.default?.WebSocketServer
      const wss = new WebSocketServer({ port, host: "0.0.0.0" })

      return new Promise((resolve) => {
        wss.on("listening", () => {
          resolve({
            async shutdown() {
              await new Promise<void>((res, rej) => {
                wss.close((err?: Error) => (err ? rej(err) : res()))
              })
            },
          })
        })

        // deno-lint-ignore no-explicit-any
        wss.on("connection", (rawWs: any) => {
          const socket: ClusterSocket = {
            send: (data: string) => rawWs.send(data),
            close: () => rawWs.close(),
            get readyState() {
              return rawWs.readyState
            },
            onopen: null,
            onmessage: null,
            onclose: null,
            onerror: null,
          }

          // deno-lint-ignore no-explicit-any
          rawWs.on("message", (data: any) => {
            socket.onmessage?.({ data: data.toString() })
          })
          rawWs.on("close", (ev: unknown) => socket.onclose?.(ev))
          rawWs.on("error", (ev: unknown) => socket.onerror?.(ev))

          // Connection is already open when "connection" fires
          onConnection(socket)
        })
      })
    },

    connect(address) {
      return new WebSocket(address) as unknown as ClusterSocket
    },
  }
}
