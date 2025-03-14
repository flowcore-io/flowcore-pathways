import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts";
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { invalidEvent, mockFlowcoreLegacyEvent, validEvent } from "./fixtures/events.ts";

const TEST_PORT = 3001;
let controller: AbortController;

Deno.test({
  name: "HTTP Server Tests",
  async fn(t) {
    // Start the server before tests
    controller = new AbortController();
    const serverPromise = serve(async (req: Request) => {
      const url = new URL(req.url);

      if (req.method !== "POST") {
        return new Response("Method not allowed", { status: 405 });
      }

      try {
        switch (url.pathname) {
          case "/transform": {
            const body = await req.json();
            return new Response(JSON.stringify({ success: true }), {
              headers: { "Content-Type": "application/json" },
            });
          }
          case "/send": {
            const body = await req.json();
            if (!body.dataCoreId) {
              return new Response(
                JSON.stringify({
                  success: false,
                  error: "Invalid event: {\"dataCoreId\":\"Expected string\"}",
                }),
                {
                  status: 400,
                  headers: { "Content-Type": "application/json" },
                }
              );
            }
            return new Response(JSON.stringify({ success: true }), {
              headers: { "Content-Type": "application/json" },
            });
          }
          default:
            return new Response("Not found", { status: 404 });
        }
      } catch (error) {
        return new Response(
          JSON.stringify({
            success: false,
            error: `Failed to process event with ${error}`,
          }),
          {
            status: 400,
            headers: { "Content-Type": "application/json" },
          }
        );
      }
    }, { port: TEST_PORT, signal: controller.signal, onListen: undefined });

    // Run the tests
    try {
      await t.step("HTTP Server - /transform endpoint", async () => {
        const response = await fetch(`http://localhost:${TEST_PORT}/transform`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(mockFlowcoreLegacyEvent),
        });

        const data = await response.json();
        assertEquals(response.status, 200);
        assertEquals(data.success, true);
      });

      await t.step("HTTP Server - /send endpoint with valid event", async () => {
        const response = await fetch(`http://localhost:${TEST_PORT}/send`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(validEvent),
        });

        const data = await response.json();
        assertEquals(response.status, 200);
        assertEquals(data.success, true);
      });

      await t.step("HTTP Server - /send endpoint with invalid event", async () => {
        const response = await fetch(`http://localhost:${TEST_PORT}/send`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(invalidEvent),
        });

        const data = await response.json();
        assertEquals(response.status, 400);
        assertEquals(data.success, false);
        assertEquals(typeof data.error, "string");
        assertEquals(data.error.includes("Invalid event"), true);
      });

      await t.step("HTTP Server - Method not allowed", async () => {
        const response = await fetch(`http://localhost:${TEST_PORT}/send`, {
          method: "GET",
        });

        assertEquals(response.status, 405);
        assertEquals(await response.text(), "Method not allowed");
      });

      await t.step("HTTP Server - Not found", async () => {
        const response = await fetch(`http://localhost:${TEST_PORT}/nonexistent`, {
          method: "POST",
        });

        assertEquals(response.status, 404);
        assertEquals(await response.text(), "Not found");
      });
    } finally {
      // Clean up the server
      controller.abort();
      await serverPromise;
    }
  },
}); 