#!/usr/bin/env -S deno run -A

console.log("Stopping PostgreSQL container...")

const stopProcess = new Deno.Command("docker-compose", {
  args: ["-f", "docker-compose.test.yml", "down"],
  stdout: "inherit",
  stderr: "inherit",
})

const stopStatus = await stopProcess.output()
if (!stopStatus.success) {
  console.error("Failed to stop PostgreSQL container")
  Deno.exit(1)
}

console.log("PostgreSQL container stopped successfully.")
Deno.exit(0)
