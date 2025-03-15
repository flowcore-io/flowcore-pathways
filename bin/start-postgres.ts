#!/usr/bin/env -S deno run -A

console.log("Starting PostgreSQL container for testing...");

// Start PostgreSQL container
const startProcess = new Deno.Command("docker-compose", {
  args: ["-f", "docker-compose.test.yml", "up", "-d"],
  stdout: "inherit",
  stderr: "inherit",
});

const startStatus = await startProcess.output();
if (!startStatus.success) {
  console.error("Failed to start PostgreSQL container");
  Deno.exit(1);
}

// Wait for PostgreSQL to be ready
console.log("Waiting for PostgreSQL to be ready...");
const maxRetries = 30;
for (let i = 1; i <= maxRetries; i++) {
  const healthCheckProcess = new Deno.Command("docker-compose", {
    args: ["-f", "docker-compose.test.yml", "exec", "postgres", "pg_isready", "-U", "postgres"],
    stdout: "null",
    stderr: "null",
  });

  try {
    const healthStatus = await healthCheckProcess.output();
    if (healthStatus.success) {
      console.log("PostgreSQL is ready!");
      Deno.exit(0);
    }
  } catch (_error) {
    // Ignore errors and retry
  }

  console.log(`Waiting for PostgreSQL to be ready... (${i}/${maxRetries})`);
  await new Promise(resolve => setTimeout(resolve, 1000));
}

console.error("PostgreSQL failed to start within the timeout period.");
Deno.exit(1); 