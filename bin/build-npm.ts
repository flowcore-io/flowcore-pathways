// ex. scripts/build_npm.ts
import { build, emptyDir } from "@deno/dnt"
import denoJson from "../deno.json" with { type: "json" }
await emptyDir("./npm")

await build({
  entryPoints: ["./src/mod.ts"],
  test: false,
  outDir: "./npm",
  importMap: "./deno.json",
  shims: {
    // see JS docs for overview and more options
    deno: true,
    webSocket: true,
  },
  package: {
    name: denoJson.name,
    description: denoJson.description,
    version: denoJson.version,
    license: denoJson.license,
    homepage: "https://github.com/flowcore-io/flowcore-sdk#readme",
    repository: {
      type: "git",
      url: "git+https://github.com/flowcore-io/flowcore-sdk.git",
    },
    bugs: {
      url: "https://github.com/flowcore-io/flowcore-sdk/issues",
    },
    optionalDependencies: {
      "bun-sqlite-key-value": "1.13.1",
    },
    devDependencies: {
      "@types/ws": "^8.5.10"
    }
  },
  postBuild() {
    // steps to run after building and before running the tests
    // Deno.copyFileSync("LICENSE", "npm/LICENSE")
    Deno.copyFileSync("README.md", "npm/README.md")
    
    // Only copy CHANGELOG.md if it exists
    try {
      const changelogStat = Deno.statSync("CHANGELOG.md");
      if (changelogStat.isFile) {
        Deno.copyFileSync("CHANGELOG.md", "npm/CHANGELOG.md");
      }
    } catch (error) {
      if (!(error instanceof Deno.errors.NotFound)) {
        throw error; // Rethrow if it's not a "file not found" error
      }
      console.log("CHANGELOG.md not found, skipping copy");
    }
  },
})
