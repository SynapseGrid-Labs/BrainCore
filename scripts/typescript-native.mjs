import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

// Use TypeScript 7 explicitly: TypeScript 5 remains the compiler API required by
// openapi-typescript. Both packages expose a tsc binary, so PATH is ambiguous.
const require = createRequire(import.meta.url);
const compiler = join(dirname(require.resolve("typescript-native/package.json")), "bin", "tsc");
const result = spawnSync(process.execPath, [compiler, ...process.argv.slice(2)], { stdio: "inherit" });
if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
