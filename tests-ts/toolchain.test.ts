import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const compiler = join(root, "scripts/typescript-native.mjs");

function node(args: string[]) {
  const result = spawnSync("node", args, { cwd: root, encoding: "utf8" });
  // Bun 1.2 also sets error on a normal nonzero child exit.
  if (result.error && result.status === null) throw result.error;
  return { status: result.status, output: `${result.stdout}${result.stderr}` };
}

describe("TypeScript compiler and OpenAPI toolchain", () => {
  test("generates the checked-in client using the compatible compiler API", () => {
    const state = mkdtempSync(join(tmpdir(), "braincore-openapi-"));
    try {
      const schema = join(state, "schema.ts");
      const result = node([join(root, "node_modules/openapi-typescript/bin/cli.js"), "openapi.json", "-o", schema]);
      expect(result.output).not.toContain("TypeError");
      expect(result.status).toBe(0);
      expect(readFileSync(schema, "utf8")).toBe(readFileSync(join(root, "packages/typescript/src/schema.ts"), "utf8"));
    } finally {
      rmSync(state, { recursive: true, force: true });
    }
  });

  test("native compiler emits usable client declarations and rejects type errors", () => {
    const state = mkdtempSync(join(tmpdir(), "braincore-typescript-"));
    try {
      const version = node([compiler, "--version"]);
      expect(version.status).toBe(0);
      expect(version.output.trim()).toMatch(/^Version 7\./);
      writeFileSync(join(state, "schema.ts"), readFileSync(join(root, "packages/typescript/src/schema.ts")));
      const consumer = join(state, "consumer.ts");
      const source = 'import type { paths } from "./schema.js";\nexport type Health = paths["/v1/health"]["get"];\nexport const version: string = "2.0.0";\n';
      writeFileSync(consumer, source);
      const config = join(state, "tsconfig.json");
      writeFileSync(config, JSON.stringify({
        compilerOptions: {
          target: "ES2022", module: "NodeNext", strict: true, types: [],
          declaration: true, emitDeclarationOnly: true, outDir: "declarations",
        },
        files: ["consumer.ts", "schema.ts"],
      }));
      const success = node([compiler, "-p", config]);
      expect(success.output).toBe("");
      expect(success.status).toBe(0);
      expect(readFileSync(join(state, "declarations/consumer.d.ts"), "utf8")).toContain("export type Health");
      writeFileSync(consumer, `${source}\nconst invalid: string = 42;\n`);
      const failure = node([compiler, "-p", config]);
      expect(failure.status).not.toBe(0);
      expect(failure.output).toContain("TS2322");
    } finally {
      rmSync(state, { recursive: true, force: true });
    }
  });
});
