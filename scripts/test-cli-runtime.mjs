// Harmless executable smoke tests: every container command goes to a fake Docker.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { chmod, mkdtemp, mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { promisify } from "node:util";

const exec = promisify(execFile);
const cli = resolve(process.argv[2] ?? "dist/cli.js");
const runtime = process.argv[3] ?? process.execPath;
const root = await mkdtemp(join(tmpdir(), "braincore-cli-runtime-"));
const bin = join(root, "bin");
const state = join(root, "state");
const dump = join(root, "backup.dump");
const restored = join(root, "restored.dump");
const commandLog = join(root, "commands.jsonl");
const payload = Buffer.from([0, 255, 10, 13, 128, ...Buffer.from("synthetic dump")]);
await mkdir(bin);
await mkdir(state);
const docker = join(bin, "docker");
await writeFile(docker, `#!/usr/bin/env node
const fs = require("node:fs");
const args = process.argv.slice(2);
fs.appendFileSync(process.env.FAKE_COMMAND_LOG, JSON.stringify(args) + "\\n");
if (process.env.FAKE_DOCKER_FAIL) process.exit(Number(process.env.FAKE_DOCKER_FAIL));
if (args.includes("pg_restore") && process.env.FAKE_RESTORE_FAIL) process.exit(99);
if (args.includes("pg_dump")) process.stdout.write(Buffer.from(process.env.FAKE_DUMP, "base64"));
if (args.includes("pg_restore")) {
  const chunks = [];
  process.stdin.on("data", chunk => chunks.push(chunk));
  process.stdin.on("end", () => fs.writeFileSync(process.env.FAKE_RESTORED, Buffer.concat(chunks)));
}
`);
await chmod(docker, 0o700);
const env = {
  ...process.env,
  PATH: `${bin}:${process.env.PATH}`,
  FAKE_COMMAND_LOG: commandLog,
  FAKE_DUMP: payload.toString("base64"),
  FAKE_RESTORED: restored,
};
// Avoid inherited client configuration or real service authentication.
for (const key of Object.keys(env)) {
  if (/^(BRAINCORE_|PG|DATABASE_URL)/.test(key)) delete env[key];
}
const run = (args, overrides = {}) => exec(runtime, [cli, ...args], { env: { ...env, ...overrides }, cwd: root });
const server = createServer((request, response) => {
  assert.equal(request.url, "/v1/readiness");
  assert.equal(request.headers.authorization, "Bearer bck_synthetic_cli");
  response.writeHead(200).end("{}");
});
try {
  await assert.rejects(run(["up", "--state-dir", state], { FAKE_DOCKER_FAIL: "97" }), error => {
    assert.match(error.stderr, /docker exited with status 97/);
    assert.doesNotMatch(error.stderr, /Bun is not defined/);
    return true;
  });
  await run(["up", "--state-dir", state]);
  assert.equal((await stat(join(state, ".env"))).mode & 0o777, 0o600);
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  await writeFile(join(state, ".env"), `BRAINCORE_URL=http://127.0.0.1:${address.port}\nBRAINCORE_BOOTSTRAP_ADMIN_KEY=bck_synthetic_cli\n`);
  await run(["doctor", "--state-dir", state]);
  await writeFile(dump, "old backup", { mode: 0o644 });
  await run(["backup", dump, "--state-dir", state]);
  assert.deepEqual(await readFile(dump), payload);
  assert.equal((await stat(dump)).mode & 0o777, 0o600);
  await assert.rejects(run(["backup", dump, "--state-dir", state], { FAKE_DOCKER_FAIL: "98" }), /Command failed/);
  await writeFile(dump, payload);
  await run(["restore-backup", dump, "--state-dir", state, "--confirm"]);
  assert.deepEqual(await readFile(restored), payload);
  await assert.rejects(run(["restore-backup", dump, "--state-dir", state]), /restore requires --confirm/);
  // A failed restore must still restart the stopped app and worker.
  await assert.rejects(run(["restore-backup", dump, "--state-dir", state, "--confirm"], { FAKE_RESTORE_FAIL: "1" }), /Command failed/);
  const commands = (await readFile(commandLog, "utf8")).trim().split("\n").map(JSON.parse);
  assert(commands.some(args => args.includes("pg_dump")));
  assert(commands.some(args => args.includes("pg_restore")));
  const restoreIndex = commands.findIndex(args => args.includes("pg_restore"));
  assert(commands[restoreIndex - 1].includes("stop"));
  assert(commands[restoreIndex + 1].includes("up"));
  assert(commands.at(-1).includes("up"));
  assert.equal(await readFile(join(dirname(cli), "../package.json"), "utf8").then(JSON.parse).then(p => p.version), "2.0.0");
  console.log("CLI process, readiness, backup and restore runtime checks passed.");
} finally {
  await new Promise(resolve => server.close(resolve));
  await rm(root, { recursive: true, force: true });
}
