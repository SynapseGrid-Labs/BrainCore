// Harmless executable smoke tests: every container command goes to a fake Docker.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { chmod, lstat, mkdtemp, mkdir, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
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
const configLog = join(root, "configs.jsonl");
const payload = Buffer.from([0, 255, 10, 13, 128, ...Buffer.from("synthetic dump")]);
await mkdir(bin);
await mkdir(state);
const docker = join(bin, "docker");
await writeFile(docker, `#!/usr/bin/env node
const fs = require("node:fs");
const args = process.argv.slice(2);
fs.appendFileSync(process.env.FAKE_COMMAND_LOG, JSON.stringify(args) + "\\n");
if (args.includes("up")) {
  const envFile = args[args.indexOf("--env-file") + 1];
  fs.appendFileSync(process.env.FAKE_CONFIG_LOG, JSON.stringify({
    content: fs.readFileSync(envFile, "utf8"),
    environment: Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith("BRAINCORE_"))),
  }) + "\\n");
}
if (process.env.FAKE_DOCKER_FAIL) process.exit(Number(process.env.FAKE_DOCKER_FAIL));
if (args.includes("pg_restore") && process.env.FAKE_RESTORE_FAIL) process.exit(99);
if (args.includes("up") && process.env.FAKE_UP_FAIL) process.exit(96);
if (args.includes("stop") && process.env.FAKE_STOP_FAIL) process.exit(95);
if (args.includes("pg_dump") && process.env.FAKE_DUMP_FAIL) {
  process.stdout.write("partial synthetic dump");
  process.exitCode = 98;
}
if (args.includes("pg_dump") && process.env.FAKE_DUMP_WAIT) {
  process.stdout.write("incomplete synthetic dump");
  fs.writeFileSync(process.env.FAKE_CANCEL_READY, "ready");
  setInterval(() => {}, 1000);
}
if (args.includes("pg_dump") && !process.env.FAKE_DUMP_FAIL && !process.env.FAKE_DUMP_WAIT) process.stdout.write(Buffer.from(process.env.FAKE_DUMP, "base64"));
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
  FAKE_CONFIG_LOG: configLog,
  FAKE_DUMP: payload.toString("base64"),
  FAKE_RESTORED: restored,
};
// Avoid inherited client configuration or real service authentication.
for (const key of Object.keys(env)) {
  if (/^(BRAINCORE_|PG|DATABASE_URL)/.test(key)) delete env[key];
}
const run = (args, overrides = {}) => exec(runtime, [cli, ...args], { env: { ...env, ...overrides }, cwd: root });
const commands = async () => (await readFile(commandLog, "utf8")).trim().split("\n").map(JSON.parse);
const configs = async () => (await readFile(configLog, "utf8")).trim().split("\n").map(JSON.parse);
const remainingTemporaryFiles = async () => (await readdir(root)).filter(name => name.startsWith(".backup.dump."));
let expectedAuthorization = "Bearer bck_synthetic_cli";
const server = createServer((request, response) => {
  assert(["/v1/readiness", "/v1/memories/synthetic"].includes(request.url));
  assert.equal(request.headers.authorization, expectedAuthorization);
  response.writeHead(200).end("{}");
});
try {
  await writeFile(join(state, ".env"), "", { mode: 0o644 });
  assert.equal((await stat(join(state, ".env"))).mode & 0o777, 0o644);
  await assert.rejects(run(["up", "--state-dir", state], { FAKE_DOCKER_FAIL: "97" }), error => {
    assert.match(error.stderr, /docker exited with status 97/);
    assert.doesNotMatch(error.stderr, /Bun is not defined/);
    return true;
  });
  assert.equal((await stat(join(state, ".env"))).mode & 0o777, 0o600, "an existing empty configuration must be protected when defaults are added");
  const generated = await readFile(join(state, ".env"), "utf8");
  assert.match(generated, /^BRAINCORE_DATABASE_PASSWORD=bcdb_[A-Za-z0-9_-]+$/m);
  assert.match(generated, /^BRAINCORE_BOOTSTRAP_ADMIN_KEY=bck_[A-Za-z0-9_-]+$/m);
  assert.match(generated, /^BRAINCORE_BIND_HOST=127\.0\.0\.1$/m);
  assert.match(generated, /^BRAINCORE_PORT=8787$/m);
  assert.match(generated, /^BRAINCORE_COOKIE_SECURE=false$/m);
  await run(["up", "--state-dir", state]);
  assert.equal(await readFile(join(state, ".env"), "utf8"), generated, "repeated up must retain generated credentials and defaults");
  assert.equal((await stat(join(state, ".env"))).mode & 0o777, 0o600);

  // Keep the original dotenv bytes: Compose, rather than a second parser in
  // this harness, owns quoting, interpolation, comments, and shell precedence.
  const saved = [
    "# Retain custom Compose configuration, including its dotenv syntax.",
    "BRAINCORE_DATABASE_PASSWORD=bcdb_synthetic_saved_password",
    "BRAINCORE_BOOTSTRAP_ADMIN_KEY=bck_synthetic_saved_bootstrap_key",
    "BRAINCORE_URL=http://127.0.0.1:9898",
    "BRAINCORE_IMAGE=example.invalid/braincore:custom",
    "BRAINCORE_EMBEDDER_URL=http://embedding.invalid:9090",
    "BRAINCORE_EMBEDDING_MODEL=custom-model",
    "BRAINCORE_EMBEDDING_PREPROCESSING_REVISION=custom-revision",
    "BRAINCORE_EMBEDDING_TIMEOUT_SECONDS=12",
    "BRAINCORE_EMBEDDING_MAX_CONCURRENCY=3",
    "BRAINCORE_EMBEDDING_CACHE_MAX_ENTRIES=17",
    "BRAINCORE_EMBEDDING_CACHE_TTL_SECONDS=21",
    "BRAINCORE_WORKER_BATCH_SIZE=4",
    "BRAINCORE_EMBEDDING_BATCH_SIZE=9",
    "BRAINCORE_SEARCH_FULL_TEXT_MODE=canonical",
    "BRAINCORE_SEARCH_VECTOR_MODE=exact",
    "BRAINCORE_SEARCH_HNSW_EF_SEARCH=321",
    "BRAINCORE_SEARCH_HNSW_MAX_SCAN_TUPLES=4567",
    "BRAINCORE_SEARCH_HNSW_SCAN_MEM_MULTIPLIER=2",
    "BRAINCORE_SEARCH_DIAGNOSTICS=true",
    'BRAINCORE_LLM_BASE_URL="http://${CUSTOM_LLM_HOST:-llm.invalid}:8081" # expand in Compose',
    "BRAINCORE_LLM_API_KEY='synthetic$literal # key'",
    "BRAINCORE_LLM_MODEL=",
    'CUSTOM_QUOTED_VALUE="keep spaces # and comments"',
    "CUSTOM_MULTILINE_VALUE='first line\nsecond line'",
    "CUSTOM_EMPTY_VALUE=",
  ].join("\r\n");
  await writeFile(join(state, ".env"), saved);
  await chmod(join(state, ".env"), 0o644);
  const retained = await run(["up", "--state-dir", state]);
  const completed = await readFile(join(state, ".env"), "utf8");
  assert(completed.startsWith(`${saved}\n`), "up must preserve all existing dotenv bytes and append missing defaults");
  assert.match(completed, /^BRAINCORE_BIND_HOST=127\.0\.0\.1$/m);
  assert.match(completed, /^BRAINCORE_PORT=8787$/m);
  assert.match(completed, /^BRAINCORE_ALLOWED_ORIGINS=http:\/\/127\.0\.0\.1:8787$/m);
  assert.match(completed, /^BRAINCORE_COOKIE_SECURE=false$/m);
  assert.doesNotMatch(retained.stdout, /Bootstrap API key|bck_synthetic|synthetic\$literal/);
  assert.equal((await stat(join(state, ".env"))).mode & 0o777, 0o600);
  assert.deepEqual((await configs()).at(-1), { content: completed, environment: {} });

  const overrides = {
    BRAINCORE_IMAGE: "example.invalid/braincore:explicit",
    BRAINCORE_EMBEDDER_URL: "http://override.invalid:8181",
    BRAINCORE_EMBEDDING_MAX_CONCURRENCY: "6",
    BRAINCORE_SEARCH_VECTOR_MODE: "iterative_hnsw",
    BRAINCORE_PORT: "9191",
    BRAINCORE_URL: "http://127.0.0.1:9191",
    BRAINCORE_BOOTSTRAP_ADMIN_KEY: "bck_synthetic_explicit_bootstrap_key",
  };
  const overridden = await run(["up", "--state-dir", state], overrides);
  assert.equal(await readFile(join(state, ".env"), "utf8"), completed, "shell overrides must not replace saved configuration");
  assert.deepEqual((await configs()).at(-1), { content: completed, environment: overrides }, "Compose must receive both saved configuration and explicit shell overrides");
  assert.match(overridden.stdout, /BrainCore is ready at http:\/\/127\.0\.0\.1:9191/);
  assert.doesNotMatch(overridden.stdout, /Bootstrap API key|bck_synthetic/);
  await assert.rejects(run(["up", "--state-dir", state], { FAKE_UP_FAIL: "1" }), error => {
    assert.doesNotMatch(error.stdout, /BrainCore is ready|Bootstrap API key|bck_synthetic/);
    return true;
  });
  assert.equal(await readFile(join(state, ".env"), "utf8"), completed, "failed up must retain saved configuration and generated defaults");
  assert.equal((await stat(join(state, ".env"))).mode & 0o777, 0o600);
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  await writeFile(join(state, ".env"), `BRAINCORE_URL=http://127.0.0.1:${address.port}\nBRAINCORE_BOOTSTRAP_ADMIN_KEY=bck_synthetic_cli\n`);
  await run(["doctor", "--state-dir", state]);

  // Missing defaults inherit explicit first-run values. Their literal encoding
  // must remain usable by both Compose and local CLI authentication on restart.
  const firstRunState = join(root, ".braincore");
  const firstRunEnvironment = {
    BRAINCORE_URL: `http://127.0.0.1:${address.port}`,
    BRAINCORE_DATABASE_PASSWORD: 'synthetic \\ password $value # "quoted" and \'single\'',
    BRAINCORE_BOOTSTRAP_ADMIN_KEY: 'bck_synthetic_\\$literal$$ # "double" and \'single\'',
    BRAINCORE_LLM_API_KEY: 'synthetic key\nBRAINCORE_IMAGE=must-stay-inside-value\r\t"\\$value',
  };
  const firstRun = await run(["up", "--state-dir", firstRunState], firstRunEnvironment);
  assert.doesNotMatch(firstRun.stdout, /Bootstrap API key|bck_synthetic|synthetic key/);
  const firstRunContent = await readFile(join(firstRunState, ".env"), "utf8");
  assert.equal((await stat(join(firstRunState, ".env"))).mode & 0o777, 0o600);
  assert.doesNotMatch(firstRunContent, /^BRAINCORE_IMAGE=/m, "newlines in explicit values must not inject configuration assignments");
  await run(["up", "--state-dir", firstRunState]);
  assert.equal(await readFile(join(firstRunState, ".env"), "utf8"), firstRunContent);
  expectedAuthorization = `Bearer ${firstRunEnvironment.BRAINCORE_BOOTSTRAP_ADMIN_KEY}`;
  await run(["doctor", "--state-dir", firstRunState]);
  await run(["get", "synthetic"]);
  expectedAuthorization = "Bearer bck_synthetic_explicit_doctor";
  await run(["doctor", "--state-dir", firstRunState], {
    BRAINCORE_BOOTSTRAP_ADMIN_KEY: "bck_synthetic_explicit_doctor",
  });
  await run(["get", "synthetic"], { BRAINCORE_API_KEY: "bck_synthetic_explicit_doctor" });

  const savedCredentials = [
    ["export BRAINCORE_BOOTSTRAP_ADMIN_KEY='bck_synthetic_$literal # single'", "bck_synthetic_$literal # single"],
    ['BRAINCORE_BOOTSTRAP_ADMIN_KEY="bck_synthetic_\\$literal # \\"double\\" \\\\slash"', 'bck_synthetic_$literal # "double" \\slash'],
    ["SYNTHETIC_KEY='bck_synthetic_reference'\nBRAINCORE_BOOTSTRAP_ADMIN_KEY=${SYNTHETIC_KEY}", "bck_synthetic_reference"],
    ["BRAINCORE_BOOTSTRAP_ADMIN_KEY=${SYNTHETIC_MISSING:-bck_synthetic_fallback}", "bck_synthetic_fallback"],
    ["BRAINCORE_BOOTSTRAP_ADMIN_KEY=old\nBRAINCORE_BOOTSTRAP_ADMIN_KEY=bck_synthetic_last # comment", "bck_synthetic_last"],
  ];
  for (const [credential, expected] of savedCredentials) {
    const source = `BRAINCORE_URL='http://127.0.0.1:${address.port}'\r\n${credential}\r\n`;
    await writeFile(join(state, ".env"), source);
    const result = await run(["up", "--state-dir", state]);
    assert((await readFile(join(state, ".env"), "utf8")).startsWith(source));
    assert.doesNotMatch(result.stdout, /Bootstrap API key|bck_synthetic/);
    expectedAuthorization = `Bearer ${expected}`;
    await run(["doctor", "--state-dir", state]);
  }
  await writeFile(dump, "old backup", { mode: 0o644 });
  await run(["backup", dump, "--state-dir", state]);
  assert.deepEqual(await readFile(dump), payload);
  assert.equal((await stat(dump)).mode & 0o777, 0o600);
  await assert.rejects(run(["backup", dump, "--state-dir", state], { FAKE_DUMP_FAIL: "1" }), error => {
    assert.match(error.stderr, /pg_dump exited with status 98/);
    return true;
  });
  assert.deepEqual(await readFile(dump), payload, "a failed dump must preserve the previous backup");
  assert.deepEqual(await remainingTemporaryFiles(), [], "failed backup temporary files must be removed");
  const absentDump = join(root, "absent.dump");
  await assert.rejects(run(["backup", absentDump, "--state-dir", state], { FAKE_DUMP_FAIL: "1" }), /Command failed/);
  await assert.rejects(lstat(absentDump), { code: "ENOENT" });
  const linkDump = join(root, "link.dump");
  await symlink(dump, linkDump);
  await assert.rejects(run(["backup", linkDump, "--state-dir", state]), /backup destination must be a regular file/);
  assert.deepEqual(await readFile(dump), payload);
  const cancelReady = join(root, "cancel-ready");
  const cancelled = run(["backup", dump, "--state-dir", state], { FAKE_DUMP_WAIT: "1", FAKE_CANCEL_READY: cancelReady });
  const rejectedCancellation = assert.rejects(cancelled, error => {
    assert.match(error.stderr, /pg_dump interrupted by SIGTERM/);
    return true;
  });
  try {
    const deadline = Date.now() + 5000;
    while (!(await lstat(cancelReady).then(() => true, () => false))) {
      if (Date.now() > deadline) throw new Error("fake dump did not start before cancellation deadline");
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  } finally {
    cancelled.child.kill("SIGTERM");
  }
  await rejectedCancellation;
  assert.deepEqual(await readFile(dump), payload, "cancellation must preserve the previous backup");
  assert.deepEqual(await remainingTemporaryFiles(), []);

  let before = (await commands()).length;
  await run(["restore-backup", dump, "--state-dir", state, "--confirm"]);
  assert.deepEqual(await readFile(restored), payload);
  let issued = (await commands()).slice(before);
  assert.equal(issued.length, 3);
  assert(issued[0].includes("stop"));
  assert(issued[1].includes("pg_restore"));
  assert(issued[1].includes("--single-transaction"), "restore must roll back all changes on any error");
  assert(issued[1].includes("--exit-on-error"));
  assert(issued[2].includes("up"));

  before = (await commands()).length;
  await assert.rejects(run(["restore-backup", dump, "--state-dir", state]), /restore requires --confirm/);
  assert.equal((await commands()).length, before, "unconfirmed restores must not stop services");
  await assert.rejects(run(["restore-backup", join(root, "missing.dump"), "--state-dir", state, "--confirm"]), /ENOENT/);
  assert.equal((await commands()).length, before, "missing archives must not stop services");

  await assert.rejects(run(["restore-backup", dump, "--state-dir", state, "--confirm"], { FAKE_RESTORE_FAIL: "1" }), error => {
    assert.match(error.stderr, /services remain stopped/);
    return true;
  });
  issued = (await commands()).slice(before);
  assert.equal(issued.length, 2, "a failed restore must not restart either application service");
  assert(issued[0].includes("stop"));
  assert(issued[1].includes("pg_restore"));

  before = (await commands()).length;
  await assert.rejects(run(["restore-backup", root, "--state-dir", state, "--confirm"]), /services remain stopped/);
  issued = (await commands()).slice(before);
  assert(issued.every(args => !args.includes("up")), "archive read failures must not restart services");

  before = (await commands()).length;
  await assert.rejects(run(["restore-backup", dump, "--state-dir", state, "--confirm"], { FAKE_STOP_FAIL: "1" }), /docker exited with status 95/);
  issued = (await commands()).slice(before);
  assert.equal(issued.length, 1, "restore must not run if services cannot be stopped");
  assert(issued[0].includes("stop"));
  await assert.rejects(run(["restore-backup", dump, "--state-dir", state, "--confirm"], { FAKE_UP_FAIL: "1" }), error => {
    assert.match(error.stderr, /docker exited with status 96/);
    assert.doesNotMatch(error.stdout, /database restore completed/);
    return true;
  });
  assert.equal(await readFile(join(dirname(cli), "../package.json"), "utf8").then(JSON.parse).then(p => p.version), "2.0.0");
  console.log("CLI process, configuration preservation, readiness, backup and restore runtime checks passed.");
} finally {
  await new Promise(resolve => server.close(resolve));
  await rm(root, { recursive: true, force: true });
}
