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
  FAKE_DUMP: payload.toString("base64"),
  FAKE_RESTORED: restored,
};
// Avoid inherited client configuration or real service authentication.
for (const key of Object.keys(env)) {
  if (/^(BRAINCORE_|PG|DATABASE_URL)/.test(key)) delete env[key];
}
const run = (args, overrides = {}) => exec(runtime, [cli, ...args], { env: { ...env, ...overrides }, cwd: root });
const commands = async () => (await readFile(commandLog, "utf8")).trim().split("\n").map(JSON.parse);
const remainingTemporaryFiles = async () => (await readdir(root)).filter(name => name.startsWith(".backup.dump."));
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
  console.log("CLI process, readiness, backup and restore runtime checks passed.");
} finally {
  await new Promise(resolve => server.close(resolve));
  await rm(root, { recursive: true, force: true });
}
