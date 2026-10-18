#!/usr/bin/env node
import { spawn, type ChildProcess } from "node:child_process";
import { createReadStream } from "node:fs";
import { access, chmod, lstat, mkdir, open, readFile, rename, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { fileURLToPath } from "node:url";

import { Command, Option } from "commander";

import { BrainCoreClient, type Scope } from "./index.js";

type LocalConfig = Record<string, string>;

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const defaultStateDir = resolve(process.cwd(), ".braincore");

function randomSecret(prefix: string): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return `${prefix}${Buffer.from(bytes).toString("base64url")}`;
}

function parseEnv(content: string): LocalConfig {
  const config: LocalConfig = {};
  // Keep the source file intact when saving. This parser is only for local CLI
  // reads and the presence of defaults, including exported and multiline values.
  const assignments = /(?:^|\n)[ \t]*(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)[ \t]*=[ \t]*(?:"((?:\\[\s\S]|[^"\\])*)"|'((?:\\[\s\S]|[^'\\])*)'|([^\r\n]*))/g;
  for (const match of content.matchAll(assignments)) {
    const [, key, doubleQuoted, singleQuoted, unquoted] = match;
    if (singleQuoted !== undefined) {
      config[key] = singleQuoted.replace(/\\'/g, "'");
    } else {
      const value = doubleQuoted !== undefined
        ? doubleQuoted.replace(/\\([\\"$nrt])/g, (_escape, char: string) => ({ "\\": "\\", '"': '"', $: "\0", n: "\n", r: "\r", t: "\t" })[char]!)
        : unquoted.replace(/[ \t]+#.*$/, "").trim();
      config[key] = interpolateEnv(value, config).replace(/\0/g, "$");
    }
  }
  return config;
}

function interpolateEnv(value: string, config: LocalConfig): string {
  let result = "";
  for (let index = 0; index < value.length;) {
    if (value[index] !== "$") {
      result += value[index++];
      continue;
    }
    if (value[index + 1] === "$") {
      result += "$";
      index += 2;
      continue;
    }
    const braced = value[index + 1] === "{";
    let end = index + (braced ? 2 : 1);
    const name = /^[A-Za-z_][A-Za-z0-9_]*/.exec(value.slice(end))?.[0];
    if (!name) {
      result += value[index++];
      continue;
    }
    end += name.length;
    const resolved = process.env[name] ?? config[name];
    if (!braced) {
      result += resolved ?? "";
      index = end;
      continue;
    }
    const operator = /^(?::?[-+?])/.exec(value.slice(end))?.[0];
    const argumentStart = end + (operator?.length ?? 0);
    end = argumentStart;
    let depth = 1;
    while (end < value.length && depth) {
      if (value[end] === "{" && value[end - 1] === "$") depth++;
      if (value[end] === "}") depth--;
      if (depth) end++;
    }
    if (depth || (!operator && value[argumentStart] !== "}")) throw new Error(`Invalid configuration interpolation for ${name}`);
    const present = resolved !== undefined && (!operator?.startsWith(":") || resolved !== "");
    if (operator?.endsWith("?") && !present) throw new Error(`Required configuration variable ${name} is missing`);
    const argument = value.slice(argumentStart, end);
    result += operator?.endsWith("+")
      ? (present ? interpolateEnv(argument, config) : "")
      : (present ? resolved! : interpolateEnv(argument, config));
    index = end + 1;
  }
  return result;
}

function envLiteral(value: string): string {
  if (value.includes("\0")) throw new Error("Configuration values cannot contain NUL");
  if (/^[A-Za-z0-9_./:@%+=,-]*$/.test(value)) return value;
  return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\$/g, "\\$").replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/\t/g, "\\t")}"`;
}

async function loadEnv(stateDir: string): Promise<string> {
  try {
    return await readFile(join(stateDir, ".env"), "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return "";
    throw error;
  }
}

async function loadConfig(stateDir = defaultStateDir): Promise<LocalConfig> {
  return parseEnv(await loadEnv(stateDir));
}

function clientFrom(config: LocalConfig): BrainCoreClient {
  const apiKey = process.env.BRAINCORE_API_KEY ?? process.env.BRAINCORE_BOOTSTRAP_ADMIN_KEY ?? config.BRAINCORE_BOOTSTRAP_ADMIN_KEY;
  const baseUrl = process.env.BRAINCORE_URL ?? config.BRAINCORE_URL ?? "http://127.0.0.1:8787";
  if (!apiKey) throw new Error("BRAINCORE_API_KEY is required; run `braincore up` first");
  return new BrainCoreClient({ baseUrl, apiKey });
}

function scopeFrom(options: Record<string, string | undefined>): Scope {
  const scope: Scope = {
    project_id: options.project,
    user_id: options.user,
    agent_id: options.agent,
    run_id: options.run,
  };
  if (!Object.values(scope).some(Boolean)) {
    throw new Error("one of --project, --user, --agent, or --run is required");
  }
  return scope;
}

async function spawnChecked(command: string[], cwd: string): Promise<void> {
  await childCompletion(spawn(command[0], command.slice(1), { cwd, stdio: "inherit" }), command[0]);
}

function childCompletion(child: ChildProcess, label: string): Promise<void> {
  return new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`${label} exited with ${signal ? `signal ${signal}` : `status ${code}`}`));
    });
  });
}

async function transferChild(child: ChildProcess, label: string, transfer: Promise<void>): Promise<void> {
  const completed = childCompletion(child, label);
  const handlers = new Map<NodeJS.Signals, () => void>();
  const interrupted = new Promise<never>((_resolve, reject) => {
    for (const signal of ["SIGINT", "SIGTERM"] as const) {
      const handler = () => reject(new Error(`${label} interrupted by ${signal}`));
      handlers.set(signal, handler);
      process.once(signal, handler);
    }
  });
  try {
    await Promise.race([Promise.all([completed, transfer]), interrupted]);
  } catch (error) {
    child.kill();
    await completed.catch(() => {});
    throw error;
  } finally {
    for (const [signal, handler] of handlers) process.removeListener(signal, handler);
  }
}

async function composeBase(stateDir: string): Promise<{ command: string[]; cwd: string }> {
  const envPath = join(resolve(stateDir), ".env");
  await access(envPath);
  const command = ["docker", "compose", "--env-file", envPath, "-f", join(packageRoot, "compose.yaml")];
  try {
    await access(join(packageRoot, "Dockerfile"));
    command.push("-f", join(packageRoot, "compose.dev.yaml"));
  } catch {
    // Published packages use the release Compose file only.
  }
  return { command, cwd: packageRoot };
}

async function up(options: { stateDir: string }): Promise<void> {
  const stateDir = resolve(options.stateDir);
  const envPath = join(stateDir, ".env");
  await mkdir(stateDir, { recursive: true, mode: 0o700 });
  const existingContent = await loadEnv(stateDir);
  const existing = parseEnv(existingContent);
  const created = existing.BRAINCORE_BOOTSTRAP_ADMIN_KEY === undefined && process.env.BRAINCORE_BOOTSTRAP_ADMIN_KEY === undefined;
  const defaults: LocalConfig = {
    BRAINCORE_DATABASE_PASSWORD:
      randomSecret("bcdb_"),
    BRAINCORE_BOOTSTRAP_ADMIN_KEY:
      randomSecret("bck_"),
    BRAINCORE_BIND_HOST: "127.0.0.1",
    BRAINCORE_PORT: "8787",
    BRAINCORE_URL: "http://127.0.0.1:8787",
    BRAINCORE_ALLOWED_ORIGINS: "http://127.0.0.1:8787",
    BRAINCORE_COOKIE_SECURE: "false",
    BRAINCORE_MAX_BODY_BYTES: "8388608",
    BRAINCORE_LLM_BASE_URL: "",
    BRAINCORE_LLM_API_KEY: "",
    BRAINCORE_LLM_MODEL: "",
  };
  const missing = Object.entries(defaults)
    .filter(([key]) => existing[key] === undefined)
    .map(([key, value]) => `${key}=${envLiteral(process.env[key] ?? value)}\n`)
    .join("");
  const content = existingContent + (existingContent && !existingContent.endsWith("\n") ? "\n" : "") + missing;
  const config = parseEnv(content);
  try {
    // writeFile's mode only applies to a new file, including when the existing
    // file is empty. Protect existing configuration before adding credentials.
    await chmod(envPath, 0o600);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  await writeFile(envPath, content, { encoding: "utf8", mode: 0o600 });
  await chmod(envPath, 0o600);
  const composePath = join(packageRoot, "compose.yaml");
  const command = ["docker", "compose", "--env-file", envPath, "-f", composePath];
  let sourceBuild = false;
  try {
    await access(join(packageRoot, "Dockerfile"));
    command.push("-f", join(packageRoot, "compose.dev.yaml"));
    sourceBuild = true;
  } catch {
    // Published npm packages use the pinned release image and do not ship source.
  }
  command.push("up", "-d", "--wait");
  if (sourceBuild) command.push("--build");
  await spawnChecked(
    command,
    packageRoot,
  );
  console.log(`BrainCore is ready at ${process.env.BRAINCORE_URL ?? config.BRAINCORE_URL}`);
  if (created) {
    console.log("Bootstrap API key (shown once; protect it):");
    console.log(config.BRAINCORE_BOOTSTRAP_ADMIN_KEY);
  }
}

async function doctor(options: { stateDir: string }): Promise<void> {
  await spawnChecked(["docker", "version"], packageRoot);
  const config = await loadConfig(resolve(options.stateDir));
  const response = await fetch(`${process.env.BRAINCORE_URL ?? config.BRAINCORE_URL ?? "http://127.0.0.1:8787"}/v1/readiness`, {
    headers: { Authorization: `Bearer ${process.env.BRAINCORE_BOOTSTRAP_ADMIN_KEY ?? config.BRAINCORE_BOOTSTRAP_ADMIN_KEY}` },
    signal: AbortSignal.timeout(5_000),
  });
  if (!response.ok) throw new Error(`readiness returned HTTP ${response.status}`);
  console.log("BrainCore configuration, container runtime, authentication, and database are healthy.");
}

async function backup(outputPath: string, options: { stateDir: string }): Promise<void> {
  const target = resolve(outputPath);
  await mkdir(dirname(target), { recursive: true });
  const { command, cwd } = await composeBase(options.stateDir);
  try {
    if (!(await lstat(target)).isFile()) throw new Error("backup destination must be a regular file");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const temporary = join(dirname(target), `.${basename(target)}.${crypto.randomUUID()}.tmp`);
  const file = await open(temporary, "wx", 0o600);
  try {
    const args = [...command.slice(1), "exec", "-T", "postgres", "pg_dump", "-U", "braincore", "-d", "braincore", "--format=custom", "--no-owner", "--no-privileges"];
    const child = spawn(command[0], args, { cwd, stdio: ["ignore", "pipe", "inherit"] });
    await transferChild(child, "pg_dump", file.writeFile(child.stdout));
    await file.sync();
    await file.close();
    await rename(temporary, target);
  } finally {
    await file.close();
    await rm(temporary, { force: true });
  }
  console.log(`Encrypted-at-rest storage is your responsibility; protected backup written to ${target}`);
}

async function restore(backupPath: string, options: { stateDir: string; confirm: boolean }): Promise<void> {
  if (!options.confirm) throw new Error("restore requires --confirm because it replaces the current database");
  const source = resolve(backupPath);
  await access(source);
  const { command, cwd } = await composeBase(options.stateDir);
  await spawnChecked([...command, "stop", "app", "worker"], cwd);
  try {
    const args = [...command.slice(1), "exec", "-T", "postgres", "pg_restore", "-U", "braincore", "-d", "braincore", "--clean", "--if-exists", "--single-transaction", "--exit-on-error", "--no-owner", "--no-privileges"];
    const child = spawn(command[0], args, { cwd, stdio: ["pipe", "inherit", "inherit"] });
    await transferChild(child, "pg_restore", pipeline(createReadStream(source), child.stdin));
  } catch (error) {
    const detail = error instanceof Error ? error.message : "unknown restore error";
    throw new Error(`Database restore failed; application services remain stopped. Inspect the error before restarting: ${detail}`);
  }
  await spawnChecked([...command, "up", "-d", "--wait", "worker", "app"], cwd);
  console.log("BrainCore database restore completed and application services are ready.");
}

function output(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

const program = new Command()
  .name("braincore")
  .description("Free, self-hosted, evidence-native memory")
  .version("2.0.0");

program
  .command("up")
  .description("Create protected local configuration and start the complete stack")
  .option("--state-dir <path>", "configuration directory", defaultStateDir)
  .action(up);

program.command("doctor").option("--state-dir <path>", "configuration directory", defaultStateDir).action(doctor);

program
  .command("backup")
  .argument("<output-path>")
  .option("--state-dir <path>", "configuration directory", defaultStateDir)
  .action(backup);

program
  .command("restore-backup")
  .argument("<backup-path>")
  .option("--state-dir <path>", "configuration directory", defaultStateDir)
  .option("--confirm", "confirm replacement of the current database", false)
  .action(restore);

function addScopeOptions(command: Command): Command {
  return command
    .addOption(new Option("--project <id>"))
    .addOption(new Option("--user <id>"))
    .addOption(new Option("--agent <id>"))
    .addOption(new Option("--run <id>"));
}

addScopeOptions(program.command("remember").argument("<content>"))
  .description("Store a direct evidence-backed memory")
  .action(async (content: string, options: Record<string, string>) => {
    output(await clientFrom(await loadConfig()).remember(content, scopeFrom(options)));
  });

addScopeOptions(program.command("ingest").argument("<content>"))
  .description("Ingest text for asynchronous extraction")
  .action(async (content: string, options: Record<string, string>) => {
    output(await clientFrom(await loadConfig()).ingest(content, scopeFrom(options)));
  });

addScopeOptions(program.command("search").argument("<query>"))
  .description("Search memories")
  .option("--limit <number>", "maximum results", "10")
  .action(async (query: string, options: Record<string, string>) => {
    output(
      await clientFrom(await loadConfig()).search(query, scopeFrom(options), {
        limit: Number(options.limit),
      }),
    );
  });

program
  .command("get")
  .argument("<memory-id>")
  .action(async (memoryId: string) => output(await clientFrom(await loadConfig()).get(memoryId)));

program
  .command("explain")
  .argument("<memory-id>")
  .action(async (memoryId: string) =>
    output(await clientFrom(await loadConfig()).explain(memoryId)),
  );

for (const action of ["suppress", "retire", "restore"] as const) {
  program
    .command(action)
    .argument("<memory-id>")
    .requiredOption("--reason <text>")
    .action(async (memoryId: string, options: { reason: string }) =>
      output(await clientFrom(await loadConfig())[action](memoryId, options.reason)),
    );
}

program.parseAsync().catch((error) => {
  console.error(error instanceof Error ? error.message : "BrainCore command failed");
  process.exitCode = 1;
});
