import { rm } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const targets = process.argv[2] === "dist" ? [join(repository, "dist")] : [
  join(repository, "dist"),
  join(repository, "dashboard", "dist"),
];

for (const target of targets) {
  if (!target.startsWith(`${repository}/`) || target === repository) {
    throw new Error(`refusing unsafe build cleanup target: ${target}`);
  }
  await rm(target, { recursive: true, force: true });
}
