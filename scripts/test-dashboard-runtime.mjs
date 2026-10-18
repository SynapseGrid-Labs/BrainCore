// Exercise the shipped browser bundle: a build alone misses React renderer mismatches.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, extname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const exec = promisify(execFile);
const distribution = resolve(dirname(fileURLToPath(import.meta.url)), "../dashboard/dist");
const dashboardRequire = createRequire(new URL("../dashboard/package.json", import.meta.url));
const profile = await mkdtemp(join(tmpdir(), "braincore-dashboard-smoke-"));
const contentTypes = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css" };
const errors = [];
const server = createServer(async (request, response) => {
  try {
    const pathname = new URL(request.url, "http://localhost").pathname;
    const path = resolve(distribution, `.${pathname === "/" ? "/index.html" : pathname}`);
    if (!path.startsWith(distribution + sep)) {
      response.writeHead(403).end();
      return;
    }
    let content = await readFile(path);
    if (extname(path) === ".html") {
      // Capture exceptions before the actual module executes; fixtures never replace App.
      content = Buffer.from(content.toString().replace("<head>", `<head><script>
        for (const event of ["error", "unhandledrejection"]) {
          window.addEventListener(event, () => {
            document.documentElement.setAttribute("data-dashboard-smoke-error", "true");
          });
        }
      </script>`));
    }
    response.writeHead(200, { "Content-Type": contentTypes[extname(path)] ?? "application/octet-stream" });
    response.end(content);
  } catch (error) {
    if (request.url !== "/favicon.ico") errors.push(error.message);
    response.writeHead(404).end();
  }
});

try {
  // Check the actual build output, which is copied intact into the runtime image.
  const notices = await readFile(join(distribution, "THIRD_PARTY_LICENSES.md"), "utf8");
  const reactDomManifest = dashboardRequire.resolve("react-dom/package.json");
  const reactDomRequire = createRequire(reactDomManifest);
  for (const manifest of [
    dashboardRequire.resolve("react/package.json"),
    reactDomManifest,
    reactDomRequire.resolve("scheduler/package.json"),
  ]) {
    const { name, version } = JSON.parse(await readFile(manifest, "utf8"));
    const license = (await readFile(join(dirname(manifest), "LICENSE"), "utf8")).trim();
    const section = notices.split(`## ${name} - ${version} (MIT)\n`)[1]?.split("\n## ")[0];
    assert(section?.includes(license), `The bundle must preserve ${name}'s complete installed license`);
  }
  await readFile(join(distribution, "index.html"));
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const browser = process.env.CHROME_BIN || "google-chrome";
  const { stdout } = await exec(browser, [
    "--headless=new", "--disable-gpu", "--disable-dev-shm-usage", "--no-first-run",
    "--disable-background-networking", "--disable-extensions", "--disable-sync",
    "--disable-component-update", "--no-proxy-server", `--user-data-dir=${profile}`,
    "--virtual-time-budget=3000", "--dump-dom",
    `http://127.0.0.1:${server.address().port}/`,
  ], { timeout: 45000, maxBuffer: 1024 * 1024 });
  assert.deepEqual(errors, [], "The browser must load every dashboard asset");
  assert(!stdout.includes('data-dashboard-smoke-error="true"'), "Dashboard raised an uncaught browser error");
  assert.match(stdout, /<h1>BrainCore<\/h1>/, "The shipped App must mount through react-dom/client");
  assert.match(stdout, /<h2>Connect locally<\/h2>/, "The dashboard must render its login screen");
  assert.match(stdout, /<input[^>]*type="password"/, "The dashboard must render its credential control");
  assert.match(stdout, />Start secure session<\/button>/, "The dashboard must render its session action");
  console.log("Dashboard browser smoke passed: shipped bundle mounted, login controls rendered, no uncaught errors.");
} finally {
  await new Promise(resolve => server.close(resolve));
  await rm(profile, { recursive: true, force: true });
}
