import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";

type Element = { type: string; props: Record<string, unknown> & { children: unknown[] } };
type Handler = (...args: unknown[]) => unknown;
type FetchHandler = (path: string, options: RequestInit) => Response | Promise<Response>;

// Execute the actual App and its rendered handlers with deterministic hook storage.
// No DOM package, server, browser profile, credentials, or global module mocks are used.
const source = readFileSync(new URL("../dashboard/src/main.tsx", import.meta.url), "utf8");
const component = new Bun.Transpiler({ loader: "tsx", tsconfig: { compilerOptions: { jsx: "react" } } })
  .transformSync(source.slice(0, source.lastIndexOf("createRoot(")))
  .replace(/^import\s*.*;\n/gm, "");

function harness(fetchHandler: FetchHandler) {
  const hooks: unknown[] = [];
  let cursor = 0;
  const react = { createElement: (type: string, props: Record<string, unknown> | null, ...children: unknown[]) => ({ type, props: { ...props, children } }) };
  const app = new Function("React", "useEffect", "useRef", "useState", "fetch", "crypto", `${component}\nreturn App;`)(
    react,
    () => {},
    (value: unknown) => { const index = cursor++; return hooks[index] ??= { current: value }; },
    (value: unknown) => {
      const index = cursor++;
      if (!(index in hooks)) hooks[index] = value;
      return [hooks[index], (next: unknown) => { hooks[index] = next; }];
    },
    (path: string, options: RequestInit) => fetchHandler(path, options),
    crypto,
  ) as () => Element;
  let tree: Element;
  const render = () => { cursor = 0; tree = app(); return tree; };
  const nodes = (value: unknown = tree): Element[] => {
    if (Array.isArray(value)) return value.flatMap((item) => nodes(item ?? null));
    if (!value || typeof value !== "object" || !("props" in value)) return [];
    const node = value as Element;
    return [node, ...nodes(node.props.children ?? [])];
  };
  const text = (value: unknown = tree): string => {
    if (Array.isArray(value)) return value.map((item) => text(item ?? null)).join("");
    if (!value || typeof value !== "object") return String(value ?? "");
    return "props" in value ? text((value as Element).props.children ?? []) : "";
  };
  const button = (label: string) => {
    const node = nodes().find((item) => item.type === "button" && text(item) === label);
    if (!node) throw new Error(`Missing button: ${label}`);
    return node;
  };
  const input = (label: string) => {
    const node = nodes().find((item) => item.type === "label" && text(item) === label);
    const field = node && nodes(node).find((item) => ["input", "textarea", "select"].includes(item.type));
    if (!field) throw new Error(`Missing input: ${label}`);
    return field;
  };
  const change = (label: string, value: string) => {
    (input(label).props.onChange as Handler)({ target: { value } });
    render();
  };
  const click = async (label: string) => {
    const node = button(label);
    if (node.props.disabled) throw new Error(`Disabled button: ${label}`);
    await (node.props.onClick as Handler)();
    render();
  };
  const submit = (buttonLabel: string) => {
    const form = nodes().find((item) => item.type === "form" && nodes(item ?? null).includes(button(buttonLabel)));
    if (!form) throw new Error(`Missing form: ${buttonLabel}`);
    return (form.props.onSubmit as Handler)({ preventDefault() {} });
  };
  const login = async (tenant: string) => { change("API key", tenant); await submit("Start secure session"); render(); };
  render();
  return { render, nodes, text, button, input, change, click, submit, login };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

function fixture(overrides?: FetchHandler) {
  let tenant = "";
  return (path: string, options: RequestInit) => {
    if (path === "/v1/dashboard/session") {
      if (options.method === "DELETE") return new Response(null, { status: 204 });
      tenant = new Headers(options.headers).get("Authorization")!.slice(7);
      return Response.json({ csrf_token: `${tenant}-csrf` });
    }
    if (overrides) return overrides(path, options);
    if (path === "/v1/search") return Response.json({ results: [{ id: "memory-id", content: `${tenant}-private-memory` }], streams: {} });
    if (path.endsWith("/explain")) return Response.json({ evidence: `${tenant}-private-evidence` });
    if (path === "/v1/admin/api-keys") return options.method === "POST"
      ? Response.json({ api_key: `${tenant}-issued-secret` })
      : Response.json({ api_keys: [{ id: "key-id", name: `${tenant}-private-key-inventory` }] });
    if (path.startsWith("/v1/operations/")) return Response.json({ result: `${tenant}-private-operation` });
    return Response.json({ status: `${tenant}-private-health` });
  };
}

async function populate(app: ReturnType<typeof harness>) {
  app.change("Scope value", "tenant-A-private-scope");
  app.change("Search query (blank lists the scope)", "tenant-A-private-query");
  await app.submit("Search memories"); app.render();
  const memory = app.nodes().find((node) => node.props.className === "memory")!;
  await (memory.props.onClick as Handler)(); app.render();
  app.change("Corrected content", "tenant-A-private-correction");
  app.change("Reason", "tenant-A-private-reason");
  app.change("Feedback note", "tenant-A-private-feedback");
  app.change("New key name", "tenant-A-private-key-name");
  await app.click("Create scope-bound key");
  app.change("Operation ID", "tenant-A-private-operation-id");
  await app.click("Inspect");
  await app.click("Check health");
  app.change("Evidence-backed content", "tenant-A-private-draft");
}

function expectNoTenantA(app: ReturnType<typeof harness>) {
  expect(app.text()).not.toContain("tenant-A");
  for (const node of app.nodes()) expect(String(node.props.value ?? "")).not.toContain("tenant-A");
}

describe("dashboard session privacy (actual App)", () => {
  test("clears issued credentials, private panels, and form drafts across tenant sessions", async () => {
    const app = harness(fixture());
    await app.login("tenant-A"); await populate(app);
    expect(app.text()).toContain("tenant-A-issued-secret");
    expect(app.text()).toContain("tenant-A-private-operation");
    expect(app.text()).toContain("tenant-A-private-evidence");
    await app.click("Sign out"); expectNoTenantA(app);
    await app.login("tenant-B"); expectNoTenantA(app);
    expect(app.input("Scope value").props.value).toBe("");
    expect(app.input("Evidence-backed content").props.value).toBe("");
  });

  for (const failure of ["network", "expired", "csrf"] as const) {
    test(`failed ${failure} sign-out clears locally and discloses uncertain server revocation`, async () => {
      const base = fixture();
      const app = harness((path, options) => {
        if (path === "/v1/dashboard/session" && options.method === "DELETE") {
          if (failure === "network") throw new Error("offline");
          return Response.json({ error: { code: failure === "expired" ? "authentication_required" : "csrf_failed" } }, { status: failure === "expired" ? 401 : 403 });
        }
        return base(path, options);
      });
      await app.login("tenant-A"); await populate(app);
      await app.click("Sign out"); expectNoTenantA(app);
      expect(app.text()).toContain("could not be confirmed");
      await app.login("tenant-B"); expectNoTenantA(app);
    });
  }

  for (const failure of [false, true]) {
    test(`ignores old-session ${failure ? "errors" : "successes"} after a new login`, async () => {
      const pending: ReturnType<typeof deferred<Response>>[] = [];
      let hold = false;
      const base = fixture();
      const app = harness((path, options) => {
        if (hold && path !== "/v1/dashboard/session") { const item = deferred<Response>(); pending.push(item); return item.promise; }
        return base(path, options);
      });
      await app.login("tenant-A"); await populate(app);
      app.change("New key name", "tenant-A-private-pending-name"); hold = true;
      const promises = [
        app.submit("Search memories"), app.click("Create scope-bound key"), app.click("Inspect"),
        app.click("Check health"), app.click("Create revision"), app.click("Record feedback"),
        app.click("Retry failed items"), app.click("Suppress"), app.click("Refresh inventory"),
        app.click("Rotate"), app.click("Revoke"), app.click("Remember directly"), app.click("Extract memories"),
      ];
      const memory = app.nodes().find((node) => node.props.className === "memory")!;
      promises.push((memory.props.onClick as Handler)());
      await app.click("Sign out"); hold = false; await app.login("tenant-B");
      for (const item of pending) failure ? item.reject(new Error("tenant-A-private-error")) : item.resolve(Response.json({ results: [{ content: "tenant-A-private-result" }], api_key: "tenant-A-issued-secret", api_keys: [{ name: "tenant-A-key" }], operation_id: "tenant-A-operation", evidence: "tenant-A-evidence", streams: "tenant-A-streams" }));
      await Promise.all(promises); app.render(); expectNoTenantA(app);
      expect(app.text()).toContain("Secure session active");
    });
  }

  for (const authError of ["authentication_required", "csrf_failed"]) {
    test(`authentication loss (${authError}) clears private data and fences pending responses`, async () => {
      let loseAuth = false;
      const base = fixture();
      const pending = deferred<Response>();
      const app = harness((path, options) => {
        if (loseAuth && path === "/v1/search") return pending.promise;
        if (loseAuth && path === "/v1/admin/api-keys") return Response.json({ error: { code: authError } }, { status: authError === "csrf_failed" ? 403 : 401 });
        return base(path, options);
      });
      await app.login("tenant-A"); await populate(app); loseAuth = true;
      const search = app.submit("Search memories");
      await app.click("Refresh inventory"); expectNoTenantA(app);
      expect(app.text()).toContain("authorization was lost");
      pending.resolve(Response.json({ results: [{ content: "tenant-A-private-late" }], streams: "tenant-A" }));
      await search; app.render(); expectNoTenantA(app);
    });
  }



  test("a non-JSON401 response still clears the authenticated browser state", async () => {
    let loseAuth = false;
    const base = fixture();
    const app = harness((path, options) => loseAuth && path === "/v1/admin/api-keys"
      ? new Response("Authentication required", { status: 401 }) : base(path, options));
    await app.login("tenant-A"); await populate(app); loseAuth = true;
    await app.click("Refresh inventory"); expectNoTenantA(app);
    expect(app.text()).toContain("authorization was lost");
  });


  test("sign-out aborts pending session requests before accepting another login", async () => {
    const signals: AbortSignal[] = [];
    const responses: ReturnType<typeof deferred<Response>>[] = [];
    let hold = false;
    const base = fixture();
    const app = harness((path, options) => {
      if (hold && path !== "/v1/dashboard/session") {
        signals.push(options.signal!);
        const response = deferred<Response>(); responses.push(response); return response.promise;
      }
      return base(path, options);
    });
    await app.login("tenant-A"); app.change("Scope value", "tenant-A-private-scope"); hold = true;
    const search = app.submit("Search memories"); const health = app.click("Check health");
    expect(signals).toHaveLength(3);
    await app.click("Sign out"); expect(signals.every((signal) => signal.aborted)).toBe(true);
    hold = false; await app.login("tenant-B");
    for (const response of responses) response.resolve(Response.json({ results: [{ content: "tenant-A-late" }] }));
    await Promise.all([search, health]); app.render(); expectNoTenantA(app);
  });

  test("permission denial preserves the current session without retaining a failed request's data", async () => {
    let forbidden = false;
    const base = fixture();
    const app = harness((path, options) => forbidden && path === "/v1/admin/api-keys"
      ? Response.json({ error: { code: "role_forbidden", message: "Permission denied" } }, { status: 403 })
      : base(path, options));
    await app.login("tenant-A"); forbidden = true;
    await app.click("Refresh inventory");
    expect(app.text()).toContain("Permission denied");
    expect(app.button("Sign out")).toBeDefined();
  });

  test("old rendered handlers cannot initiate requests in a new session", async () => {
    let calls = 0;
    const base = fixture();
    const app = harness((path, options) => { calls++; return base(path, options); });
    await app.login("tenant-A");
    const oldHealth = app.button("Check health").props.onClick as Handler;
    const oldLogout = app.button("Sign out").props.onClick as Handler;
    await app.click("Sign out"); await app.login("tenant-B");
    const before = calls;
    await oldHealth(); await oldLogout(); app.render();
    expect(calls).toBe(before);
    expect(app.text()).toContain("Secure session active");
  });

  test("serializes cookie-changing login and logout requests even before React rerenders", async () => {
    const login = deferred<Response>(); const logout = deferred<Response>();
    const calls: string[] = [];
    const base = fixture(); let block = false;
    const app = harness((path, options) => {
      if (path === "/v1/dashboard/session") {
        calls.push(options.method!);
        if (!block && options.method === "POST") return login.promise;
        if (block && options.method === "DELETE") return logout.promise;
      }
      return base(path, options);
    });
    app.change("API key", "tenant-A");
    const first = app.submit("Start secure session");
    const duplicate = app.submit("Start secure session");
    expect(calls).toEqual(["POST"]);
    login.resolve(Response.json({ csrf_token: "tenant-A-csrf" }));
    await Promise.all([first, duplicate]); app.render(); block = true;
    const signingOut = app.click("Sign out"); app.render();
    expect(app.button("Please wait…").props.disabled).toBe(true);
    expect(app.input("API key").props.disabled).toBe(true);
    app.change("API key", "tenant-B");
    await app.submit("Please wait…");
    expect(calls).toEqual(["POST", "DELETE"]);
    logout.resolve(new Response(null, { status: 204 })); await signingOut;
    await app.login("tenant-B");
    expect(calls).toEqual(["POST", "DELETE", "POST"]);
  });
});
