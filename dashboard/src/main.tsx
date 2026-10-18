import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";

import "./styles.css";

type Json = Record<string, unknown>;
type ScopeField = "project_id" | "user_id" | "agent_id" | "run_id";

function record(value: unknown): Json {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Json : {};
}

function records(value: unknown): Json[] {
  return Array.isArray(value) ? value.filter((item): item is Json => typeof item === "object" && item !== null && !Array.isArray(item)) : [];
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}

class ApiError extends Error {
  constructor(message: string, readonly status: number, readonly code: string) { super(message); }
}

class StaleSessionResponse extends Error {}

type SessionRequest = (path: string, options?: RequestInit, csrf?: string) => Promise<Json>;

async function api(path: string, options: RequestInit = {}, csrf?: string): Promise<Json> {
  const response = await fetch(path, {
    ...options,
    credentials: "same-origin",
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(csrf ? { "X-CSRF-Token": csrf } : {}),
      ...options.headers,
    },
  });
  let body: Json = {};
  try { if (response.status !== 204) body = record(await response.json()); } catch {
    // Authentication loss must clear the session even if an intermediary returns HTML.
    if (!response.ok) throw new ApiError(`HTTP ${response.status}`, response.status, "");
    throw new Error("API returned invalid JSON");
  }
  if (!response.ok) {
    const error = record(body.error);
    throw new ApiError(stringValue(error.message) || `HTTP ${response.status}`, response.status, stringValue(error.code));
  }
  return body;
}

function App(): React.JSX.Element {
  const sessionGeneration = useRef(0);
  const authTransition = useRef(false);
  const pendingRequests = useRef(new Set<AbortController>());
  const [authBusy, setAuthBusy] = useState(false);
  const [csrf, setCsrf] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [scopeField, setScopeField] = useState<ScopeField>("user_id");
  const [scopeValue, setScopeValue] = useState("");
  const [query, setQuery] = useState("");
  const [content, setContent] = useState("");
  const [results, setResults] = useState<Json[]>([]);
  const [selected, setSelected] = useState<Json | null>(null);
  const [explanation, setExplanation] = useState<Json | null>(null);
  const [revisionContent, setRevisionContent] = useState("");
  const [actionReason, setActionReason] = useState("");
  const [feedbackRating, setFeedbackRating] = useState<-1 | 0 | 1>(1);
  const [feedbackComment, setFeedbackComment] = useState("");
  const [operationId, setOperationId] = useState("");
  const [operation, setOperation] = useState<Json | null>(null);
  const [keys, setKeys] = useState<Json[]>([]);
  const [keyName, setKeyName] = useState("");
  const [issuedKey, setIssuedKey] = useState("");
  const [health, setHealth] = useState<Json | null>(null);
  const [status, setStatus] = useState("Enter an API key to start a same-origin session.");

  const scope = { [scopeField]: scopeValue };
  const generation = sessionGeneration.current;

  // Invalidate pending reads and writes before clearing any tenant-derived state.
  const clearSession = () => {
    sessionGeneration.current += 1;
    for (const controller of pendingRequests.current) controller.abort();
    pendingRequests.current.clear();
    setCsrf("");
    setApiKey("");
    setScopeField("user_id");
    setScopeValue("");
    setQuery("");
    setContent("");
    setResults([]);
    setSelected(null);
    setExplanation(null);
    setRevisionContent("");
    setActionReason("");
    setFeedbackRating(1);
    setFeedbackComment("");
    setOperationId("");
    setOperation(null);
    setKeys([]);
    setKeyName("");
    setIssuedKey("");
    setHealth(null);
  };

  const run = async (work: (request: SessionRequest, update: (apply: () => void) => void) => Promise<void>) => {
    // Old rendered handlers cannot start work during a session transition.
    if (!csrf || authTransition.current || generation !== sessionGeneration.current) return;
    const request: SessionRequest = async (path, options = {}, token) => {
      if (generation !== sessionGeneration.current) throw new StaleSessionResponse();
      const controller = new AbortController();
      pendingRequests.current.add(controller);
      try {
        const body = await api(path, { ...options, signal: controller.signal }, token);
        if (generation !== sessionGeneration.current) throw new StaleSessionResponse();
        return body;
      } finally { pendingRequests.current.delete(controller); }
    };
    const update = (apply: () => void) => {
      if (generation === sessionGeneration.current && !authTransition.current) apply();
    };
    try { await work(request, update); } catch (error) {
      if (generation !== sessionGeneration.current || error instanceof StaleSessionResponse) return;
      if (error instanceof ApiError && (error.status === 401 || error.code === "csrf_failed")) {
        clearSession();
        setStatus("Session expired or authorization was lost. Local session data was cleared. Sign in again.");
      } else setStatus(error instanceof Error ? error.message : "Request failed");
    }
  };

  const login = async (event: React.FormEvent) => {
    event.preventDefault();
    // Serialize cookie-changing requests: aborting fetch cannot undo Set-Cookie.
    if (authTransition.current || generation !== sessionGeneration.current) return;
    authTransition.current = true;
    setAuthBusy(true);
    const credential = apiKey;
    clearSession();
    setStatus("Starting secure session…");
    try {
      const session = await api("/v1/dashboard/session", {
        method: "POST",
        headers: { Authorization: `Bearer ${credential}`, "Idempotency-Key": crypto.randomUUID() },
      });
      const token = stringValue(session.csrf_token);
      if (!token) throw new Error("Session response did not include a CSRF token.");
      setCsrf(token);
      setStatus("Secure session active. The API key was cleared from browser state.");
    } catch (error) {
      clearSession();
      setStatus(error instanceof Error ? error.message : "Sign in failed");
    } finally {
      authTransition.current = false;
      setAuthBusy(false);
    }
  };

  const logout = async () => {
    if (authTransition.current || generation !== sessionGeneration.current) return;
    authTransition.current = true;
    setAuthBusy(true);
    const token = csrf;
    clearSession();
    setStatus("Local session data cleared. Closing server session…");
    try {
      await api("/v1/dashboard/session", { method: "DELETE", headers: { "Idempotency-Key": crypto.randomUUID() } }, token);
      setStatus("Session closed. Local session data was cleared.");
    } catch {
      setStatus("Local session data was cleared. Server sign-out could not be confirmed; its session may remain active until it expires.");
    } finally {
      authTransition.current = false;
      setAuthBusy(false);
    }
  };

  const refreshHealth = async () => run(async (request, update) => {
    const [readiness, system] = await Promise.all([request("/v1/readiness"), request("/v1/admin/status")]);
    update(() => setHealth({ readiness, system }));
  });

  useEffect(() => {
    if (csrf) void refreshHealth();
  }, [csrf]);

  const search = async (event?: React.FormEvent) => {
    event?.preventDefault();
    await run(async (request, update) => {
      const body = await request("/v1/search", {
        method: "POST",
        body: JSON.stringify({ query: query || null, scope, limit: 25 }),
      });
      update(() => {
        setResults(records(body.results));
        setStatus(`Search streams: ${JSON.stringify(body.streams)}`);
      });
    });
  };

  const remember = async (mode: "remember" | "ingest") => run(async (request, update) => {
    const body = await request(mode === "remember" ? "/v1/memories" : "/v1/ingestions", {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
      body: JSON.stringify({ content, scope }),
    }, csrf);
    update(() => {
      setOperationId(stringValue(body.operation_id));
      setContent("");
      setStatus(`${mode === "remember" ? "Memory" : "Ingestion"} accepted.`);
    });
  });

  const inspect = async (memory: Json) => run(async (request, update) => {
    update(() => {
      setSelected(memory);
      setRevisionContent(stringValue(memory.content));
      setExplanation(null);
    });
    const explanation = await request(`/v1/memories/${encodeURIComponent(stringValue(memory.id))}/explain`);
    update(() => setExplanation(explanation));
  });

  const revise = async () => {
    if (!selected) return;
    await run(async (request, update) => {
      const memoryId = stringValue(selected.id);
      const accepted = await request(`/v1/memories/${encodeURIComponent(memoryId)}/revisions`, {
        method: "POST",
        headers: { "Idempotency-Key": crypto.randomUUID() },
        body: JSON.stringify({ content: revisionContent, reason: actionReason }),
      }, csrf);
      update(() => {
        setOperationId(stringValue(accepted.operation_id));
        setActionReason("");
        setStatus("Revision accepted; history was preserved and embedding was queued.");
      });
    });
  };

  const submitFeedback = async () => {
    if (!selected) return;
    await run(async (request, update) => {
      const memoryId = stringValue(selected.id);
      const accepted = await request(`/v1/memories/${encodeURIComponent(memoryId)}/feedback`, {
        method: "POST",
        headers: { "Idempotency-Key": crypto.randomUUID() },
        body: JSON.stringify({ rating: feedbackRating, comment: feedbackComment || null }),
      }, csrf);
      update(() => {
        setOperationId(stringValue(accepted.operation_id));
        setFeedbackComment("");
        setStatus("Feedback recorded with an audit-linked operation.");
      });
    });
  };

  const lifecycle = async (action: string) => {
    if (!selected) return;
    await run(async (request, update) => {
      const memoryId = stringValue(selected.id);
      await request(`/v1/memories/${encodeURIComponent(memoryId)}/lifecycle`, {
        method: "PATCH",
        headers: { "Idempotency-Key": crypto.randomUUID() },
        body: JSON.stringify({ action, reason: actionReason || `Dashboard ${action}` }),
      }, csrf);
      const refreshed = await request(`/v1/memories/${encodeURIComponent(memoryId)}`);
      update(() => {
        setSelected(refreshed);
        setActionReason("");
        setStatus(`Memory ${action} completed with an audit event.`);
      });
    });
  };

  const inspectOperation = async () => run(async (request, update) => {
    const operation = await request(`/v1/operations/${encodeURIComponent(operationId)}`);
    update(() => setOperation(operation));
  });

  const retryOperation = async () => run(async (request, update) => {
    await request(`/v1/operations/${encodeURIComponent(operationId)}/retry`, {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
    }, csrf);
    const operation = await request(`/v1/operations/${encodeURIComponent(operationId)}`);
    update(() => {
      setOperation(operation);
      setStatus("Failed operation items were queued for an audited retry.");
    });
  });

  const loadKeys = async () => run(async (request, update) => {
    const body = await request("/v1/admin/api-keys");
    update(() => setKeys(records(body.api_keys)));
  });

  const createKey = async () => run(async (request, update) => {
    const body = await request("/v1/admin/api-keys", {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
      body: JSON.stringify({ name: keyName, roles: ["read", "write"], scope }),
    }, csrf);
    update(() => {
      setIssuedKey(stringValue(body.api_key));
      setKeyName("");
    });
    const inventory = await request("/v1/admin/api-keys");
    update(() => setKeys(records(inventory.api_keys)));
  });

  const keyAction = async (keyId: string, action: "revoke" | "rotate") => run(async (request, update) => {
    const body = await request(`/v1/admin/api-keys/${encodeURIComponent(keyId)}/${action}`, { method: "POST", headers: { "Idempotency-Key": crypto.randomUUID() } }, csrf);
    update(() => { if (action === "rotate") setIssuedKey(stringValue(body.api_key)); });
    const inventory = await request("/v1/admin/api-keys");
    update(() => setKeys(records(inventory.api_keys)));
  });

  return (
    <main>
      <header>
        <div><p className="eyebrow">Evidence-native memory</p><h1>BrainCore</h1><p>{status}</p></div>
        {csrf && <div className="header-actions"><button onClick={refreshHealth}>Check health</button><button onClick={logout}>Sign out</button></div>}
      </header>

      {!csrf ? (
        <section className="panel login"><h2>Connect locally</h2><form onSubmit={login}>
          <label>API key<input type="password" disabled={authBusy} value={apiKey} onChange={(event) => setApiKey(event.target.value)} autoComplete="off" required /></label>
          <button disabled={authBusy} type="submit">{authBusy ? "Please wait…" : "Start secure session"}</button>
        </form></section>
      ) : (
        <div className="grid">
          <section className="panel span-two"><h2>Remember and retrieve</h2>
            <div className="scope-row"><label>Scope type<select value={scopeField} onChange={(event) => setScopeField(event.target.value as ScopeField)}><option value="user_id">User</option><option value="project_id">Project</option><option value="agent_id">Agent</option><option value="run_id">Run</option></select></label><label>Scope value<input value={scopeValue} onChange={(event) => setScopeValue(event.target.value)} required /></label></div>
            <label>Evidence-backed content<textarea value={content} onChange={(event) => setContent(event.target.value)} /></label>
            <div className="actions"><button disabled={!content || !scopeValue} onClick={() => remember("remember")}>Remember directly</button><button disabled={!content || !scopeValue} onClick={() => remember("ingest")}>Extract memories</button></div>
            <form onSubmit={search}><label>Search query (blank lists the scope)<input value={query} onChange={(event) => setQuery(event.target.value)} /></label><button disabled={!scopeValue} type="submit">Search memories</button></form>
            <div className="results">{results.map((memory) => <button className="memory" key={stringValue(memory.id)} onClick={() => inspect(memory)}><strong>{stringValue(memory.content)}</strong><span>{stringValue(memory.trust)} · {stringValue(memory.state)} · {stringValue(memory.embedding_state)}</span></button>)}</div>
          </section>

          <section className="panel"><h2>Evidence and lifecycle</h2>{selected ? <><h3>{stringValue(selected.content)}</h3><p>{stringValue(selected.trust)} · {stringValue(selected.state)} · valid from {stringValue(selected.valid_from)}</p><label>Corrected content<textarea value={revisionContent} onChange={(event) => setRevisionContent(event.target.value)} /></label><label>Reason<input value={actionReason} onChange={(event) => setActionReason(event.target.value)} /></label><div className="actions"><button disabled={!revisionContent || !actionReason} onClick={revise}>Create revision</button><button onClick={() => lifecycle("suppress")}>Suppress</button><button onClick={() => lifecycle("retire")}>Retire</button><button onClick={() => lifecycle("restore")}>Restore</button><button onClick={() => lifecycle("promote")}>Promote trust</button></div><label>Feedback rating<select value={feedbackRating} onChange={(event) => setFeedbackRating(Number(event.target.value) as -1 | 0 | 1)}><option value={1}>Helpful</option><option value={0}>Neutral</option><option value={-1}>Unhelpful</option></select></label><label>Feedback note<input value={feedbackComment} onChange={(event) => setFeedbackComment(event.target.value)} /></label><button onClick={submitFeedback}>Record feedback</button><pre>{JSON.stringify(explanation, null, 2)}</pre></> : <p>Select a memory to inspect provenance, revisions, and lifecycle history.</p>}</section>

          <section className="panel"><h2>Operations and batch outcomes</h2><label>Operation ID<input value={operationId} onChange={(event) => setOperationId(event.target.value)} /></label><div className="actions"><button disabled={!operationId} onClick={inspectOperation}>Inspect</button><button disabled={!operationId} onClick={retryOperation}>Retry failed items</button></div><pre>{JSON.stringify(operation, null, 2)}</pre></section>

          <section className="panel"><h2>API key administration</h2><label>New key name<input value={keyName} onChange={(event) => setKeyName(event.target.value)} /></label><p>Read/write keys are restricted to the scope selected above.</p><div className="actions"><button disabled={!keyName || !scopeValue} onClick={createKey}>Create scope-bound key</button><button onClick={loadKeys}>Refresh inventory</button></div>{issuedKey && <div className="issued"><strong>Copy now—shown once</strong><code>{issuedKey}</code><button onClick={() => setIssuedKey("")}>I saved it</button></div>}<div className="key-list">{keys.map((key) => <div key={stringValue(key.id)}><span>{stringValue(key.name)} · {stringValue(key.prefix)} · {JSON.stringify(key.scope)}</span><div className="actions"><button onClick={() => keyAction(stringValue(key.id), "rotate")}>Rotate</button><button onClick={() => keyAction(stringValue(key.id), "revoke")}>Revoke</button></div></div>)}</div></section>

          <section className="panel"><h2>System health</h2><pre>{JSON.stringify(health, null, 2)}</pre><p>No product telemetry is sent. This console talks only to the same-origin local API.</p></section>
        </div>
      )}
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
