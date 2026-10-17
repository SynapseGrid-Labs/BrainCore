import React, { useEffect, useState } from "react";
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
  const body = response.status === 204 ? {} : record(await response.json());
  if (!response.ok) {
    const error = record(body.error);
    throw new Error(stringValue(error.message) || `HTTP ${response.status}`);
  }
  return body;
}

function App(): React.JSX.Element {
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

  const run = async (work: () => Promise<void>) => {
    try { await work(); } catch (error) { setStatus(error instanceof Error ? error.message : "Request failed"); }
  };

  const login = async (event: React.FormEvent) => {
    event.preventDefault();
    await run(async () => {
      const session = await api("/v1/dashboard/session", {
        method: "POST",
        headers: { Authorization: `Bearer ${apiKey}`, "Idempotency-Key": crypto.randomUUID() },
      });
      setCsrf(stringValue(session.csrf_token));
      setApiKey("");
      setStatus("Secure session active. The API key was cleared from browser state.");
    });
  };

  const logout = async () => run(async () => {
    await api("/v1/dashboard/session", { method: "DELETE", headers: { "Idempotency-Key": crypto.randomUUID() } }, csrf);
    setCsrf("");
    setResults([]);
    setSelected(null);
    setStatus("Session closed.");
  });

  const refreshHealth = async () => run(async () => {
    const [readiness, system] = await Promise.all([api("/v1/readiness"), api("/v1/admin/status")]);
    setHealth({ readiness, system });
  });

  useEffect(() => {
    if (csrf) void refreshHealth();
  }, [csrf]);

  const search = async (event?: React.FormEvent) => {
    event?.preventDefault();
    await run(async () => {
      const body = await api("/v1/search", {
        method: "POST",
        body: JSON.stringify({ query: query || null, scope, limit: 25 }),
      });
      setResults(records(body.results));
      setStatus(`Search streams: ${JSON.stringify(body.streams)}`);
    });
  };

  const remember = async (mode: "remember" | "ingest") => run(async () => {
    const body = await api(mode === "remember" ? "/v1/memories" : "/v1/ingestions", {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
      body: JSON.stringify({ content, scope }),
    }, csrf);
    setOperationId(stringValue(body.operation_id));
    setContent("");
    setStatus(`${mode === "remember" ? "Memory" : "Ingestion"} accepted.`);
  });

  const inspect = async (memory: Json) => run(async () => {
    setSelected(memory);
    setRevisionContent(stringValue(memory.content));
    setExplanation(await api(`/v1/memories/${encodeURIComponent(stringValue(memory.id))}/explain`));
  });

  const revise = async () => {
    if (!selected) return;
    await run(async () => {
      const memoryId = stringValue(selected.id);
      const accepted = await api(`/v1/memories/${encodeURIComponent(memoryId)}/revisions`, {
        method: "POST",
        headers: { "Idempotency-Key": crypto.randomUUID() },
        body: JSON.stringify({ content: revisionContent, reason: actionReason }),
      }, csrf);
      setOperationId(stringValue(accepted.operation_id));
      setActionReason("");
      setStatus("Revision accepted; history was preserved and embedding was queued.");
    });
  };

  const submitFeedback = async () => {
    if (!selected) return;
    await run(async () => {
      const memoryId = stringValue(selected.id);
      const accepted = await api(`/v1/memories/${encodeURIComponent(memoryId)}/feedback`, {
        method: "POST",
        headers: { "Idempotency-Key": crypto.randomUUID() },
        body: JSON.stringify({ rating: feedbackRating, comment: feedbackComment || null }),
      }, csrf);
      setOperationId(stringValue(accepted.operation_id));
      setFeedbackComment("");
      setStatus("Feedback recorded with an audit-linked operation.");
    });
  };

  const lifecycle = async (action: string) => {
    if (!selected) return;
    await run(async () => {
      const memoryId = stringValue(selected.id);
      await api(`/v1/memories/${encodeURIComponent(memoryId)}/lifecycle`, {
        method: "PATCH",
        headers: { "Idempotency-Key": crypto.randomUUID() },
        body: JSON.stringify({ action, reason: actionReason || `Dashboard ${action}` }),
      }, csrf);
      const refreshed = await api(`/v1/memories/${encodeURIComponent(memoryId)}`);
      setSelected(refreshed);
      setActionReason("");
      setStatus(`Memory ${action} completed with an audit event.`);
    });
  };

  const inspectOperation = async () => run(async () => {
    setOperation(await api(`/v1/operations/${encodeURIComponent(operationId)}`));
  });

  const retryOperation = async () => run(async () => {
    await api(`/v1/operations/${encodeURIComponent(operationId)}/retry`, {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
    }, csrf);
    await inspectOperation();
    setStatus("Failed operation items were queued for an audited retry.");
  });

  const loadKeys = async () => run(async () => {
    const body = await api("/v1/admin/api-keys");
    setKeys(records(body.api_keys));
  });

  const createKey = async () => run(async () => {
    const body = await api("/v1/admin/api-keys", {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
      body: JSON.stringify({ name: keyName, roles: ["read", "write"], scope }),
    }, csrf);
    setIssuedKey(stringValue(body.api_key));
    setKeyName("");
    await loadKeys();
  });

  const keyAction = async (keyId: string, action: "revoke" | "rotate") => run(async () => {
    const body = await api(`/v1/admin/api-keys/${encodeURIComponent(keyId)}/${action}`, { method: "POST", headers: { "Idempotency-Key": crypto.randomUUID() } }, csrf);
    if (action === "rotate") setIssuedKey(stringValue(body.api_key));
    await loadKeys();
  });

  return (
    <main>
      <header>
        <div><p className="eyebrow">Evidence-native memory</p><h1>BrainCore</h1><p>{status}</p></div>
        {csrf && <div className="header-actions"><button onClick={refreshHealth}>Check health</button><button onClick={logout}>Sign out</button></div>}
      </header>

      {!csrf ? (
        <section className="panel login"><h2>Connect locally</h2><form onSubmit={login}>
          <label>API key<input type="password" value={apiKey} onChange={(event) => setApiKey(event.target.value)} autoComplete="off" required /></label>
          <button type="submit">Start secure session</button>
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
