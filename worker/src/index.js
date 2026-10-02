// pr-push — GitHub PR events -> pub/sub for Harsha's PR babysitters.
//
// GitHub repo webhooks POST to /gh. The payload is HMAC-verified, then reduced to
// {repo, pr, event, action, state, sender}: no code, no comment text. The single
// Hub Durable Object keeps the last 500 events in SQLite and pushes each one to
// every connected subscriber over a hibernating WebSocket (/ws), so idle
// connections cost nothing. Subscribers reconnect with ?after=<last id> and get
// whatever they missed while asleep or offline.
//
// None of Harsha's machines accept inbound connections: the laptop, the mini
// (and anything else) only dial out to this Worker.
//
// Secrets (wrangler secret put): WEBHOOK_SECRET (GitHub), SUB_TOKEN (subscribers).

import { DurableObject } from "cloudflare:workers";

const KEEP = 500;

function hex(buf) {
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function safeEqual(a, b) {
  if (a.length !== b.length) return false;
  let d = 0;
  for (let i = 0; i < a.length; i++) d |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return d === 0;
}

async function validSignature(secret, body, header) {
  const key = await crypto.subtle.importKey(
    "raw", new TextEncoder().encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const mac = await crypto.subtle.sign("HMAC", key, body);
  return safeEqual("sha256=" + hex(mac), header || "");
}

function reduce(event, p) {
  const repo = p.repository?.full_name || "";
  const sender = p.sender?.login || "";
  const action = p.action || "";
  let prs = [];
  let state = "";
  switch (event) {
    case "pull_request_review":
    case "pull_request_review_comment":
      prs = [p.pull_request.number];
      state = (p.review?.state || "").toUpperCase();
      break;
    case "pull_request":
      prs = [p.pull_request.number];
      state = p.pull_request.merged ? "MERGED" : (p.pull_request.state || "").toUpperCase();
      break;
    case "issue_comment":
      if (!p.issue?.pull_request) return [];
      prs = [p.issue.number];
      break;
    case "check_suite": {
      const cs = p.check_suite || {};
      if (cs.status !== "completed") return [];
      prs = (cs.pull_requests || []).map((x) => x.number);
      state = (cs.conclusion || "").toUpperCase();
      break;
    }
    case "ping":
      prs = [0];
      break;
    default:
      return [];
  }
  return prs.map((pr) => ({ repo, pr, event, action, state, sender }));
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const hub = env.HUB.get(env.HUB.idFromName("hub"));

    if (url.pathname === "/health") return new Response("ok");

    if (url.pathname === "/gh" && req.method === "POST") {
      const body = await req.arrayBuffer();
      if (body.byteLength > 5 * 1024 * 1024) return new Response("too big", { status: 413 });
      if (!(await validSignature(env.WEBHOOK_SECRET, body, req.headers.get("X-Hub-Signature-256"))))
        return new Response("bad signature", { status: 401 });
      let recs;
      try {
        recs = reduce(req.headers.get("X-GitHub-Event") || "", JSON.parse(new TextDecoder().decode(body)));
      } catch {
        return new Response("bad payload", { status: 400 });
      }
      if (recs.length) await hub.publish(recs);
      return new Response(null, { status: 204 });
    }

    if (url.pathname === "/ws") {
      if (!safeEqual(url.searchParams.get("token") || "", env.SUB_TOKEN))
        return new Response("unauthorized", { status: 401 });
      if (req.headers.get("Upgrade") !== "websocket")
        return new Response("expected websocket", { status: 426 });
      return hub.fetch(req);
    }

    return new Response("not found", { status: 404 });
  },
};

export class Hub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, body TEXT)`);
    // Client keepalives are answered without waking the object.
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  lastId() {
    const r = this.sql.exec("SELECT COALESCE(MAX(id), 0) AS m FROM events").one();
    return r.m;
  }

  async fetch(req) {
    const after = parseInt(new URL(req.url).searchParams.get("after") || "0", 10) || 0;
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    const last = this.lastId();
    if (after > 0) {
      // Catch-up: everything since the subscriber's last seen id.
      for (const row of this.sql.exec("SELECT id, ts, body FROM events WHERE id > ? ORDER BY id", after)) {
        server.send(JSON.stringify({ id: row.id, ts: row.ts, ...JSON.parse(row.body) }));
      }
    }
    server.send(JSON.stringify({ hello: true, last }));
    return new Response(null, { status: 101, webSocket: client });
  }

  async publish(recs) {
    const ts = Math.floor(Date.now() / 1000);
    const out = [];
    for (const r of recs) {
      const row = this.sql.exec("INSERT INTO events(ts, body) VALUES(?, ?) RETURNING id", ts, JSON.stringify(r)).one();
      out.push(JSON.stringify({ id: row.id, ts, ...r }));
    }
    this.sql.exec("DELETE FROM events WHERE id <= ?", this.lastId() - KEEP);
    for (const ws of this.ctx.getWebSockets()) {
      for (const m of out) {
        try { ws.send(m); } catch {}
      }
    }
  }

  async webSocketMessage() {}
  async webSocketClose(ws, code) {
    try { ws.close(code, "bye"); } catch {}
  }
  async webSocketError() {}
}
