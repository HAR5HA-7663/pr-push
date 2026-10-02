# pr-push

GitHub PR events → pub/sub for a local PR babysitter, on the Cloudflare free plan.

```
GitHub repo webhooks
  pull_request_review, pull_request_review_comment, issue_comment, check_suite, pull_request
        │ HMAC-signed POST /gh
        ▼
Cloudflare Worker `pr-push`
  verifies the signature → reduces the payload to {repo, pr, event, action, state, sender}
  Hub Durable Object (SQLite, last 500 events) → hibernating WebSocket /ws?token=…&after=<id>
        │ outbound-only subscribers, catch-up from the last seen id
        ├─► machine A ─┐ listener pokes the PR's watcher and runs the sweep
        └─► machine B ─┘
```

No code or comment text passes through, and no subscriber accepts inbound connections.

## Deploy

```sh
cd worker
wrangler deploy
printf %s "$WEBHOOK_SECRET" | wrangler secret put WEBHOOK_SECRET   # GitHub webhook secret
printf %s "$SUB_TOKEN"      | wrangler secret put SUB_TOKEN        # subscriber token
```

Point each repo webhook (content type `application/json`, same secret) at `https://<worker>/gh`.
Subscribers connect to `wss://<worker>/ws?token=<SUB_TOKEN>&after=<last id>`; send `ping` to keep alive
(answered with `pong` without waking the Durable Object).
