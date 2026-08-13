---
title: "See what is arriving"
weight: 30
description: "The debug page and the query command, for the first question after pointing an app at telemetryd."
---

# See what is arriving

The first minutes after wiring an exporter, only one question matters: is anything
landing. Two ways to answer it, neither of which requires composing a query.

## In a browser

Open `/debug` — `http://127.0.0.1:4319/debug` locally, or whatever hostname sits in front
of it on a server.

Tabs for **logs, traces and metrics**, the last 100 records of whichever you are looking
at, a summary of which applications are sending, and three time windows. The query box is
for narrowing once you can see something, not for getting started, and it completes from
this instance's own labels — the applications and metric names that actually exist here.

### Signing in

An instance with tokens shows a sign-in form and takes the **admin token**, or the query
token when no admin token is configured — the same credential `/status` and `/metrics`
take. An instance with no tokens, which is the default locally, is simply open.

The token is held in a cookie that scripts cannot read, scoped to `/debug`, and marked
`Secure` whenever the request arrived over TLS — including through a reverse proxy that
sets `X-Forwarded-Proto`. It is never put in a URL, where it would survive in the proxy's
access log, in browser history and in a `Referer` header. **Sign out** clears it.

`Authorization: Bearer <token>` works too, for a script.

### Reaching it on a server

If telemetryd is on loopback behind nginx — the [recommended
shape](expose-it-safely.md) — the page is already reachable at your hostname and needs
nothing further.

Binding telemetryd itself to a public address is the other route, and it needs
`auth.ingest_token` and `auth.query_token` set as well: telemetryd refuses to start
exposed without them, and an admin token alone does not satisfy that check. It guards
`/status` and `/debug`; it does not guard the telemetry.

Failing both, an SSH tunnel needs no configuration at all:

```bash
ssh -L 4319:127.0.0.1:4319 forge@your-server
```

Then open `http://127.0.0.1:4319/debug` in your own browser.

## Over SSH

When the browser is the thing you cannot reach — or the thing that is broken — the same
data is one command:

```bash
telemetryd query
```

No argument means everything from the last hour, newest first. That is deliberately the
default, because "is anything arriving" is the question people have before they have a
query to run.

```bash
telemetryd query '{app="checkout"}'                     # one application
telemetryd query '{app="checkout", level="error"}'      # errors only
telemetryd query '{app="checkout"} |= "declined"'       # containing a substring
telemetryd query '{app=~".+"} | json | level="error"'   # parse the body, filter a field
telemetryd query --output json | jq -r .body            # for piping
```

`telemetryd query --help` lists more, with what each construct means and which ones are
refused here.

## When nothing shows

Work down this list; it is ordered by how often each is the answer.

1. **The window.** Both the page and the command default to a short one. Widen it before
   concluding anything.
2. **The exporter's endpoint.** It wants the base URL — `http://127.0.0.1:4319` — not a
   path. A trailing `/v1/logs` in the configuration produces `/v1/logs/v1/logs`.
3. **A `200` is not proof.** telemetryd rejects **per record** and reports what it refused
   in OTLP's `partialSuccess` field. Read the exporter's response body, or check
   `telemetryd_ingest_rejected_total` on `/metrics`.
4. **Retention.** The default is 7 days for logs and traces. Records with timestamps
   older than that are deleted by the reaper, which is easy to hit while replaying a
   fixture with hard-coded timestamps.
5. **Clock skew.** Timestamps in seconds or milliseconds instead of nanoseconds are
   corrected automatically and counted on
   `telemetryd_ingest_timestamps_rescaled_total`; anything further out lands somewhere you
   are not looking.

If the page shows records and your dashboard does not, the problem is between the UI and
telemetryd rather than at ingest — [connect the UI](connect-the-ui.md) starts there.
