---
title: "Query from the shell"
weight: 70
description: "Read your logs over SSH, and get them out as JSON lines."
---

# Query from the shell

```bash
telemetryd query '{app="checkout"} |= "declined"' --since 6h --limit 50
```

```
2026-08-08T14:50:51.762Z checkout payment 45 declined
2026-08-08T14:50:46.762Z checkout payment 40 declined
```

Two jobs, one command.

## Debugging

When something is wrong, the UI is often the thing you cannot reach — it is a browser
away, or it is the component that broke. A query you can run over SSH and pipe into
ordinary tools is the difference between diagnosing a problem and describing it.

```bash
# How many errors in the last hour, by app?
telemetryd query '{level="error"}' --since 1h --limit 5000 --output json \
  | jq -r '.labels.app' | sort | uniq -c | sort -rn
```

It talks to a running instance, so it sees records still in the buffer and it obeys the
configured tokens:

```bash
export TELEMETRYD_AUTH_QUERY_TOKEN=…     # not --token: arguments show up in `ps`
telemetryd query '{app="checkout"}' --url http://10.0.0.5:4319
```

## Getting your data out

A self-hosted tool that can only be read through its own interface has quietly become a
place data goes in. `--output json` writes one JSON object per line — timestamp, line,
labels and structured metadata — which every other tool on the machine can read.

```bash
telemetryd query '{app="checkout"}' --since 7d --limit 100000 --forward \
  --output json > checkout-week.jsonl
```

`--forward` reads oldest first, which is the order you want when exporting.

`--limit` is capped by the server at 5,000 per request, so a large export is a loop
over time windows rather than one call:

```bash
for day in {7..1}; do
  telemetryd query '{app="checkout"}' --since "${day}d" --limit 5000 --forward \
    --output json >> export.jsonl
done
```

## Exporting from a backup

There is no offline reader, and there does not need to be one: start an instance on the
copy and query that.

```bash
telemetryd serve --data-dir ./restored --listen 127.0.0.1:4399 &
telemetryd query '{app="checkout"}' --url http://127.0.0.1:4399 --output json
```

See [Back up and restore](back-up-and-restore.md) for why copying the directory is safe
in the first place.

## When a query is refused

telemetryd implements a subset of LogQL and names the construct it does not support,
rather than failing vaguely:

```
error: the query was rejected: LogQL `| unwrap` is not supported by telemetryd
```

A malformed query says where it went wrong:

```
error: the query was rejected: expected a quoted label value but the query ended: "{app="
```

[COMPATIBILITY.md](https://github.com/cboxdk/telemetryd/blob/main/COMPATIBILITY.md) is
the full list of what is supported.
