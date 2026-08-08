# Fuzzing

Every parser here reads untrusted bytes straight off the network: OTLP JSON and
Prometheus `remote_write` from an ingest request, and three query languages from an
HTTP query string. A panic in any of them is a request that kills a worker.

```bash
rustup toolchain install nightly          # libFuzzer needs it
cargo install cargo-fuzz

cargo fuzz run logql fuzz/seeds/logql -- -max_total_time=60
cargo fuzz list                           # the five targets
```

Targets seed themselves from `fuzz/seeds/<target>/`, which is committed. Starting from
random bytes wastes most of a budget on inputs the parser rejects in its first few
bytes; starting from valid queries and payloads puts the fuzzer inside the grammar
straight away. On a laptop the difference measured about 300k executions per minute
against 24k.

Whatever the fuzzer then discovers lives in `fuzz/corpus/` and is not committed: it is
reproducible from the seeds and belongs to a run rather than to the repository. A
crashing input *is* worth committing — as a test case in the crate it broke, not as a
corpus file.

## Why this exists

A proptest over the shared lexer once found a reachable panic: a backslash before a
multi-byte character advanced the cursor by one byte, leaving it mid-character, and the
next slice panicked. That is exactly the shape coverage-guided fuzzing finds quickly
and hand-written examples find by luck.
