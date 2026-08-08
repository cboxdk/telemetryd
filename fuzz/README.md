# Fuzzing

Every parser here reads untrusted bytes straight off the network: OTLP JSON and
Prometheus `remote_write` from an ingest request, three query languages from an HTTP
query string, and — before any of them — the decompressor that undoes
`Content-Encoding`. A panic in any of them is a request that kills a worker.

```bash
rustup toolchain install nightly          # libFuzzer needs it
cargo install cargo-fuzz

cargo fuzz run logql fuzz/corpus/logql fuzz/seeds/logql -- -max_total_time=60
cargo fuzz list                           # the six targets
```

`body_decompression` is the one target that asserts more than "did not panic". It
checks that whatever comes out is under the cap, for every input the fuzzer can build —
because a decompression cap that holds only for the streams we thought of is not a
bound on memory, and this is the one place where a few bytes of input choose how many
gigabytes we allocate.

**Give the corpus directory first and the seeds second.** libFuzzer writes new
discoveries into the *first* directory it is handed, so `cargo fuzz run t seeds/t`
fills the seed set with generated inputs — which is exactly what happened here once,
and left 1,799 files where six belonged.

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
