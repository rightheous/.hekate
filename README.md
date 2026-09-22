# HEKATE v1

HEKATE is a persistent Rust/Tokio/SQLite runtime for one continuing relationship. The SQLite event ledger is canonical; the current state snapshot and entity tables are projections rebuilt from it. The cognitive model proposes a three phase Thought Cycle, while policy and the operation ledger control every capability execution.

The default model endpoint is the already running Hermes service:

```text
http://127.0.0.1:19190/v1  model: hekate-qwen
```

HEKATE does not add a provider or download a model. Override the endpoint, model, database, or workspace with `config.example.toml` or `HEKATE_*` environment variables. The local Hermes endpoint does not require an API key; an optional `HEKATE_MODEL_API_KEY` is supported for compatible deployments.

Run an interaction and keep its external message identity for deduplication:

```bash
cargo run -- --thread-id thread-1 --message-id message-1 "What should we do next?"
```

Useful commands are `--inspect`, `--identity`, `--positions`, `--conflicts`, `--pending`, `--resume`, `--completion-status TASK_ID`, and `--completion-claims`. Memory candidates use `--memory-candidate TEXT`, `--memory-list`, `--promote-memory ID`, and `--reject-memory ID`.

Action proposals are persisted as planned operations. A write proposal returns an approval ID; resolve it with `--approve ID` or `--deny ID`, then execute the operation with `--execute ID`. Execution records a receipt, verification, and artifact provenance where the result identifies a file.

Required checks:

```bash
cargo fmt --check
cargo check
cargo test
```
