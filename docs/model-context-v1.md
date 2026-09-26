# Model I/O & Context Budget v1

The existing CognitiveModel remains the judgment boundary; model_transport handles
HTTP only. Foreground and Sleep render their own prompts, apply the same final
budget check, use explicitly selected transports, then pass completed content to
the existing strict parser and engine validation.

ModelIoConfig is a defaulted [model_io] table. Missing context_tokens is an error
at generation time, not bootstrap. Context metadata maxima are never used as
deployment limits. The compatibility defaults retain existing output limits; the
evaluation explicitly sets context/output/output/margin to 8192/2048/2048/512.

Estimation uses UTF-8 bytes plus message/template overhead. It is an estimate, not
an exact tokenizer guarantee. Optional items are selected whole, in priority order.
Mandatory overflow fails without HTTP. Corrections use the same final budget gate.

Native Ollama uses options.num_ctx/num_predict and optional typed think, distinct
from OpenAI reasoning_effort. The foreground context-reduction evaluation sets
num_ctx=8192, max generation=2048 and safety margin=512. Truncation, empty content
and unknown completion are failures; thinking never supplies a final answer. Safe
diagnostics contain no prompt, response or thinking body.

For a live run, build the committed code and explicitly opt in:

```sh
cargo run --bin model_context_eval -- --run-live-evaluation
```

The evaluator requires a clean worktree, uses the local `127.0.0.1:19191` tunnel,
creates its own temporary SQLite database and Git workspace, and writes a unique
JSONL file under `/home/hekate/hekate-evals`. It makes at most eight generation
requests, including one correction per foreground scenario and Sleep. Configured
context is recorded separately from `/api/ps` observed context; unloaded model
metadata is not treated as observed capacity.

If the lexical cross-thread recall scenario misses its source Event, the bounded
follow-up mode reuses that report's isolated DB, indexes its existing events through
the local embedding endpoint, then retries only the recall question. It checks the
prior and follow-up generation count together against the same limit and writes a
separate report:

```sh
cargo run --bin model_context_eval -- --run-recall-followup /home/hekate/hekate-evals/<prior-jsonl>
```

## Settings and wire format

`[model_io]` accepts `transport`, `context_tokens`, `foreground_max_tokens`,
`sleep_max_tokens`, `safety_margin`, `foreground_reasoning_effort`,
`sleep_reasoning_effort`, `foreground_think`, and `sleep_think`. Matching
`HEKATE_MODEL_*` environment variables override these values. Context must be an
explicit deployment setting; model-advertised maxima are informational only.
Existing configs without `[model_io]` still load, but generation fails with a
configuration error until a context limit is supplied.

OpenAI-compatible requests use `/chat/completions`, top-level foreground
`reasoning_effort`, and the existing nested Sleep `model_options` setting. Native
Ollama requests use `/api/chat`, `stream:false`, `options.num_ctx`, and
`options.num_predict`. Native `think` is emitted only when configured; OpenAI
reasoning effort is not translated into native `think`.

## Budget and failures

The request bound is:

```text
estimated complete messages + reserved output + safety margin <= configured context
```

The estimate sums UTF-8 bytes and fixed message/template overhead. It is not an
exact-tokenizer bound. The complete serialized messages include any correction.
Whole optional evidence items are removed in snapshot priority order; required
input is never cut. If the required prompt does not fit, the call fails before HTTP.
Server prompt/completion usage and finish reason are recorded alongside the
estimate so operators can compare them without logging prompt or response text.

Trace `error_kind` distinguishes `context_budget_exceeded`, `provider_error`,
`timeout`, `malformed_response`, `truncated_response`, `incomplete_response`,
`empty_content`, `thought_cycle_json_error`, and `invalid_judgment`. Both transports
require an explicit normal completion marker. A length-limited response is rejected
even if its partial body happens to parse; reasoning/thinking text never substitutes
for final content. Existing malformed Thought Cycle correction remains limited to
one retry and is budget-checked again.

## Dry run

To inspect an already serialized `ThoughtContext` without opening the database or
contacting a model, use:

```sh
hekate --model-dry-run context.json --model-purpose foreground
```

It prints counts, IDs, snapshot hash, and budget only. Use `--model-purpose sleep`
for a serialized `SleepContext`.

Reference review (no source copied): ZeroClaw commit
7374ac7353d3228f8c7bdd1cae6b81a4d50b9d2e, providers/ollama.rs and runtime/agent/{history,
history_trim,memory_inject}. Whole-item selection is appropriate here; its
thinking-to-content fallback and character-based token estimate are not adopted.
API references: https://docs.ollama.com/api/chat and
https://docs.ollama.com/capabilities/thinking.
