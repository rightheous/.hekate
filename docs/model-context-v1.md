# Model I/O & Context Budget v1

Skeleton checkpoint. The existing CognitiveModel remains the judgment boundary;
model_transport handles HTTP only. Implementation proceeds immediately after this
compilable checkpoint. The new functions are not yet used by PrimaryModel and
cannot return a placeholder success.

Planned path: ThoughtContext/SleepContext -> prompt_renderer -> prompt_budget ->
PreparedModelRequest -> explicit transport -> completed content -> strict parser
with the prepared reference allowlist -> existing engine validation and ledger.

ModelIoConfig will be a defaulted [model_io] table. Missing context_tokens is an
error at generation time, not bootstrap. Context metadata maxima are never used
as deployment limits. Existing generation limits remain the compatibility defaults;
the evaluation explicitly sets 8192/2048/2048/512.

Estimation starts with UTF-8 bytes plus message/template overhead (estimated, not
an exact tokenizer guarantee). Optional items are selected whole, in priority order.
Mandatory overflow fails without HTTP. Corrections use the same final budget gate.

Native Ollama uses options.num_ctx/num_predict and optional typed think, distinct
from OpenAI reasoning_effort. Truncation, empty content and unknown completion are
failures; thinking never supplies a final answer. Safe diagnostics contain no body.

Reference review (no source copied): ZeroClaw commit
7374ac7353d3228f8c7bdd1cae6b81a4d50b9d2e, providers/ollama.rs and runtime/agent/{history,
history_trim,memory_inject}. Whole-item selection is appropriate here; its
thinking-to-content fallback and character-based token estimate are not adopted.
API references: https://docs.ollama.com/api/chat and
https://docs.ollama.com/capabilities/thinking.
