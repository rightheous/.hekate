# Sleep model budget batching

Sleep preflight calls the same `PrimaryModel::prepare_sleep` renderer and
`prompt_budget::prepare` check used immediately before a request. The check is
local and does not contact the provider. Existing output reservations, safety
margin, context limit, and whole-item recall removal stay unchanged.

For a new run, Sleep checks anchors first, then candidate prefixes from longest
to shortest. Recall is fetched once per bounded candidate Observation and reused
by the local prefix checks. Recall queries exclude the whole bounded candidate
window so no deferred Observation can appear as evidence while it waits for
its own cycle. Each attempt rebuilds seed lists, selected recall, byte report,
and snapshot hash. A fitting prefix is written to `SleepRunStarted`; an
unfittable anchors-only or one-Observation attempt is recorded as a failed Run
with its attempted prefix and budget report.

Completion advances the cursor through the last selected Observation. Deferred
Events remain untouched and are eligible in the next cycle. A resumed run keeps
its recorded `seed_event_ids`; if current settings no longer fit, it fails with
the same IDs and no cursor or candidate change. Foreground leases and stale
revisions still interrupt before generation or completion.

`SleepRun.context_budget_report` keeps the existing byte report and adds an
optional model `BudgetReport`. The selected count is `included_seed_count`; the
number considered is selected plus `deferred_seed_count`. `model_budget` records
the estimated input, configured context, reserved output, safety margin, and
optional recall items removed by the shared preparer. Run `error_kind`
distinguishes anchors-only overflow, a single Observation that cannot fit, a
resumed batch that no longer fits, and correction-request overflow. The input
estimate remains conservative UTF-8 bytes plus template overhead, not a
tokenizer measurement.

An Observation larger than the remaining required-input budget cannot be
shortened or skipped; the run records the failure and the cursor stays put. A
later smaller Observation is not processed ahead of it. No migration is needed:
the new report field defaults to absent when older run events are replayed.
