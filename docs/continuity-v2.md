# Continuity engine v2

Continuity resolves each recorded Observation against owned, open Goal and Task
records. A resolution records the selected Focus, candidate evidence, reason,
and source revision before the model context is built. Strong explicit IDs and
continuation language can select a task; weak or competing matches produce a
clarification without changing Goal or Task state. Thread IDs are supporting
evidence only.

The event flow is `ObservationRecorded` → `FocusResolved` with any Goal/Task/Run
changes in one expected-revision batch → `AttemptStarted` → context snapshot
and model call → Decision/Response/Attempt completion. Projection and replay
remain owned by `Projector`. `FocusResolved` is an additive ledger event; the
projected `focus_resolutions` field has a serde default, so this adds no SQL
schema migration.

`runtime/consolidation` is only a boundary around existing Sleep and
MemoryIntegration ownership. `runtime/initiative` defines a future proposal
and outbox boundary; it does not deliver messages or authorize capabilities.
Neither module has behavior in this implementation unit.
