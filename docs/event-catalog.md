# Event catalog

| Area | Events |
| --- | --- |
| Identity | `PrincipalCreated`, `IdentityVersionCreated`, `RelationshipCreated` |
| Input and work | `ObservationRecorded`, `GoalCreated`, `TaskCreated`, `RunStarted`, `RunSuspended`, `RunCompleted`, `AttemptStarted`, `AttemptCompleted`, `WorkingStateUpdated` |
| Judgment | `DecisionCreated`, `ResponseProduced`, position establish/maintain/revise/retract/record events, conflict open/update/resolve/record events, commitment create/fulfill events |
| Action plane | `ActionIntentCreated`, `OperationPlanned`, `OperationAuthorized`, `ApprovalRequested`, `ApprovalResolved`, `OperationStarted`, `OperationSucceeded`, `OperationFailed`, `OperationStateUnknown`, `ReceiptRecorded`, `VerificationRecorded`, `ArtifactCreated` |
| Completion evidence | `CompletionCriterionDefined`, `CompletionClaimCreated`, `CompletionClaimVerified`, `CompletionClaimRejected` |
| Memory | `MemoryCandidateCreated`, `MemoryPromoted`, `MemoryRejected`, `MemorySuperseded`, `MemoryExpired` |
| Sleep | `SleepRunStarted`, `IntegrationCandidateCreated`, `SleepRunCompleted`, `SleepRunInterrupted`, `SleepRunFailed` |

Every event stores actor, subject, source, correlation/causation, confidence where applicable, and a SHA-256 integrity hash. Cognitive traces are stored alongside the ledger transaction and reference the context sequence/hash and evidence IDs.
