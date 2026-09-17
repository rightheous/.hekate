# `~/.hekate` 디렉토리 트리

`target`, `.git` 디렉토리는 제외하고 깊이 3까지 표시한 구조다.

```text
~/.hekate
├── .gitignore
├── Cargo.lock
├── Cargo.toml
├── README.md
├── HEKATE_continuity_architecture_v0.1.md
├── HEKATE_project_structure_v0.1.md
├── clippy.toml
├── config.example.toml
├── docs/
│   ├── architecture.md
│   ├── decisions/
│   ├── event-catalog.md
│   ├── identity-and-agency.md
│   ├── invariants.md
│   ├── state-machines.md
│   └── v1-progress.md
├── migrations/
│   ├── 0001_initial.sql
│   ├── 0002_indexes.sql
│   ├── 0003_cognitive_traces.sql
│   └── 0004_v1_state.sql
├── rust-toolchain.toml
├── rustfmt.toml
├── src/
│   ├── adapters/
│   │   ├── local_policy.rs
│   │   ├── local_workspace.rs
│   │   ├── primary_model.rs
│   │   └── sqlite/
│   ├── bin/
│   │   └── seed_safe_positions.rs
│   ├── capabilities/
│   ├── core/
│   ├── interfaces/
│   │   ├── cli.rs
│   │   └── mod.rs
│   ├── ports/
│   ├── runtime/
│   ├── bootstrap.rs
│   ├── config.rs
│   ├── lib.rs
│   └── main.rs
├── tests/
│   ├── common/
│   ├── grounded_dissent.rs
│   ├── new_interaction_same_task.rs
│   ├── restart_and_resume.rs
│   ├── sqlite_store.rs
│   ├── two_runs_do_not_collide.rs
│   ├── v1_invariants.rs
│   └── workspace_read.rs
└── var/
    ├── .gitkeep
    ├── artifacts/
    ├── hekate.db
    └── logs/
```
