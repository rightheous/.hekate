# `~/.hekate` 디렉토리 트리

현재 작업 트리 기준 구조다. `.git`과 빌드 산출물 `target`은 제외했다.

```text
~/.hekate
├── .gitignore
├── Cargo.lock
├── Cargo.toml
├── HEKATE_continuity_architecture_v0.1.md
├── HEKATE_project_structure_v0.1.md
├── README.md
├── clippy.toml
├── config.example.toml
├── docs/
│   ├── architecture.md
│   ├── decisions/
│   │   ├── 0001-experience-first.md
│   │   ├── 0002-sessionless-state.md
│   │   └── 0003-rust-sqlite-first.md
│   ├── event-catalog.md
│   ├── identity-and-agency.md
│   ├── invariants.md
│   ├── state-machines.md
│   └── v1-progress.md
├── hekate-directory-tree.md
├── migrations/
│   ├── 0001_initial.sql
│   ├── 0002_indexes.sql
│   ├── 0003_cognitive_traces.sql
│   ├── 0004_v1_state.sql
│   └── 0005_embedding_foundation.sql
├── rust-toolchain.toml
├── rustfmt.toml
├── skills/
│   ├── browser/
│   │   └── README.md
│   ├── computer-use/
│   │   └── SKILL.md
│   ├── document-reader/
│   ├── git/
│   └── workspace/
├── src/
│   ├── adapters/
│   │   ├── browser/
│   │   │   ├── mod.rs
│   │   │   └── tests.rs
│   │   ├── computer_use/
│   │   │   └── mod.rs
│   │   ├── docling/
│   │   │   └── mod.rs
│   │   ├── embedding/
│   │   │   └── mod.rs
│   │   ├── sqlite/
│   │   │   ├── database.rs
│   │   │   ├── embedding.rs
│   │   │   ├── mod.rs
│   │   │   └── store.rs
│   │   ├── git.rs
│   │   ├── local_policy.rs
│   │   ├── local_workspace.rs
│   │   ├── mod.rs
│   │   └── primary_model.rs
│   ├── bin/
│   │   └── seed_safe_positions.rs
│   ├── capabilities/
│   │   ├── browser.rs
│   │   ├── computer_use.rs
│   │   ├── document_reader.rs
│   │   ├── git.rs
│   │   ├── mod.rs
│   │   ├── registry.rs
│   │   ├── workspace_read.rs
│   │   └── workspace_write.rs
│   ├── core/
│   │   ├── embedding.rs
│   │   ├── event.rs
│   │   ├── mod.rs
│   │   ├── model.rs
│   │   └── transition.rs
│   ├── interfaces/
│   │   ├── cli.rs
│   │   └── mod.rs
│   ├── ports/
│   │   ├── capability.rs
│   │   ├── embedding.rs
│   │   ├── mod.rs
│   │   ├── model.rs
│   │   ├── policy.rs
│   │   └── storage.rs
│   ├── runtime/
│   │   ├── context.rs
│   │   ├── contest.rs
│   │   ├── deliberation.rs
│   │   ├── embedding_indexer.rs
│   │   ├── engine.rs
│   │   ├── focus.rs
│   │   ├── mod.rs
│   │   ├── projector.rs
│   │   └── recovery.rs
│   ├── bootstrap.rs
│   ├── config.rs
│   ├── lib.rs
│   └── main.rs
├── tests/
│   ├── common/
│   │   └── mod.rs
│   ├── browser_unknown.rs
│   ├── computer_use_x11.rs
│   ├── document_reader.rs
│   ├── embedding_foundation.rs
│   ├── git.rs
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
