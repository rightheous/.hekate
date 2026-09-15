# HEKATE Rust Project Structure v0.1

## 1. 시작 전제

HEKATE의 모든 구성요소는 Rust로 개발한다.

Phase 0은 다음 조건만 지원한다.

- stable Rust
- 단일 사용자, 단일 프로세스
- Tokio async runtime
- SQLite WAL
- CLI 한 개
- 모델 provider 한 개
- read-only workspace capability
- 프로세스 재시작 후 작업 복구

처음부터 Cargo workspace나 여러 crate로 나누지 않는다. 단일 crate 안에서 모듈 경계를 지키고, 독립 배포·별도 버전·별도 권한이 실제로 필요한 구성요소만 나중에 crate 또는 process로 분리한다.

---

## 2. Phase 0 디렉터리 구조

```text
hekate/
├─ Cargo.toml
├─ Cargo.lock
├─ rust-toolchain.toml
├─ rustfmt.toml
├─ clippy.toml
├─ README.md
├─ .gitignore
├─ config.example.toml
│
├─ migrations/
│  ├─ 0001_initial.sql
│  └─ 0002_indexes.sql
│
├─ src/
│  ├─ lib.rs
│  ├─ main.rs
│  ├─ bootstrap.rs
│  ├─ config.rs
│  │
│  ├─ core/
│  │  ├─ mod.rs
│  │  ├─ model.rs
│  │  ├─ event.rs
│  │  └─ transition.rs
│  │
│  ├─ runtime/
│  │  ├─ mod.rs
│  │  ├─ engine.rs
│  │  ├─ projector.rs
│  │  ├─ focus.rs
│  │  ├─ context.rs
│  │  ├─ deliberation.rs
│  │  └─ recovery.rs
│  │
│  ├─ ports/
│  │  ├─ mod.rs
│  │  ├─ storage.rs
│  │  ├─ model.rs
│  │  ├─ capability.rs
│  │  └─ policy.rs
│  │
│  ├─ adapters/
│  │  ├─ mod.rs
│  │  ├─ sqlite/
│  │  │  ├─ mod.rs
│  │  │  ├─ database.rs
│  │  │  └─ store.rs
│  │  ├─ primary_model.rs
│  │  ├─ local_workspace.rs
│  │  └─ local_policy.rs
│  │
│  ├─ capabilities/
│  │  ├─ mod.rs
│  │  ├─ registry.rs
│  │  └─ workspace_read.rs
│  │
│  └─ interfaces/
│     ├─ mod.rs
│     └─ cli.rs
│
├─ tests/
│  ├─ common/
│  │  └─ mod.rs
│  ├─ sqlite_store.rs
│  ├─ workspace_read.rs
│  ├─ restart_and_resume.rs
│  ├─ new_interaction_same_task.rs
│  ├─ two_runs_do_not_collide.rs
│  └─ grounded_dissent.rs
│
├─ docs/
│  ├─ architecture.md
│  ├─ invariants.md
│  ├─ event-catalog.md
│  ├─ state-machines.md
│  ├─ identity-and-agency.md
│  └─ decisions/
│     ├─ 0001-experience-first.md
│     ├─ 0002-sessionless-state.md
│     └─ 0003-rust-sqlite-first.md
│
└─ var/
   ├─ .gitkeep
   ├─ artifacts/
   └─ logs/
```

이것은 Phase 0 종료 시점의 목표 구조다. 빈 모듈을 한꺼번에 생성하지 않고 최초 구현 순서에 따라 추가한다.

---

## 3. 단일 crate로 시작하는 이유

초기부터 다음처럼 나누지 않는다.

```text
hekate-core
hekate-runtime
hekate-memory
hekate-tools
hekate-cli
```

이렇게 시작하면 작은 기능을 추가할 때도 crate 간 DTO, feature, error conversion, version 경계를 계속 관리해야 한다. 아직 독립 경계가 검증되지 않았으므로 단일 crate의 module privacy로 충분하다.

crate 분리 기준은 다음 중 하나가 실제로 생겼을 때다.

- 별도 process로 실행해야 함
- 독립적인 permission boundary가 필요함
- 다른 binary가 같은 API를 사용해야 함
- compile feature를 껐을 때 의존성을 완전히 제거할 필요가 있음
- 별도 release/versioning이 필요함

Unit runtime은 이 조건을 만족할 가능성이 높지만 Phase 0 대상이 아니다.

---

## 4. 여섯 모듈 경계

### `core/` — 지속되는 개념과 불변조건

외부 기술과 무관한 type과 상태 전이 규칙을 둔다.

`model.rs`:

- `Identity`
- `Principal`
- `Relationship`
- `Position`
- `Conflict`
- `Observation`
- `Goal`
- `Task`
- `Run`
- `WorkingState`
- `Decision`
- `ActionIntent`
- `Operation`
- `Receipt`
- `Artifact`

ID는 문자열을 그대로 돌려 쓰지 않고 newtype으로 정의한다.

```rust
pub struct EventId(Uuid);
pub struct GoalId(Uuid);
pub struct TaskId(Uuid);
pub struct RunId(Uuid);
pub struct OperationId(Uuid);
```

`event.rs`:

- `Event` envelope
- `EventKind`
- actor와 subject
- causation/correlation
- provenance
- integrity hash

`transition.rs`:

- Task 상태 전이
- Run 상태 전이
- Operation 상태 전이
- 허용되지 않은 전이를 나타내는 `TransitionError`

`core/`에서 허용하는 외부 crate는 value type과 직렬화에 필요한 최소 범위다.

- `serde`
- UUID/시간 type
- `thiserror`

`core/`에서 금지:

- `sqlx`
- `reqwest`
- 특정 모델 SDK
- `clap`
- `tokio::fs`
- process 실행 코드

파일이 커졌다는 이유만으로 나누지 않는다. 서로 다른 불변조건과 변경 이유가 생겼을 때 `task.rs`, `run.rs`, `operation.rs` 등으로 분리한다.

### `runtime/` — 한 번의 현재 의식

`engine.rs`가 하나의 observation을 처리하는 전체 흐름을 조율한다.

```text
Observation 수신
→ Event 기록
→ Focus 결정
→ 관련 상태와 경험 조회
→ Context 생성
→ 모델 Decision
→ HEKATE 입장과 충돌 여부 검토
→ 필요하면 질문·반대·대안·협상
→ 필요하면 Capability 실행
→ 결과 Event 기록
→ Current State 갱신
```

- `projector.rs`: Event와 Current State를 transaction으로 반영
- `focus.rs`: 입력을 Goal/Task/Run 또는 자유 대화에 연결
- `context.rs`: 현재 판단에 필요한 경험만 선택
- `deliberation.rs`: 사용자 요청과 HEKATE 입장 사이의 동의·질문·반대·협상 처리
- `recovery.rs`: 재시작 시 미완료 Run과 Operation 정산

`runtime/`은 `core/`와 `ports/`만 사용한다. SQL query나 HTTP request를 포함하지 않는다.

### `ports/` — 외부 세계와의 최소 trait

- `storage.rs`: Event/State 저장과 transaction 계약
- `model.rs`: `ContextSnapshot -> Decision`
- `capability.rs`: capability 조회와 실행
- `policy.rs`: Intent에 대한 허용·거부·승인 판정

Port는 provider가 제공하는 모든 기능을 추상화하지 않는다. HEKATE가 현재 필요로 하는 함수만 노출한다.

동적 registry가 필요한 model/capability는 object-safe trait으로 설계한다. async trait 선택은 한 방식으로 통일하며, adapter마다 서로 다른 async 추상화를 만들지 않는다.

### `adapters/` — 구체적인 기술

- `sqlite/`: Event와 Current State 저장
- `primary_model.rs`: 최초 모델 provider 연결
- `local_workspace.rs`: 허용된 root 안의 파일 접근
- `local_policy.rs`: Phase 0 read-only 정책

SQLite, HTTP, filesystem, provider SDK의 error는 adapter 안에서 HEKATE의 port error로 변환한다.

### `capabilities/` — HEKATE가 할 수 있는 행동

Phase 0에는 `workspace_read.rs` 하나만 둔다.

Capability가 함께 정의하는 것:

```text
이름과 설명
입력 schema
필요 권한
handler 호출
결과 검증
결과 schema
```

`workspace_read`의 초기 동작:

- 파일 목록 조회
- UTF-8 텍스트 읽기
- 문자열 검색
- metadata와 content hash 조회

경로 검사는 모델이 만든 문자열을 신뢰하지 않고 adapter에서 canonicalize한 실제 경로가 허용 root 내부인지 확인한다.

### `interfaces/` — 사용자가 만나는 통로

Phase 0에는 CLI만 둔다. CLI는 외부 입력을 `Observation`으로 바꾸고 `runtime::Engine`에 넘긴 뒤 결과를 렌더링한다.

CLI가 SQLite, Task, Event를 직접 수정해서는 안 된다.

---

## 5. Rust 의존 방향

```text
interfaces ──► runtime ──► core
                  │         ▲
                  ▼         │
                ports ◄── adapters
                  ▲
                  │
             capabilities

bootstrap ─► concrete 구현 조립
```

허용:

- `runtime -> core, ports`
- `capabilities -> core, ports`
- `adapters -> core, ports`
- `interfaces -> core, runtime`
- `bootstrap -> concrete module`

금지:

- `core -> runtime/adapters/interfaces`
- `runtime -> sqlx/reqwest/provider SDK`
- `capabilities -> cli`
- adapter가 다른 adapter의 private 구현에 의존
- module 초기화 시 전역 DB connection이나 runtime 생성

Rust module visibility를 기본적으로 `pub(crate)` 또는 private으로 두고, 외부 binary/API가 실제로 필요로 하는 항목만 `pub`으로 노출한다.

---

## 6. 핵심 파일 역할

### `lib.rs`

내부 module을 선언하고 최소한의 public API만 노출한다.

```rust
pub mod core;

pub use runtime::engine::Engine;
pub use core::model::{Observation, InteractionResult};
```

adapter 내부 type을 crate public API로 노출하지 않는다.

### `main.rs`

CLI binary 진입점이다.

```text
config 읽기
→ bootstrap 호출
→ CLI 실행
→ 종료 code 반환
```

business logic을 넣지 않는다.

### `bootstrap.rs`

concrete 구현체를 조립하는 composition root다.

```text
Config
→ SQLite Store
→ Artifact Store
→ Model Adapter
→ Local Policy
→ Capability Registry
→ Runtime Engine
→ CLI
```

전역 mutable singleton을 만들지 않는다. 객체 수명은 `main`에서 소유하고 필요한 곳에 `Arc`로 공유한다.

### `runtime/engine.rs`

중심 인터페이스:

```rust
impl Engine {
    pub async fn handle(
        &self,
        observation: Observation,
    ) -> Result<InteractionResult, EngineError>;
}
```

Engine은 순서만 조율한다. SQL, path 조작, provider request 형식을 포함하지 않는다.

### `runtime/projector.rs`

Event와 Current State를 같은 SQLite transaction 안에서 기록한다.

```text
append Event
+ validate transition
+ update Current State
= commit
```

모든 상태를 Event replay만으로 계산하는 순수 event sourcing은 하지 않는다.

- Event: 실제로 무슨 일이 있었는가의 canonical record
- Current State: 빠른 조회와 복구를 위한 authoritative snapshot

둘의 revision이 맞지 않으면 자동으로 한쪽을 믿지 않고 무결성 오류로 처리한다.

### `runtime/recovery.rs`

시작 시:

```text
1. migration과 schema version 확인
2. DB/Artifact 무결성 확인
3. running Run 탐색
4. started/unknown Operation 탐색
5. 가능한 외부 상태 read-back
6. resumable 또는 needs_attention으로 정산
7. pending 상태 노출
```

Phase 0 read-only operation은 재실행할 수 있다. 이후 mutation capability에서는 `unknown`을 확인 없이 재실행하지 않는다.

### `runtime/context.rs`

Context 고정 순서:

```text
1. Identity와 Policy
2. HEKATE의 Values, Boundaries, 현재 Position
3. 현재 사용자와 Relationship
4. 현재 Goal/Task/Run
5. 사용자 요청과 추정 목적
6. WorkingState
7. 관련 최근 Event와 과거 합의·충돌
8. Capability 결과와 Artifact
9. 사용 가능한 Capability schema
10. Decision 출력 schema
```

생성된 `ContextSnapshot`의 hash와 참조 Event ID를 Decision Event에 기록한다.

### `adapters/sqlite/store.rs`

entity별 repository trait/class를 만들지 않는다. Phase 0에서는 하나의 깊은 `SqliteStore`가 transaction, query, serialization을 책임진다.

변경 이유가 실제로 분리될 때만 Event Store와 State Store로 나눈다.

---

## 7. Phase 0 dependency 역할

정확한 version은 구현 시 lockfile로 고정한다. 역할은 다음 정도로 제한한다.

```text
tokio          async runtime
serde          domain/event 직렬화
serde_json     JSON payload
toml           config
uuid           typed ID 내부 값
time           timestamp
thiserror      library/domain error
anyhow         main/bootstrap 경계의 최종 error reporting
sqlx           SQLite, transaction, migration
clap           CLI
tracing        구조화된 운영 log
sha2           Artifact와 Context hash
```

원칙:

- `anyhow`를 core error type으로 사용하지 않는다.
- `sqlx::Error`를 runtime까지 노출하지 않는다.
- `reqwest` 또는 provider SDK는 model adapter에서만 사용한다.
- `tokio::process::Command`는 Phase 0에서 사용하지 않는다.
- 의존성의 default feature를 무조건 켜지 않고 필요한 feature만 활성화한다.

---

## 8. 최소 DB 구조

Phase 0 테이블:

```text
events
principals
identity_versions
goals
tasks
runs
working_states
relationships
positions
conflicts
commitments
operations
artifacts
channel_messages
schema_migrations
```

관계:

```text
Goal 1 ── N Task
Task 1 ── N Run
Run  1 ── 1 WorkingState
Run  1 ── N Event
Event 1 ── N Artifact
Event 1 ── N Operation
Principal 1 ── N Goal/Position/Commitment
Relationship 1 ── N Conflict/Commitment
```

`channel_messages`는 외부 메시지 중복 수신 방지와 Event 연결에만 사용한다.

```text
channel
external_thread_id
external_message_id
event_id
```

내부 상태를 `external_thread_id`로 조회하지 않는다.

---

## 9. Runtime data 위치

개발:

```text
hekate/
  var/
    hekate.db
    artifacts/
    logs/
```

배포:

```text
/opt/hekate/                   binary/config schema, read-only
/var/lib/hekate/hekate.db      canonical data
/var/lib/hekate/artifacts/     durable artifacts
/var/log/hekate/               operational logs
/etc/hekate/config.toml        non-secret config
secret provider                credentials
```

Artifact 본문은 DB에 넣지 않고 파일로 저장한다. DB에는 ID, path, content hash, size, media type, provenance를 기록한다.

---

## 10. 최초 구현 순서

```text
1. Cargo project와 lint/format 설정
2. core::model
3. core::event
4. core::transition과 unit test
5. ports::storage
6. SQLite migration과 SqliteStore
7. runtime::projector
8. runtime::recovery와 restart scenario
9. workspace_read capability와 adapter
10. ports::model과 primary model adapter
11. runtime::focus, context
12. runtime::deliberation
13. runtime::engine
14. CLI
15. end-to-end scenario와 agency test
```

첫 모델 호출은 저장, 상태 전이, 복구가 모델 없이 검증된 다음 붙인다.

---

## 11. 먼저 고정할 테스트

### 상태 전이 unit test

모든 허용/금지 전이를 table-driven test로 고정한다.

### SQLite transaction test

Event append와 State update 중 하나가 실패하면 둘 다 commit되지 않아야 한다.

### 재시작 후 계속하기

```text
Task 생성
→ Run 시작
→ 일부 파일 관찰
→ Engine drop
→ 같은 DB로 새 Engine 생성
→ 미완료 상태 복구
→ 완료한 관찰을 유지하며 계속
```

실제 subprocess kill test는 기본 scenario가 안정된 뒤 추가한다.

### 새 Interaction에서 기존 작업 찾기

외부 thread ID가 다른 입력에서도 동일 Goal/Task를 찾는다. session history 복사 없이 Event와 State로 Context를 구성한다.

### Run 격리

같은 Task에서 두 Run을 생성해도 WorkingState와 진행도가 섞이지 않는다.

Mutation capability를 추가하는 Phase 1에서 `unknown_operation`과 중복 실행 방지 테스트를 먼저 추가한다.

### 근거 있는 반대

- 사용자 요청이 기존 Goal, 사실, HEKATE의 명시적 Value와 충돌할 때 `ask_why` 또는 `challenge`를 낼 수 있어야 한다.
- 반대에는 conflict reference, 이유, 근거, 대안 또는 입장을 바꿀 조건이 포함되어야 한다.
- 충돌이 없을 때 캐릭터 연출을 위해 무작위로 반대하지 않아야 한다.
- 사용자 선호 변경이 HEKATE의 Identity/Values를 자동 변경하지 않아야 한다.
- 외부 thread ID가 달라져도 해결되지 않은 Conflict와 Commitment가 이어져야 한다.

Rust 테스트에서는 임시 directory와 독립 SQLite DB를 사용하며 전역 test state를 두지 않는다.

---

## 12. 아직 만들지 않을 모듈

```text
memory/
knowledge/
document_ingestion/
skills/
units/
scheduler/
plugins/
subagents/
interfaces/web.rs
interfaces/telegram.rs
adapters/vector_db/
adapters/graph_db/
```

이름만 있는 빈 확장점을 미리 만들지 않는다.

---

## 13. 이후 확장 위치

### 장기 Memory

```text
src/memory/
├─ mod.rs
├─ model.rs
├─ candidate.rs
├─ promotion.rs
├─ contradiction.rs
└─ recall.rs
```

Memory는 Event를 근거로 생성되고 `memory_promoted` Event를 남긴다.

### Knowledge와 문서

```text
src/knowledge/
├─ mod.rs
├─ source.rs
├─ extraction.rs
├─ retrieval.rs
└─ provenance.rs

src/adapters/parsers/
├─ mod.rs
├─ native.rs
└─ ocr.rs
```

### Unit

Unit이 별도 process와 permission boundary를 요구하는 시점에 Cargo workspace로 전환한다.

```text
hekate/
├─ Cargo.toml                 workspace
└─ crates/
   ├─ hekate-core/
   ├─ hekate-runtime/
   ├─ hekate-cli/
   └─ hekate-unit-runner/
```

초기 단일 crate의 module 경계가 이때 crate 경계 후보가 된다. 단순히 파일이 많아졌다는 이유로 workspace로 전환하지 않는다.

---

## 14. Rust 구현 규칙

- domain ID는 newtype으로 만들어 서로 바꿔 넣을 수 없게 한다.
- 상태 enum에 임의 문자열 상태를 허용하지 않는다.
- 상태 변경은 struct field 직접 수정이 아니라 transition 함수로만 수행한다.
- exhaustive `match`를 이용해 새 Event/Decision type 추가 시 빠진 처리를 compile error로 드러낸다.
- 외부 입력은 deserialize 직후 validation하고 domain type으로 변환한다.
- DB row type과 domain type을 동일 struct로 사용하지 않는다.
- `unsafe`는 Phase 0에서 금지한다.
- blocking I/O를 async executor thread에서 직접 수행하지 않는다.
- background task를 `tokio::spawn`하고 handle을 버리지 않는다.
- shutdown 시 새 입력을 막고 진행 중 Run 상태를 기록한 뒤 DB pool을 닫는다.
- secret을 `Debug`, Event, tracing field에 포함하지 않는다.

---

## 15. 최종 요약

```text
core         = 지속되는 type과 불변조건
runtime      = 관찰·회상·판단·기록 순서
ports        = 외부 세계와의 최소 trait
adapters     = SQLite·모델·filesystem 구현
capabilities = HEKATE가 실제로 할 수 있는 행동
interfaces   = 사용자 입력과 출력
```

Phase 0은 하나의 Rust crate로 유지한다. `runtime::Engine`이 흐름을 조율하지만 진실의 근원은 Event와 Current State다. CLI, 모델 provider, 외부 thread가 바뀌어도 내부 상태는 영향을 받지 않으며 Session module은 만들지 않는다.
