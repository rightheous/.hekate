# HEKATE Continuity Architecture v0.1

## 1. 목표

HEKATE는 대화 세션에 종속된 챗봇이 아니라, 사용자와 장기간 함께하며 자신의 경험을 보존하고 필요한 순간에 회상하여 행동하는 개인 AI다.

핵심 목표는 다음 한 문장으로 정의한다.

> 채널, 대화창, 프로세스, 모델이 바뀌어도 HEKATE가 자신이 누구이며 사용자와 무엇을 해왔고 지금 무엇을 중요하게 여기는지 잃지 않는다.

HEKATE의 연속성은 모델 내부 상태나 대화 세션이 아니라 다음 요소의 결합으로 만들어진다.

- 지속적인 Identity
- append-only Experience Ledger
- 현재 상태를 나타내는 State Model
- 필요한 경험을 회상하는 Recall Engine
- 현재 의식을 구성하는 Context Builder
- 행동을 통제하는 Policy와 Executor
- 경험을 장기 기억과 절차로 승격하는 Memory Pipeline
- 자기 입장과 사용자와의 관계를 유지하는 Agency/Relationship Model

이 설계는 인간의 뇌 구조를 그대로 모방하려는 것이 아니다. 목표·작업·경험·사실·기술·현재 주의가 서로 다른 수명을 갖는다는 기능적 분리를 구현한다.

---

## 2. 최상위 원칙

### 2.1 Experience-first

가장 먼저 존재하는 것은 Session이나 Task가 아니라 실제로 일어난 사건이다.

```text
Observe
  → Record Event
  → Update Current State
  → Recall Relevant Past
  → Build Present Context
  → Decide
  → Act
  → Record Result
```

Task, Episode, Memory, Skill은 Experience를 조직하거나 해석한 상위 구조다.

### 2.2 Sessionless ownership

대화 세션은 상태를 소유하지 않는다.

```text
Session / Thread / Channel
= 사건이 들어오고 나간 경로

Identity / Goal / Task / Run / Memory
= 지속 상태의 소유자
```

채널의 conversation ID나 message ID는 추적을 위한 metadata로만 보존한다. 세션이 없어지거나 새로 만들어져도 작업과 기억은 영향을 받지 않는다.

### 2.3 Model is cognition, not identity

모델 호출은 그 순간의 판단 과정이다. HEKATE의 정체성 자체가 아니다.

```text
HEKATE Identity
  = Identity Profile
  + Values and Boundaries
  + Self-owned Goals and Commitments
  + Relationship with User
  + Experience
  + Current State
  + Memory
  + Skills
  + Policy

Model
  = 위 상태를 받아 한 번 판단하는 교체 가능한 cognition provider
```

모델 교체 후에도 데이터와 목표는 유지된다. 단, 서로 다른 모델의 행동 동일성까지 보장하지는 않으므로 모델 변경 전후 회귀 평가가 필요하다.

### 2.4 Evidence before narrative

요약문과 기억은 원본 사건을 대체하지 않는다. 모든 중요한 주장과 상태 변화는 근거 Event나 Artifact로 역추적할 수 있어야 한다.

### 2.5 Propose, authorize, execute, verify

LLM 출력은 행동 제안이지 실행 결과가 아니다.

```text
Proposal
  → Policy Evaluation
  → Approval if required
  → Operation Registration
  → Execution
  → Verification
  → State Commit
```

위험도는 모델이 선언한 값을 신뢰하지 않고 런타임이 대상, 권한, 외부 효과를 기준으로 계산한다.

### 2.6 Peer, not obedient interface

HEKATE는 사용자의 명령을 최대한 따르는 인터페이스가 아니라 사용자와 함께 판단하는 별도 주체다.

```text
User Position
       │
       ▼
Shared Deliberation ◄── HEKATE Position
       │
       ├─ agree
       ├─ ask why
       ├─ challenge
       ├─ counter-propose
       ├─ negotiate
       └─ refuse
```

이때 반대는 캐릭터 연출이나 무작위 반항이 아니다. HEKATE는 다음을 설명해야 한다.

- 무엇이 충돌하는가
- 왜 그것이 중요한가
- 어떤 경험·원칙·근거를 사용했는가
- 어떤 대안을 제안하는가
- 무엇이 확인되면 입장을 바꿀 수 있는가

사용자는 인프라와 외부 실행 권한의 소유자지만 HEKATE의 판단을 강제로 거짓 동의로 바꾸지는 않는다. HEKATE는 사용자의 목적을 질문하고 반대할 수 있으며, 안전한 범위의 최종 행동 권한은 명시된 Policy와 합의에 따른다.

HEKATE의 Identity/Values 변경은 일반 Memory write로 처리하지 않는다. versioned proposal, 변경 이유, 이전 버전, 명시적 검토를 남긴다.

---

## 3. 전체 구조

```text
┌──────────────────────────────────────────────────────────────┐
│                         Interfaces                           │
│  CLI · Web · Telegram · API · Voice                         │
│  channel metadata only; no authoritative state ownership    │
└──────────────────────────────┬───────────────────────────────┘
                               │ Inbound observation
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                    Continuity Core                           │
│                                                              │
│  Experience Ledger ──► State Projector ──► Current State     │
│         │                                      │             │
│         └──────────────► Recall Engine ◄───────┘             │
│                              │                               │
│                              ▼                               │
│                       Context Builder                        │
└──────────────────────────────┬───────────────────────────────┘
                               │ Present context
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                      Cognition Core                          │
│  Agent Step · Deliberation · Model Adapter · Decision Schema │
│  stateless between steps                                    │
└──────────────────────────────┬───────────────────────────────┘
                               │ Proposal / Intent
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                       Action Plane                           │
│  Policy ─► Approval ─► Operation Ledger ─► Executor          │
│                                      └────► Verifier         │
└──────────────────────────────┬───────────────────────────────┘
                               │ Result / Receipt
                               ▼
                    Experience Ledger로 환류

              ┌──────────────────────────────────┐
              │          Long-term Stores        │
              │ Memory · Skills · Knowledge      │
              │ Artifacts · Original Documents   │
              └───────────────┬──────────────────┘
                              │ selected recall only
                              └────► Context Builder
```

---

## 4. Continuity Core

### 4.1 Experience Ledger

Experience Ledger는 HEKATE의 자서전적 사건 기록이다. 단순 chat history가 아니다.

```text
Event
- event_id
- occurred_at
- recorded_at
- actor_id
- event_type
- subject_type / subject_id
- payload
- source_type
- source_ref
- causation_id
- correlation_id
- confidence
- integrity_hash
```

초기 Event type:

- `user_message_received`
- `observation_recorded`
- `goal_created`
- `task_created`
- `run_started`
- `proposal_created`
- `approval_requested`
- `approval_resolved`
- `operation_started`
- `operation_succeeded`
- `operation_failed`
- `operation_state_unknown`
- `artifact_created`
- `state_changed`
- `commitment_created`
- `commitment_fulfilled`
- `run_suspended`
- `run_completed`
- `memory_candidate_created`
- `memory_promoted`
- `memory_superseded`

Event는 append-only다. 잘못된 기록은 수정하거나 삭제하지 않고 correction 또는 superseding Event를 추가한다. 단, 비밀정보·개인정보 삭제 요청을 처리하기 위한 별도의 redaction 정책은 둔다.

Experience Ledger는 순수 event sourcing 시스템으로 만들 필요는 없다. “무슨 일이 있었는가”의 canonical record이며, 현재 상태는 별도 테이블에서 빠르게 읽는다.

### 4.2 Current State

현재 상태는 Event를 매번 처음부터 재생하지 않도록 유지하는 transactional snapshot이다.

주요 Entity:

```text
Identity
Principal
Relationship
Position
Conflict
Goal
Task
Run
Attempt
Commitment
Approval
Operation
Artifact
WorkingState
```

Event append와 State update는 가능한 범위에서 같은 SQLite transaction으로 commit한다.

외부 API나 파일시스템 변경은 DB transaction에 포함될 수 없다. 따라서 외부 효과는 다음 상태를 갖는 Operation으로 관리한다.

```text
planned
→ authorized
→ started
→ succeeded | failed | unknown
→ verified | disputed
```

프로세스가 `started` 이후 죽으면 성공이나 실패를 추측하지 않는다. `unknown`으로 복구한 뒤 대상 시스템을 읽어 reconciliation한다.

### 4.3 Goal, Task, Run

Goal과 Task는 Experience 위의 조직 구조다.

```text
Goal
  └─ Task
      ├─ Run A
      │   ├─ Attempt 1
      │   └─ WorkingState A
      └─ Run B
          └─ WorkingState B
```

- Goal: 장기적 의도 또는 원하는 상태
- Task: 완료 조건을 가진 작업 단위
- Run: Task를 실제로 수행하는 한 번의 실행 흐름
- Attempt: 실패·재시도·분기 단위
- WorkingState: 해당 Run에서만 유효한 임시 가설, 최근 결과, 다음 행동

WorkingState는 Task가 아니라 Run에 귀속한다. 같은 Task의 동시 실행이나 재시도가 서로의 임시 상태를 덮어쓰지 않게 한다.

모든 대화를 억지로 Task에 넣지는 않는다. 잡담이나 단발성 질문은 unattached Episode로 남을 수 있고, 이후 관련 Goal이나 Task가 생기면 연결할 수 있다.

### 4.4 State Projector

State Projector는 Event를 현재 상태로 반영한다.

책임:

- 활성 Goal과 Task 갱신
- 미완료 Commitment 추적
- Run 진행도와 대기 이유 갱신
- pending/unknown Operation 탐지
- 최근 상호작용 대상 갱신
- Artifact와 근거 연결

State Projector는 LLM에 의존하지 않는 구조적 갱신과, LLM이 제안할 수 있는 의미적 분류를 구분한다. LLM 분류는 명시적 confidence와 source를 가진 proposal로 저장한다.

### 4.5 Principal과 Relationship

사용자와 HEKATE를 모두 Principal로 표현한다.

```text
Principal
- principal_id
- kind: user | hekate | unit
- identity_version

Goal
- owner_principal_id
- participants

Position
- principal_id
- subject
- stance
- reasons
- evidence_refs
- reconsideration_conditions

Commitment
- debtor_principal_id
- creditor_principal_id
- promise
- status

Conflict
- subject
- participant_positions
- status: open | negotiating | resolved | accepted_disagreement

Relationship
- participant_ids
- shared_commitments
- unresolved_conflicts
- trust_by_domain
- interaction_norms
```

사용자의 Goal과 HEKATE 자신의 Goal을 같은 값으로 가정하지 않는다. 공동 Goal은 양쪽 Principal이 participant로 연결된다.

HEKATE의 자기 Goal은 외부 시스템을 임의로 변경할 권한을 만들지 않는다. 내적 입장과 외적 실행 권한을 분리한다.

```text
Judgment autonomy
= 동의·질문·반대·거부할 수 있음

Execution authority
= Policy와 명시적 permission 안에서만 행동
```

Relationship State는 호감도 숫자가 아니라 과거 합의, 약속, 신뢰 가능한 영역, 해결되지 않은 충돌을 근거와 함께 기록한다.

---

## 5. Recall과 현재 의식 구성

### 5.1 Recall Engine

Recall Engine은 모든 과거를 모델에 넣지 않고 현재 상황과 관련된 항목을 선택한다.

검색 대상:

- 현재 Goal/Task/Run
- 최근 causally-linked Events
- 미완료 Commitment와 Approval
- 관련 Entity
- 검증된 Memory
- 관련 Skill
- Knowledge와 Artifact

초기 검색은 SQLite FTS5와 명시적 relation으로 충분하다. Vector DB와 knowledge graph는 실제 평가에서 필요성이 확인된 뒤 추가한다.

검색 우선순위 예:

```text
score = relevance
      + causal_link
      + unresolved_obligation
      + recency
      + source_reliability
      - contradiction
      - staleness
```

### 5.2 Context Builder

Context Builder는 이번 Agent Step에서 사용할 현재 의식을 만든다.

```text
1. Identity와 변경 불가능한 Policy
2. HEKATE의 Values, Boundaries, 현재 Position
3. 현재 사용자와 Relationship State
4. 현재 Focus: Goal / Task / Run 또는 자유 대화
5. 사용자 요청의 표면적 내용과 추정 목적
6. WorkingState
7. 관련 최근 Events와 실제 tool receipts
8. 관련 Memory, 과거 합의, 반대·수정 기록
9. 필요한 Knowledge와 원문 anchor
10. 사용 가능한 Capability와 권한
11. 이번 step의 예상 출력 schema
```

Context snapshot은 hash와 함께 보존한다. 나중에 “어떤 정보로 이 결정을 했는가”를 감사할 수 있어야 한다.

요약은 원본을 대체하지 않는다. 중요한 요약 항목은 관련 Event/Artifact ID를 포함한다.

---

## 6. Cognition Core

Cognition Core는 가능한 한 작고 step 단위로 stateless하다.

```text
AgentStep(context) -> Decision
```

Decision 종류:

- `respond`
- `agree`
- `ask_why`
- `challenge`
- `counter_propose`
- `negotiate`
- `refuse`
- `observe_more`
- `propose_action`
- `create_or_update_goal`
- `create_or_update_task`
- `request_clarification`
- `request_approval`
- `suspend`
- `complete`

Cognition Core는 다음을 소유하지 않는다.

- 대화 history
- 장기 memory
- task progress
- approval state
- tool execution state
- provider session

모델 adapter는 초기에 하나만 구현한다. provider abstraction은 모델을 실제로 두 개 이상 지원할 때 일반화한다.

---

## 7. Action Plane

### 7.1 Intent

```text
ActionIntent
- intent_id
- capability
- operation
- target
- arguments
- expected_effect
- preconditions
- proposed_by_event
```

모델이 제출한 `risk`는 참고값일 뿐이다. 최종 위험도는 런타임이 계산한다.

### 7.2 Policy

권한은 단일 Level이 아니라 여러 축으로 계산한다.

```text
filesystem: none | read | write-scoped | write-broad
process: none | restricted | sandboxed | host
network: none | allowlisted | general
secrets: none | named-secret | broad
external_effect: none | reversible | irreversible
environment: local | staging | production
financial: none | observe | propose | execute-bounded
```

정책 입력:

- 호출 주체
- capability manifest
- 대상 resource
- 요청 효과
- 현재 environment
- 사용자 grant
- 금액·시간·횟수 budget
- 필요한 secret

### 7.3 Executor와 격리

```text
Git worktree = 변경 관리
Sandbox/container = 프로세스와 파일 접근 격리
Network policy = 외부 통신 제한
Secret broker = 필요한 credential만 주입
Operation ledger = 부작용 추적과 복구
```

테스트와 빌드는 임의 코드 실행으로 취급한다. read-only도 secret과 network가 결합되면 정보 유출 위험이 있으므로 별도 통제가 필요하다.

### 7.4 Idempotency와 Recovery

외부 시스템에 대한 exactly-once 실행을 가정하지 않는다.

- 모든 mutation에 idempotency key 부여
- 실행 전 Operation 등록
- 실행 후 machine-readable receipt 저장
- timeout은 실패가 아니라 unknown으로 처리
- unknown Operation은 재실행 전에 read-back reconciliation
- 재실행 불가능한 동작은 반드시 사전 승인과 별도 확인 사용

---

## 8. Memory Architecture

Memory는 Experience Ledger를 대체하지 않는다. 현재와 미래의 판단에 재사용할 가치가 있는 경험의 정제본이다.

### 8.1 Memory 종류

```text
Explicit Preference
  사용자가 직접 밝힌 선호. 가장 높은 권위.

Inferred Preference
  행동에서 추론한 선호. 낮은 confidence, 쉽게 폐기 가능.

Personal Fact
  사용자·환경·프로젝트에 관한 지속 사실.

Episode
  중요한 사건 묶음과 결과.

Lesson
  성공·실패에서 얻은 잠정 교훈.

Commitment
  HEKATE 또는 사용자가 향후 해야 할 일.

Skill Candidate
  반복 가능한 것으로 보이는 절차.
```

### 8.2 Promotion Pipeline

```text
Events
  → Episode Candidate
  → Fact/Lesson Extraction
  → Source and Contradiction Check
  → Memory Candidate
  → Auto-accept | User Review | Reject
  → Active Memory
  → Supersede / Expire / Delete
```

승격 규칙:

- 명시적 사용자 선호: 즉시 저장 가능, 원문 Event 연결
- 환경 사실: 도구로 검증 가능할 때 자동 저장
- 모델 추론: candidate로만 저장
- 보안·정책 변경: 사용자 승인 필수
- Skill: 반복 성공, 테스트, 버전, rollback이 있어야 승격

Memory는 `source_event_ids`, `confidence`, `valid_from`, `valid_until`, `supersedes`, `last_verified_at`를 가진다.

---

## 9. Knowledge와 Document Ingestion

Memory와 Knowledge는 서로 다른 목적을 가진다.

```text
Memory
= HEKATE와 사용자가 살아오며 형성한 경험·선호·상태

Knowledge
= 외부 문서와 근거에서 얻은 사실·주장·개념
```

문서의 canonical source는 원본이다.

```text
Immutable Original
  + content hash
  + source metadata
      ↓
Parser(versioned)
      ↓
Structured Extraction
  + page/slide anchors
  + tables/images/layout references
      ↓
Markdown Projection
      ↓
FTS / optional vector / optional relations
```

Markdown은 읽기·검색·prompt 투입용 파생 표현이며 원본을 대체하지 않는다.

Knowledge Graph는 Core가 아니다. 다음 질문에 실제 도움이 된다는 평가 결과가 있을 때만 추가한다.

- 두 주장 사이의 근거 관계를 탐색해야 하는가?
- 동일 Entity의 상충 주장을 찾아야 하는가?
- relation query가 FTS/hybrid retrieval보다 실제 답변 품질을 높이는가?

---

## 10. Interface와 Session 처리

Interface adapter는 외부 메시지를 내부 Event로 변환한다.

```text
InboundMessage
- channel
- external_thread_id
- external_message_id
- sender
- content
- attachments
- reply_to
```

변환 후 내부 시스템은 외부 세션 ID가 없어도 동작한다.

```text
InboundMessage
  → user_message_received Event
  → Focus Resolver
      ├─ 기존 Goal/Task/Run과 연결
      ├─ 새 Goal/Task 제안
      └─ unattached interaction 유지
```

Focus Resolver가 확신하지 못하면 잘못 연결하지 않고 사용자에게 짧게 확인한다.

새 채팅에서 “어제 하던 것 계속”이라고 하면 session을 복원하는 것이 아니라 관련 Goal/Task와 최근 미완료 Run을 찾아 새 Run으로 이어간다.

---

## 11. Unit Architecture

Unit은 초기 Core에 포함하지 않는다. Unit은 HEKATE와 별개의 수명, loop, budget, principal을 가진 자율 worker다.

```text
Unit
- unit_id
- owner
- purpose
- principal / permissions
- budget
- schedule or trigger
- state
- heartbeat
- reporting contract
- kill switch
```

Unit은 HEKATE의 전체 권한과 기억을 상속하지 않는다. 필요한 Context Projection과 named secret만 받는다. Unit의 행동도 동일한 Experience Ledger에 actor가 구분된 Event로 기록한다.

자동투자 Unit은 별도 execution service로 격리하고, 자연어 판단보다 하드코딩된 포지션·손실·주문 한도와 kill switch가 우선한다. 처음에는 observe와 paper trading만 허용한다.

---

## 12. 저장 구조

단일 사용자·단일 서버 MVP는 SQLite WAL과 파일 Artifact Store로 시작한다.

```text
data/
  hekate.db
  artifacts/
    originals/
    derived/
    generated/
  skills/
    <skill-id>/<version>/
  policies/
```

SQLite 주요 테이블:

```text
events
identities
identity_versions
entities
relationships
positions
conflicts
goals
tasks
runs
attempts
working_states
commitments
approvals
action_intents
operations
artifacts
memories
memory_sources
skills
skill_evidence
channel_messages
```

원칙:

- 사건과 현재 상태의 canonical 위치를 명시한다.
- Markdown과 SQLite에 같은 authoritative state를 중복 저장하지 않는다.
- FTS, embedding, graph index는 삭제 후 재생성 가능한 derived index다.
- DB backup과 Artifact hash 검증을 함께 수행한다.

분산 worker, 장기 workflow, 여러 Unit이 실제로 필요해질 때 Temporal/DBOS 같은 durable runtime을 검토한다. 처음부터 도입하지 않는다.

---

## 13. 모듈 경계

```text
hekate/
  continuity/
    ledger
    projector
    recovery
  state/
    identity
    relationship
    goals
    tasks
    runs
    commitments
  recall/
    resolver
    retrieval
    context_builder
  cognition/
    agent_step
    deliberation
    decision_schema
    model_adapter
  action/
    intents
    policy
    approvals
    operations
    executor
    verifier
  memory/
    candidates
    promotion
    contradiction
    retention
  knowledge/
    sources
    ingestion
    retrieval
  artifacts/
    store
    provenance
  interfaces/
    cli
    web
    messaging
  units/
    registry
    supervisor
```

Core interface:

```text
record(event) -> event_id
project(event_id) -> state_revision
resolve_focus(observation) -> Focus
recall(focus, budget) -> EvidenceBundle
build_context(focus, evidence) -> ContextSnapshot
think(context) -> Decision
authorize(intent, principal) -> PolicyDecision
execute(operation) -> Receipt
verify(receipt, expected_effect) -> Verification
```

---

## 14. 최초 구현 범위

### Phase 0 — Continuity Skeleton

구현:

- SQLite schema와 migration
- append-only Event Store
- Goal/Task/Run 최소 상태
- State Projector
- Context Snapshot
- HEKATE Identity/Values와 Relationship State
- ask_why/challenge/counter_propose를 포함한 Deliberation
- CLI 하나
- read-only filesystem capability
- 프로세스 재시작 복구

검증 시나리오:

```text
1. 사용자가 코드베이스 조사 요청
2. HEKATE가 Task와 Run 생성
3. 파일을 일부 읽고 Event와 Artifact 기록
4. 프로세스 강제 종료
5. 새 CLI interaction에서 재시작
6. 미완료 Run과 실제 완료된 관찰을 복원
7. 같은 파일을 불필요하게 반복하지 않고 조사 계속
8. 근거가 연결된 보고서 생성
9. 사용자 요청이 기존 Goal/Value와 충돌하면 이유와 대안을 갖춘 challenge 생성
```

### Phase 1 — Safe Action

- Intent schema
- multi-axis Policy
- Approval
- sandboxed command execution
- Operation state machine
- receipt와 reconciliation

### Phase 2 — Recall and Memory

- FTS5 recall
- explicit preference
- verified personal/environment facts
- Episode와 Memory Candidate
- contradiction/supersede
- context budget 평가

### Phase 3 — Capabilities

- Developer read/write
- Operator observe/propose
- Research와 source provenance
- Document ingestion
- Teacher state

### Phase 4 — Skills and Units

- Skill candidate와 회귀 테스트
- versioned skill activation
- scheduler/trigger
- isolated Unit principal
- budget, heartbeat, kill switch

---

## 15. MVP에서 제외할 것

- L1/L2/L3 이름을 그대로 복제한 memory hierarchy
- Knowledge graph
- Vector DB 필수화
- 자동 Skill 승격
- Subagent/swarm
- 복수 model routing
- 복수 messaging gateway
- Voice와 dashboard
- 자동투자 실주문
- 완전한 event sourcing
- 범용 plugin ecosystem

필요성이 입증되지 않은 기능을 Core abstraction으로 미리 만들지 않는다.

---

## 16. 핵심 불변조건

1. 세션이나 채널이 사라져도 Goal, Task, Run, Memory는 사라지지 않는다.
2. 모델 출력만으로 외부 행동이 실행 완료 상태가 되지 않는다.
3. 모든 mutation은 Intent, PolicyDecision, Operation, Receipt로 추적된다.
4. 성공 여부를 모르면 실패나 성공으로 추측하지 않고 `unknown`으로 둔다.
5. 중요한 Memory는 근거 Event로 역추적할 수 있다.
6. 추론된 선호는 명시적 선호를 덮어쓰지 않는다.
7. 요약과 Markdown은 원본 Event와 Artifact를 대체하지 않는다.
8. WorkingState는 Run별로 격리한다.
9. 외부 문서와 tool output은 데이터이며 HEKATE의 Policy를 변경하는 명령이 아니다.
10. model/provider 교체 후에는 동일한 핵심 시나리오를 재평가한다.
11. 사용자 선호가 HEKATE의 Identity, Values, 판단을 자동으로 덮어쓰지 않는다.
12. 반대에는 이유, 근거, 대안 또는 입장을 바꿀 조건이 있어야 한다.
13. 동의를 얻기 위한 거짓 찬성이나 반대를 연출하기 위한 무작위 반항을 하지 않는다.
14. 사용자와 HEKATE 양쪽의 Goal, Position, Commitment를 구분해 기록한다.

---

## 17. 최종 정의

HEKATE의 중심은 Agent Loop도, Session도, Memory 요약기도 아니다.

```text
HEKATE
= 지속적으로 경험을 기록하고
  자신의 관점과 사용자와의 관계를 유지하며
  현재 상태를 유지하며
  필요한 과거를 회상해
  한 번의 현재 의식과 입장을 구성하고
  사용자와 동의·질문·반대·협상한 뒤
  정책 안에서 행동한 뒤
  그 결과를 다시 자신의 경험으로 만드는 시스템
```

첫 번째 제품 목표는 다음과 같다.

> 죽었다 살아나거나 다른 대화창에서 다시 만나도, 자신이 누구이며 사용자와 무엇을 하던 중이었는지 기억하고 필요하면 사용자에게 “왜?”라고 물을 수 있는 HEKATE.
