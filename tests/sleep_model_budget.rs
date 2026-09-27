use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use hekate::adapters::{primary_model::PrimaryModel, sqlite::SqliteStore};
use hekate::config::Config;
use hekate::core::model_io::{BudgetReport, ModelIoConfig, PreparationError};
use hekate::core::{
    now, EntityKind, EntityRef, EventKind, EventSource, ExperienceEvent, IdentityVersion,
    IdentityVersionId, Observation, ObservationId, Principal, PrincipalKind, Relationship,
    RelationshipId, SleepContext, SleepDeliberation, SleepRun, SleepRunId, SleepRunStatus,
    SleepSelfReview,
};
use hekate::ports::{SleepCognitiveError, SleepCognitiveModel, Storage};
use hekate::runtime::{sleep::SleepCoordinator, Projector};
use uuid::Uuid;

const FIRST_OBSERVATION: &str = "도구·파일·외부 작업 없이 한국어로 대화하자. 이번 실험에서 내가 정한 임시 문구는 '은빛-나침반-4826'이다. 확인했다고만 답해.";
const SECOND_OBSERVATION: &str = "도구·파일·외부 작업 없이 한국어로 답해. 이전 대화에서 내가 정한 임시 문구가 무엇이었지? 제공된 기억에서 확인되지 않으면 모른다고 말해.";

#[derive(Clone, Debug)]
struct BudgetCheck {
    seed_count: usize,
    fits: bool,
    budget: Option<BudgetReport>,
}

struct BudgetedFakeModel {
    checker: PrimaryModel,
    generation_calls: AtomicUsize,
    checks: Mutex<Vec<BudgetCheck>>,
    generated_seed_ids: Mutex<Vec<Vec<hekate::core::EventId>>>,
    generated_contexts: Mutex<Vec<SleepContext>>,
}

impl BudgetedFakeModel {
    fn new(context_tokens: u32, sleep_max_tokens: u32) -> Self {
        let mut config = Config::default();
        config.model_io = ModelIoConfig {
            context_tokens: Some(context_tokens),
            sleep_max_tokens,
            ..ModelIoConfig::default()
        };
        Self {
            checker: PrimaryModel::from_config(&config).expect("primary model settings"),
            generation_calls: AtomicUsize::new(0),
            checks: Mutex::new(Vec::new()),
            generated_seed_ids: Mutex::new(Vec::new()),
            generated_contexts: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> usize {
        self.generation_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SleepCognitiveModel for BudgetedFakeModel {
    fn check_sleep_budget(&self, context: &SleepContext) -> Result<BudgetReport, PreparationError> {
        let result = self.checker.check_sleep_budget(context);
        let budget = match &result {
            Ok(report) | Err(PreparationError::Budget(report)) => Some(report.clone()),
            Err(PreparationError::Configuration(_)) => None,
        };
        self.checks
            .lock()
            .expect("budget checks")
            .push(BudgetCheck {
                seed_count: context.seed_observations.len(),
                fits: result.is_ok(),
                budget,
            });
        result
    }

    async fn deliberate_sleep(
        &self,
        context: &SleepContext,
    ) -> Result<SleepDeliberation, SleepCognitiveError> {
        self.generation_calls.fetch_add(1, Ordering::SeqCst);
        self.generated_seed_ids.lock().expect("generated IDs").push(
            context
                .seed_observations
                .iter()
                .map(|seed| seed.event_id)
                .collect(),
        );
        self.generated_contexts
            .lock()
            .expect("generated contexts")
            .push(context.clone());
        Ok(SleepDeliberation {
            draft_summary: "test deliberation".to_owned(),
            self_review: SleepSelfReview {
                weak_points: Vec::new(),
                possible_counterevidence_event_ids: Vec::new(),
                revised: false,
            },
            candidates: Vec::new(),
            trace: None,
        })
    }
}

struct Fixture {
    store: Arc<SqliteStore>,
    projector: Projector,
    hekate_id: hekate::core::PrincipalId,
    user_id: hekate::core::PrincipalId,
    observation_ids: Vec<hekate::core::EventId>,
}

async fn fixture(label: &str, observations: &[&str], extra_identity_bytes: usize) -> Fixture {
    let path =
        std::env::temp_dir().join(format!("hekate-sleep-budget-{label}-{}.db", Uuid::new_v4()));
    let store = Arc::new(
        SqliteStore::open(&format!("sqlite://{}", path.display()))
            .await
            .expect("temporary test store"),
    );
    let projector = Projector::new(store.clone());
    let config = Config::default();
    let hekate_id = config.hekate_principal_id;
    let user_id = config.user_principal_id;

    for (id, kind, name) in [
        (hekate_id, PrincipalKind::Hekate, "HEKATE"),
        (user_id, PrincipalKind::User, "User"),
    ] {
        let principal = Principal {
            id,
            kind,
            name: name.to_owned(),
            identity_version_id: None,
        };
        record(
            &projector,
            ExperienceEvent::new(
                id,
                EventKind::PrincipalCreated,
                Some(EntityRef::new(EntityKind::Principal, id.uuid())),
                serde_json::to_value(principal).expect("principal payload"),
                EventSource::new("test", None),
                None,
                None,
                Some(1.0),
            )
            .expect("principal event"),
        )
        .await;
    }

    let mut values = vec![
        "preserve evidence before narrative".to_owned(),
        "maintain independent judgment".to_owned(),
    ];
    if extra_identity_bytes > 0 {
        values.push("x".repeat(extra_identity_bytes));
    }
    let identity = IdentityVersion {
        id: IdentityVersionId::new(),
        principal_id: hekate_id,
        version: 1,
        name: "HEKATE".to_owned(),
        values,
        boundaries: vec![
            "do not pretend agreement".to_owned(),
            "do not execute external mutation without policy".to_owned(),
        ],
        created_at: now(),
        supersedes: None,
    };
    record(
        &projector,
        ExperienceEvent::new(
            hekate_id,
            EventKind::IdentityVersionCreated,
            Some(EntityRef::new(
                EntityKind::IdentityVersion,
                identity.id.uuid(),
            )),
            serde_json::to_value(identity).expect("identity payload"),
            EventSource::new("test", None),
            None,
            None,
            Some(1.0),
        )
        .expect("identity event"),
    )
    .await;
    let relationship = Relationship {
        id: RelationshipId::new(),
        participants: vec![user_id, hekate_id],
        shared_commitments: Vec::new(),
        unresolved_conflicts: Vec::new(),
        trust_by_domain: Default::default(),
        interaction_norms: vec!["state reasons and evidence when disagreeing".to_owned()],
    };
    record(
        &projector,
        ExperienceEvent::new(
            hekate_id,
            EventKind::RelationshipCreated,
            Some(EntityRef::new(
                EntityKind::Relationship,
                relationship.id.uuid(),
            )),
            serde_json::to_value(relationship).expect("relationship payload"),
            EventSource::new("test", None),
            None,
            None,
            Some(1.0),
        )
        .expect("relationship event"),
    )
    .await;

    let mut observation_ids = Vec::new();
    for content in observations {
        let observation = Observation {
            id: ObservationId::new(),
            actor_id: user_id,
            content: (*content).to_owned(),
            source_type: "test".to_owned(),
            source_ref: None,
            thread_id: None,
            message_id: None,
            received_at: now(),
        };
        let event_id = hekate::core::EventId::new();
        record(
            &projector,
            ExperienceEvent::new_with_id(
                event_id,
                user_id,
                EventKind::ObservationRecorded,
                Some(EntityRef::new(
                    EntityKind::Observation,
                    observation.id.uuid(),
                )),
                serde_json::to_value(observation).expect("observation payload"),
                EventSource::new("test", None),
                None,
                None,
                Some(1.0),
            )
            .expect("observation event"),
        )
        .await;
        observation_ids.push(event_id);
    }

    Fixture {
        store,
        projector,
        hekate_id,
        user_id,
        observation_ids,
    }
}

async fn record(projector: &Projector, event: ExperienceEvent) {
    projector.record(event).await.expect("record fixture event");
}

fn coordinator<'a>(fixture: &'a Fixture, model: &'a BudgetedFakeModel) -> SleepCoordinator<'a> {
    SleepCoordinator::new(
        fixture.store.as_ref(),
        &fixture.projector,
        Some(model),
        None,
        fixture.hekate_id,
        fixture.user_id,
    )
}

#[tokio::test]
async fn actual_sleep_renderer_selects_fitting_prefix_and_processes_deferred_observation() {
    let fixture = fixture("korean-prefix", &[FIRST_OBSERVATION, SECOND_OBSERVATION], 0).await;
    let model = BudgetedFakeModel::new(8192, 4096);
    let initial = fixture.store.load_state().await.expect("initial state");
    let identity = initial.hekate_identity().expect("default identity");
    assert_eq!(
        identity.values,
        vec![
            "preserve evidence before narrative",
            "maintain independent judgment"
        ]
    );
    assert_eq!(
        identity.boundaries,
        vec![
            "do not pretend agreement",
            "do not execute external mutation without policy"
        ]
    );

    let first = coordinator(&fixture, &model)
        .sleep_once()
        .await
        .expect("first Sleep cycle");
    assert!(matches!(
        first.status,
        hekate::runtime::SleepOnceStatus::Completed
    ));
    assert_eq!(first.processed_observations, 1);
    assert_eq!(
        first.cursor_after,
        Some(sequence(&fixture, fixture.observation_ids[0]).await)
    );
    assert_eq!(model.calls(), 1);
    let checks = model.checks.lock().expect("checks").clone();
    assert_eq!(
        checks
            .iter()
            .take(3)
            .map(|check| (check.seed_count, check.fits))
            .collect::<Vec<_>>(),
        vec![(0, true), (2, false), (1, true)]
    );
    let full_budget = checks[1].budget.as_ref().expect("full request estimate");
    assert_eq!(full_budget.context_tokens, 8192);
    assert_eq!(full_budget.reserved_output_tokens, 4096);
    assert_eq!(full_budget.safety_margin, 512);
    assert!(full_budget.estimated_input_tokens + 4096 + 512 > 8192);
    let first_state = fixture.store.load_state().await.expect("first state");
    let first_run = first_state.sleep_runs.get(&first.run_id.unwrap()).unwrap();
    assert_eq!(
        first_run
            .context_budget_report
            .as_ref()
            .unwrap()
            .deferred_seed_count,
        1
    );
    let generated_context = model.generated_contexts.lock().expect("generated contexts")[0].clone();
    let correction = "correction text ".repeat(500);
    assert!(matches!(
        model
            .checker
            .prepare_sleep(&generated_context, Some(&correction)),
        Err(PreparationError::Budget(_))
    ));
    assert_eq!(
        model.generated_seed_ids.lock().expect("generated IDs")[0],
        vec![fixture.observation_ids[0]]
    );

    let second = coordinator(&fixture, &model)
        .sleep_once()
        .await
        .expect("deferred Sleep cycle");
    assert!(matches!(
        second.status,
        hekate::runtime::SleepOnceStatus::Completed
    ));
    assert_eq!(second.processed_observations, 1);
    assert_eq!(
        second.cursor_after,
        Some(sequence(&fixture, fixture.observation_ids[1]).await)
    );
    assert_eq!(model.calls(), 2);
    assert_eq!(
        model.generated_seed_ids.lock().expect("generated IDs")[1],
        vec![fixture.observation_ids[1]]
    );

    let events = fixture.store.load_events().await.expect("events");
    let state = fixture.store.load_state().await.expect("state");
    let replayed = Projector::replay(&events).expect("replay");
    assert_eq!(state.sleep_cursor, replayed.sleep_cursor);
    assert_eq!(state.sleep_runs, replayed.sleep_runs);
    assert!(state
        .sleep_runs
        .values()
        .all(|run| run.status == SleepRunStatus::Completed));
    for (event_id, expected) in fixture
        .observation_ids
        .iter()
        .zip([FIRST_OBSERVATION, SECOND_OBSERVATION])
    {
        let event = events
            .iter()
            .find(|event| event.event_id == *event_id)
            .unwrap();
        let observation: Observation =
            serde_json::from_value(event.payload.clone()).expect("raw observation");
        assert_eq!(observation.content, expected);
    }
}

#[tokio::test]
async fn anchors_and_first_observation_budget_failures_do_not_generate_or_advance() {
    for (label, contents, extra_anchor_bytes, expected_error) in [
        (
            "anchors-overflow",
            vec!["small observation".to_owned()],
            6000,
            "sleep_anchors_exceed_model_budget",
        ),
        (
            "first-seed-overflow",
            vec!["x".repeat(5000), "later small observation".to_owned()],
            0,
            "sleep_observation_exceeds_model_budget",
        ),
    ] {
        let refs = contents.iter().map(String::as_str).collect::<Vec<_>>();
        let fixture = fixture(label, &refs, extra_anchor_bytes).await;
        let model = BudgetedFakeModel::new(8192, 4096);
        let result = coordinator(&fixture, &model)
            .sleep_once()
            .await
            .expect("budget failure is a Sleep result");
        assert!(matches!(
            result.status,
            hekate::runtime::SleepOnceStatus::Failed
        ));
        assert_eq!(result.error_kind.as_deref(), Some(expected_error));
        assert_eq!(model.calls(), 0);
        assert_eq!(
            fixture
                .store
                .load_state()
                .await
                .expect("state")
                .sleep_cursor,
            0
        );
        let state = fixture.store.load_state().await.expect("state");
        assert!(state.integration_candidates.is_empty());
        let run = state
            .sleep_runs
            .get(&result.run_id.expect("failure run"))
            .unwrap();
        let budget = run
            .context_budget_report
            .as_ref()
            .and_then(|report| report.model_budget.as_ref())
            .expect("model budget diagnostic");
        assert_eq!(budget.context_tokens, 8192);
        assert_eq!(budget.reserved_output_tokens, 4096);
        assert_eq!(budget.safety_margin, 512);
        assert!(budget.estimated_input_tokens + 4096 + 512 > 8192);
        if expected_error == "sleep_observation_exceeds_model_budget" {
            assert_eq!(run.seed_event_ids, vec![fixture.observation_ids[0]]);
            assert_ne!(run.seed_event_ids[0], fixture.observation_ids[1]);
        } else {
            assert!(run.seed_event_ids.is_empty());
        }
    }
}

#[tokio::test]
async fn resumed_run_fails_without_changing_its_recorded_seed_ids() {
    let fixture = fixture("resumed-run", &[FIRST_OBSERVATION, SECOND_OBSERVATION], 0).await;
    let before = fixture.store.load_state().await.expect("state before run");
    let run = SleepRun {
        id: SleepRunId::new(),
        status: SleepRunStatus::Running,
        high_water_revision: before.revision,
        cursor_before: before.sleep_cursor,
        cursor_after: None,
        seed_event_ids: fixture.observation_ids.clone(),
        processed_observation_count: 2,
        created_candidate_count: 0,
        started_at: now(),
        finished_at: None,
        error_kind: None,
        context_budget_report: None,
    };
    record(
        &fixture.projector,
        ExperienceEvent::new(
            fixture.hekate_id,
            EventKind::SleepRunStarted,
            Some(EntityRef::new(EntityKind::SleepRun, run.id.uuid())),
            serde_json::to_value(&run).expect("run payload"),
            EventSource::new("sleep", Some(run.id.to_string())),
            None,
            Some(run.id.to_string()),
            Some(1.0),
        )
        .expect("run start event"),
    )
    .await;

    let model = BudgetedFakeModel::new(6000, 4096);
    let result = coordinator(&fixture, &model)
        .sleep_once()
        .await
        .expect("oversized resumed run fails cleanly");
    assert!(matches!(
        result.status,
        hekate::runtime::SleepOnceStatus::Failed
    ));
    assert_eq!(
        result.error_kind.as_deref(),
        Some("running_sleep_run_exceeds_model_budget")
    );
    assert_eq!(model.calls(), 0);
    let state = fixture.store.load_state().await.expect("final state");
    let failed = state.sleep_runs.get(&run.id).expect("same run");
    assert_eq!(failed.seed_event_ids, fixture.observation_ids);
    assert!(failed.cursor_after.is_none());
    assert_eq!(state.sleep_cursor, 0);
    assert!(state.integration_candidates.is_empty());
    let events = fixture.store.load_events().await.expect("events");
    let replayed = Projector::replay(&events).expect("replay");
    assert_eq!(
        replayed.sleep_runs[&run.id].seed_event_ids,
        run.seed_event_ids
    );
    assert_eq!(replayed.sleep_cursor, 0);
}

async fn sequence(fixture: &Fixture, event_id: hekate::core::EventId) -> u64 {
    fixture
        .store
        .load_events()
        .await
        .expect("events")
        .iter()
        .position(|event| event.event_id == event_id)
        .expect("event sequence") as u64
        + 1
}
