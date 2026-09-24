use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::adapters::sqlite::{SqliteEmbeddingStore, SqliteStore};
use crate::bootstrap::{
    build_embedding_provider, build_engine, build_response_profile, embedding_space,
};
use crate::config::Config;
use crate::core::{
    ApprovalId, InteractionResult, MemoryCandidateId, MemoryKind, Observation, OperationId,
};
use crate::ports::EmbeddingStore;
use crate::runtime::embedding_indexer::{embedding_documents, EmbeddingIndexer};
use crate::runtime::engine::Engine;
use crate::runtime::sleep_worker::{SleepWorker, SleepWorkerConfig};

mod repl;

#[derive(Debug, Parser)]
#[command(name = "hekate", about = "HEKATE continuity runtime")]
pub struct Cli {
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub database_url: Option<String>,
    #[arg(long)]
    pub workspace_root: Option<PathBuf>,
    #[arg(long)]
    pub thread_id: Option<String>,
    #[arg(long)]
    pub chat: bool,
    #[arg(long)]
    pub message_id: Option<String>,
    #[arg(long)]
    pub inspect: bool,
    #[arg(long)]
    pub identity: bool,
    #[arg(long)]
    pub positions: bool,
    #[arg(long)]
    pub conflicts: bool,
    #[arg(long)]
    pub goals: bool,
    #[arg(long)]
    pub tasks: bool,
    #[arg(long, value_name = "TASK_ID")]
    pub completion_status: Option<String>,
    #[arg(long, value_name = "TASK_ID")]
    pub complete_task: Option<String>,
    #[arg(long)]
    pub completion_claims: bool,
    #[arg(long)]
    pub runs: bool,
    #[arg(long)]
    pub pending: bool,
    #[arg(long)]
    pub resume: bool,
    #[arg(long)]
    pub memory_list: bool,
    #[arg(long)]
    pub response_profile: bool,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub embedding_status: bool,
    #[arg(long)]
    pub embedding_index_once: bool,
    #[arg(long)]
    pub embedding_search: Option<String>,
    #[arg(long)]
    pub sleep_once: bool,
    #[arg(long)]
    pub sleep_status: bool,
    #[arg(long)]
    pub integration_candidates: bool,
    #[arg(long, value_name = "CANDIDATE_ID")]
    pub verify_integration: Option<String>,
    #[arg(long, value_name = "CANDIDATE_ID")]
    pub reject_integration: Option<String>,
    #[arg(long, value_name = "CANDIDATE_ID")]
    pub integrate_memory: Option<String>,
    #[arg(long, value_name = "CANDIDATE_ID")]
    pub integrate_position: Option<String>,
    #[arg(long)]
    pub integration_reason: Option<String>,
    #[arg(long)]
    pub sleep_worker: bool,
    #[arg(long)]
    pub memory_candidate: Option<String>,
    #[arg(long)]
    pub memory_kind: Option<String>,
    #[arg(long)]
    pub memory_confidence: Option<u8>,
    #[arg(long)]
    pub promote_memory: Option<String>,
    #[arg(long)]
    pub reject_memory: Option<String>,
    #[arg(long)]
    pub supersede_memory: Option<String>,
    #[arg(long)]
    pub expire_memory: Option<String>,
    #[arg(long)]
    pub approve: Option<String>,
    #[arg(long)]
    pub deny: Option<String>,
    #[arg(long)]
    pub execute: Option<String>,
    #[arg(value_name = "MESSAGE")]
    pub message: Vec<String>,
}

pub async fn run(cli: Cli) -> anyhow::Result<()> {
    if cli.chat {
        validate_chat_args(&cli)?;
    }
    validate_sleep_args(&cli)?;
    validate_integration_args(&cli)?;
    if cli.sleep_worker {
        validate_sleep_worker_args(&cli)?;
    }
    validate_completion_args(&cli)?;
    let mut config = Config::load(cli.config.as_deref())?;
    if let Some(database_url) = cli.database_url.clone() {
        config.database_url = database_url;
    }
    if let Some(workspace_root) = cli.workspace_root.clone() {
        config.workspace_root = workspace_root;
    }
    if cli.response_profile {
        validate_response_profile_args(&cli)?;
        let resolution = build_response_profile(&config).await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&response_profile_json(&resolution))?
        );
        return Ok(());
    }
    if cli.embedding_status || cli.embedding_index_once || cli.embedding_search.is_some() {
        return run_embedding_command(&config, &cli).await;
    }
    let engine = build_engine(&config).await?;
    if cli.sleep_worker {
        let worker_config = SleepWorkerConfig::from_config(&config)?;
        let result = SleepWorker::new(&engine, worker_config)?.run().await;
        let shutdown = engine.shutdown().await;
        result?;
        shutdown?;
        return Ok(());
    }
    if cli.chat {
        let thread_id = cli
            .thread_id
            .clone()
            .unwrap_or_else(|| format!("cli-{}", Uuid::new_v4()));
        let result = repl::run(&engine, thread_id).await;
        let shutdown = engine.shutdown().await;
        result?;
        shutdown?;
        return Ok(());
    }
    let result = run_command(&engine, &cli).await;
    let shutdown = engine.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}

fn validate_sleep_args(cli: &Cli) -> anyhow::Result<()> {
    if cli.sleep_once && !cli.message.is_empty() {
        anyhow::bail!("--sleep-once cannot be combined with MESSAGE");
    }
    if cli.sleep_status && !cli.message.is_empty() {
        anyhow::bail!("--sleep-status cannot be combined with MESSAGE");
    }
    if cli.sleep_once && cli.sleep_status {
        anyhow::bail!("--sleep-once cannot be combined with --sleep-status");
    }
    Ok(())
}

fn validate_integration_args(cli: &Cli) -> anyhow::Result<()> {
    let selected = [
        cli.integration_candidates,
        cli.verify_integration.is_some(),
        cli.reject_integration.is_some(),
        cli.integrate_memory.is_some(),
        cli.integrate_position.is_some(),
    ]
    .into_iter()
    .filter(|selected| *selected)
    .count();
    if selected > 1 {
        anyhow::bail!("integration commands cannot be combined")
    }
    if selected > 0 && (cli.chat || cli.response_profile) {
        anyhow::bail!("integration commands cannot be combined with --chat or --response-profile")
    }
    if selected > 0 && !cli.message.is_empty() {
        anyhow::bail!("integration commands cannot be combined with MESSAGE")
    }
    if cli.integration_reason.is_some()
        && cli.verify_integration.is_none()
        && cli.reject_integration.is_none()
    {
        anyhow::bail!("--integration-reason requires --verify-integration or --reject-integration")
    }
    if (cli.verify_integration.is_some() || cli.reject_integration.is_some())
        && cli.integration_reason.is_none()
    {
        anyhow::bail!("verification and rejection require --integration-reason")
    }
    Ok(())
}

fn validate_sleep_worker_args(cli: &Cli) -> anyhow::Result<()> {
    let mut conflicts = Vec::new();
    if cli.chat {
        conflicts.push("--chat");
    }
    if !cli.message.is_empty() {
        conflicts.push("MESSAGE");
    }
    if cli.thread_id.is_some() {
        conflicts.push("--thread-id");
    }
    if cli.message_id.is_some() {
        conflicts.push("--message-id");
    }
    if cli.inspect {
        conflicts.push("--inspect");
    }
    if cli.identity {
        conflicts.push("--identity");
    }
    if cli.positions {
        conflicts.push("--positions");
    }
    if cli.conflicts {
        conflicts.push("--conflicts");
    }
    if cli.goals {
        conflicts.push("--goals");
    }
    if cli.tasks {
        conflicts.push("--tasks");
    }
    if cli.completion_status.is_some() {
        conflicts.push("--completion-status");
    }
    if cli.completion_claims {
        conflicts.push("--completion-claims");
    }
    if cli.runs {
        conflicts.push("--runs");
    }
    if cli.pending {
        conflicts.push("--pending");
    }
    if cli.resume {
        conflicts.push("--resume");
    }
    if cli.memory_list {
        conflicts.push("--memory-list");
    }
    if cli.response_profile {
        conflicts.push("--response-profile");
    }
    if cli.json {
        conflicts.push("--json");
    }
    if cli.embedding_status {
        conflicts.push("--embedding-status");
    }
    if cli.embedding_index_once {
        conflicts.push("--embedding-index-once");
    }
    if cli.embedding_search.is_some() {
        conflicts.push("--embedding-search");
    }
    if cli.sleep_once {
        conflicts.push("--sleep-once");
    }
    if cli.sleep_status {
        conflicts.push("--sleep-status");
    }
    if cli.integration_candidates {
        conflicts.push("--integration-candidates");
    }
    if cli.verify_integration.is_some() {
        conflicts.push("--verify-integration");
    }
    if cli.reject_integration.is_some() {
        conflicts.push("--reject-integration");
    }
    if cli.integrate_memory.is_some() {
        conflicts.push("--integrate-memory");
    }
    if cli.integrate_position.is_some() {
        conflicts.push("--integrate-position");
    }
    if cli.integration_reason.is_some() {
        conflicts.push("--integration-reason");
    }
    if cli.memory_candidate.is_some() {
        conflicts.push("--memory-candidate");
    }
    if cli.memory_kind.is_some() {
        conflicts.push("--memory-kind");
    }
    if cli.memory_confidence.is_some() {
        conflicts.push("--memory-confidence");
    }
    if cli.promote_memory.is_some() {
        conflicts.push("--promote-memory");
    }
    if cli.reject_memory.is_some() {
        conflicts.push("--reject-memory");
    }
    if cli.supersede_memory.is_some() {
        conflicts.push("--supersede-memory");
    }
    if cli.expire_memory.is_some() {
        conflicts.push("--expire-memory");
    }
    if cli.approve.is_some() {
        conflicts.push("--approve");
    }
    if cli.deny.is_some() {
        conflicts.push("--deny");
    }
    if cli.execute.is_some() {
        conflicts.push("--execute");
    }
    if conflicts.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(
            "--sleep-worker cannot be combined with {}",
            conflicts.join(", ")
        )
    }
}

fn validate_response_profile_args(cli: &Cli) -> anyhow::Result<()> {
    let mut conflicts = Vec::new();
    if cli.chat {
        conflicts.push("--chat");
    }
    if !cli.message.is_empty() {
        conflicts.push("MESSAGE");
    }
    if cli.message_id.is_some() {
        conflicts.push("--message-id");
    }
    if cli.thread_id.is_some() {
        conflicts.push("--thread-id");
    }
    if cli.inspect {
        conflicts.push("--inspect");
    }
    if cli.identity {
        conflicts.push("--identity");
    }
    if cli.positions {
        conflicts.push("--positions");
    }
    if cli.conflicts {
        conflicts.push("--conflicts");
    }
    if cli.goals {
        conflicts.push("--goals");
    }
    if cli.tasks {
        conflicts.push("--tasks");
    }
    if cli.runs {
        conflicts.push("--runs");
    }
    if cli.pending {
        conflicts.push("--pending");
    }
    if cli.resume {
        conflicts.push("--resume");
    }
    if cli.memory_list {
        conflicts.push("--memory-list");
    }
    if cli.memory_kind.is_some() {
        conflicts.push("--memory-kind");
    }
    if cli.memory_confidence.is_some() {
        conflicts.push("--memory-confidence");
    }
    if cli.memory_candidate.is_some() {
        conflicts.push("--memory-candidate");
    }
    if cli.promote_memory.is_some() {
        conflicts.push("--promote-memory");
    }
    if cli.reject_memory.is_some() {
        conflicts.push("--reject-memory");
    }
    if cli.supersede_memory.is_some() {
        conflicts.push("--supersede-memory");
    }
    if cli.expire_memory.is_some() {
        conflicts.push("--expire-memory");
    }
    if cli.approve.is_some() {
        conflicts.push("--approve");
    }
    if cli.deny.is_some() {
        conflicts.push("--deny");
    }
    if cli.execute.is_some() {
        conflicts.push("--execute");
    }
    if cli.embedding_status {
        conflicts.push("--embedding-status");
    }
    if cli.embedding_index_once {
        conflicts.push("--embedding-index-once");
    }
    if cli.embedding_search.is_some() {
        conflicts.push("--embedding-search");
    }
    if cli.json {
        conflicts.push("--json");
    }
    if conflicts.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(
            "--response-profile cannot be combined with {}",
            conflicts.join(", ")
        )
    }
}

fn validate_chat_args(cli: &Cli) -> anyhow::Result<()> {
    let mut conflicts = Vec::new();
    if !cli.message.is_empty() {
        conflicts.push("MESSAGE");
    }
    if cli.message_id.is_some() {
        conflicts.push("--message-id");
    }
    if cli.inspect {
        conflicts.push("--inspect");
    }
    if cli.identity {
        conflicts.push("--identity");
    }
    if cli.positions {
        conflicts.push("--positions");
    }
    if cli.conflicts {
        conflicts.push("--conflicts");
    }
    if cli.goals {
        conflicts.push("--goals");
    }
    if cli.tasks {
        conflicts.push("--tasks");
    }
    if cli.completion_status.is_some() {
        conflicts.push("--completion-status");
    }
    if cli.completion_claims {
        conflicts.push("--completion-claims");
    }
    if cli.runs {
        conflicts.push("--runs");
    }
    if cli.pending {
        conflicts.push("--pending");
    }
    if cli.resume {
        conflicts.push("--resume");
    }
    if cli.memory_list {
        conflicts.push("--memory-list");
    }
    if cli.memory_kind.is_some() {
        conflicts.push("--memory-kind");
    }
    if cli.memory_confidence.is_some() {
        conflicts.push("--memory-confidence");
    }
    if cli.memory_candidate.is_some() {
        conflicts.push("--memory-candidate");
    }
    if cli.promote_memory.is_some() {
        conflicts.push("--promote-memory");
    }
    if cli.reject_memory.is_some() {
        conflicts.push("--reject-memory");
    }
    if cli.supersede_memory.is_some() {
        conflicts.push("--supersede-memory");
    }
    if cli.expire_memory.is_some() {
        conflicts.push("--expire-memory");
    }
    if cli.approve.is_some() {
        conflicts.push("--approve");
    }
    if cli.deny.is_some() {
        conflicts.push("--deny");
    }
    if cli.execute.is_some() {
        conflicts.push("--execute");
    }
    if cli.embedding_status {
        conflicts.push("--embedding-status");
    }
    if cli.embedding_index_once {
        conflicts.push("--embedding-index-once");
    }
    if cli.embedding_search.is_some() {
        conflicts.push("--embedding-search");
    }
    if cli.sleep_once {
        conflicts.push("--sleep-once");
    }
    if cli.sleep_status {
        conflicts.push("--sleep-status");
    }
    if cli.json {
        conflicts.push("--json");
    }
    if conflicts.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("--chat cannot be combined with {}", conflicts.join(", "))
    }
}

fn validate_completion_args(cli: &Cli) -> anyhow::Result<()> {
    if (cli.completion_status.is_some() || cli.completion_claims)
        && (!cli.message.is_empty() || cli.message_id.is_some())
    {
        anyhow::bail!("completion queries cannot be combined with MESSAGE or --message-id")
    }
    if cli.complete_task.is_some()
        && [
            cli.chat,
            cli.thread_id.is_some(),
            cli.message_id.is_some(),
            !cli.message.is_empty(),
            cli.inspect,
            cli.identity,
            cli.positions,
            cli.conflicts,
            cli.goals,
            cli.tasks,
            cli.completion_status.is_some(),
            cli.completion_claims,
            cli.runs,
            cli.pending,
            cli.resume,
            cli.memory_list,
            cli.response_profile,
            cli.embedding_status,
            cli.embedding_index_once,
            cli.embedding_search.is_some(),
            cli.sleep_once,
            cli.sleep_status,
            cli.sleep_worker,
            cli.integration_candidates,
            cli.verify_integration.is_some(),
            cli.reject_integration.is_some(),
            cli.integrate_memory.is_some(),
            cli.integrate_position.is_some(),
            cli.integration_reason.is_some(),
            cli.memory_candidate.is_some(),
            cli.memory_kind.is_some(),
            cli.memory_confidence.is_some(),
            cli.promote_memory.is_some(),
            cli.reject_memory.is_some(),
            cli.supersede_memory.is_some(),
            cli.expire_memory.is_some(),
            cli.approve.is_some(),
            cli.deny.is_some(),
            cli.execute.is_some(),
        ]
        .into_iter()
        .any(|selected| selected)
    {
        anyhow::bail!("--complete-task cannot be combined with another command or MESSAGE")
    }
    Ok(())
}

async fn run_embedding_command(config: &Config, cli: &Cli) -> anyhow::Result<()> {
    let canonical = SqliteStore::open(&config.database_url).await?;
    let state = canonical.state().await?;
    let events = canonical.events().await?;
    let space = embedding_space(config);
    let store = Arc::new(SqliteEmbeddingStore::open(&config.database_url).await?);

    if cli.embedding_status {
        store.register_space(&space).await?;
        let documents = embedding_documents(&state, &events)?;
        let missing = store.discover_missing_documents(&space, &documents).await?;
        let mut status = store.embedding_status(&space).await?;
        status.enabled = config.embedding_enabled;
        status.missing_record_count = missing.len() as u64;
        println!("{}", serde_json::to_string_pretty(&status)?);
        return Ok(());
    }

    let provider = build_embedding_provider(config)?
        .ok_or_else(|| anyhow::anyhow!("embedding provider is disabled"))?;
    let indexer = EmbeddingIndexer::new(Arc::new(provider), store, config.embedding_batch_size);
    if cli.embedding_index_once {
        let report = indexer.index_once(&state, &events).await?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    if let Some(query) = &cli.embedding_search {
        let matches = indexer.search(query, 10).await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "query": query,
                "matches": matches,
            }))?
        );
        return Ok(());
    }
    unreachable!("embedding command was selected")
}

async fn run_command(engine: &Engine, cli: &Cli) -> anyhow::Result<()> {
    if let Some(id) = &cli.complete_task {
        let expected_revision = engine.state().await?.revision;
        let task = engine
            .complete_task(parse_id(id)?, expected_revision)
            .await?;
        println!("{}", serde_json::to_string_pretty(&task)?);
        return Ok(());
    }
    if cli.integration_candidates {
        let candidates = engine
            .list_integration_candidates()
            .await?
            .into_iter()
            .map(|item| {
                serde_json::json!({
                    "candidate_id": item.candidate.id,
                    "kind": item.candidate.kind,
                    "disposition": item.candidate.disposition,
                    "confidence": item.candidate.confidence,
                    "source_event_ids": item.candidate.source_event_ids,
                    "counterevidence_event_ids": item.candidate.counterevidence_event_ids,
                    "fingerprint": item.candidate.fingerprint,
                    "as_of_revision": item.candidate.as_of_revision,
                    "content": item.candidate.content,
                    "rationale": item.candidate.rationale,
                    "materialized_memory_id": item.materialization.map(|value| value.memory_id),
                    "materialized_position_id": item.position_materialization.map(|value| value.position_id),
                })
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({"candidates": candidates}))?
        );
        return Ok(());
    }
    if let Some(id) = &cli.verify_integration {
        let candidate = engine
            .verify_integration_candidate(
                parse_id(id)?,
                engine.user_id(),
                cli.integration_reason.as_deref().unwrap_or_default(),
            )
            .await?;
        println!("{}", serde_json::to_string_pretty(&candidate)?);
        return Ok(());
    }
    if let Some(id) = &cli.reject_integration {
        let candidate = engine
            .reject_integration_candidate(
                parse_id(id)?,
                engine.user_id(),
                cli.integration_reason.as_deref().unwrap_or_default(),
            )
            .await?;
        println!("{}", serde_json::to_string_pretty(&candidate)?);
        return Ok(());
    }
    if let Some(id) = &cli.integrate_memory {
        let result = engine.integrate_memory(parse_id(id)?).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }
    if let Some(id) = &cli.integrate_position {
        let result = engine.integrate_position(parse_id(id)?).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }
    if cli.sleep_once {
        println!(
            "{}",
            serde_json::to_string_pretty(&engine.sleep_once().await?)?
        );
        return Ok(());
    }
    if cli.sleep_status {
        println!(
            "{}",
            serde_json::to_string_pretty(&engine.sleep_status().await?)?
        );
        return Ok(());
    }
    if cli.inspect
        || cli.identity
        || cli.positions
        || cli.conflicts
        || cli.goals
        || cli.tasks
        || cli.completion_status.is_some()
        || cli.completion_claims
        || cli.runs
        || cli.pending
    {
        let state = engine.state().await?;
        let value = if cli.identity {
            serde_json::json!({
                "principals": state.principals,
                "identity_versions": state.identity_versions,
                "relationships": state.relationships,
            })
        } else if cli.positions {
            serde_json::json!({"positions": state.positions})
        } else if cli.conflicts {
            serde_json::json!({"conflicts": state.conflicts})
        } else if cli.goals {
            serde_json::json!({"goals": state.goals})
        } else if cli.tasks {
            serde_json::json!({"tasks": state.tasks})
        } else if let Some(task_id) = &cli.completion_status {
            let task_id = parse_id(task_id)?;
            let report = engine.completion_status(task_id, state.revision).await?;
            serde_json::to_value(report)?
        } else if cli.completion_claims {
            let claims = state
                .completion_claims
                .values()
                .map(|claim| {
                    serde_json::json!({
                        "claim_id": claim.id,
                        "task_id": claim.task_id,
                        "criterion_id": claim.criterion_id,
                        "disposition": claim.disposition,
                        "confidence": claim.confidence,
                        "fingerprint": claim.fingerprint,
                        "evidence_count": claim.evidence_refs.len(),
                        "as_of_sequence": claim.as_of_sequence,
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({"claims": claims})
        } else if cli.runs {
            serde_json::json!({
                "runs": state.runs,
                "attempts": state.attempts,
                "working_states": state.working_states,
            })
        } else if cli.pending {
            let recovery = engine.recovery_report().await?;
            serde_json::json!({
                "approvals": state.approvals,
                "active_runs": recovery.active_runs,
                "unknown_operations": recovery.unknown_operations,
                "incomplete_attempts": recovery.incomplete_attempts,
                "projection_verified": recovery.projection_verified,
            })
        } else {
            serde_json::to_value(state)?
        };
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }
    if cli.resume {
        println!(
            "{}",
            serde_json::to_string_pretty(&engine.recovery_report().await?)?
        );
        return Ok(());
    }
    if cli.memory_list {
        let state = engine.state().await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "candidates": state.memory_candidates,
                "active": state.active_memories,
            }))?
        );
        return Ok(());
    }
    if let Some(content) = &cli.memory_candidate {
        let memory = engine
            .create_memory_candidate(
                parse_memory_kind(cli.memory_kind.as_deref().unwrap_or("explicit_preference"))?,
                content.clone(),
                cli.memory_confidence.unwrap_or(50),
                false,
            )
            .await?;
        println!("{}", serde_json::to_string_pretty(&memory)?);
        return Ok(());
    }
    if let Some(id) = &cli.promote_memory {
        let memory = engine
            .promote_memory(parse_id::<MemoryCandidateId>(id)?)
            .await?;
        println!("{}", serde_json::to_string_pretty(&memory)?);
        return Ok(());
    }
    if let Some(id) = &cli.reject_memory {
        let memory = engine
            .reject_memory(parse_id::<MemoryCandidateId>(id)?)
            .await?;
        println!("{}", serde_json::to_string_pretty(&memory)?);
        return Ok(());
    }
    if let Some(id) = &cli.supersede_memory {
        let memory = engine
            .supersede_memory(parse_id::<crate::core::MemoryId>(id)?)
            .await?;
        println!("{}", serde_json::to_string_pretty(&memory)?);
        return Ok(());
    }
    if let Some(id) = &cli.expire_memory {
        let memory = engine
            .expire_memory(parse_id::<crate::core::MemoryId>(id)?)
            .await?;
        println!("{}", serde_json::to_string_pretty(&memory)?);
        return Ok(());
    }
    if let Some(id) = &cli.approve {
        let approval = engine
            .resolve_approval(parse_id::<ApprovalId>(id)?, true)
            .await?;
        println!("{}", serde_json::to_string_pretty(&approval)?);
        return Ok(());
    }
    if let Some(id) = &cli.deny {
        let approval = engine
            .resolve_approval(parse_id::<ApprovalId>(id)?, false)
            .await?;
        println!("{}", serde_json::to_string_pretty(&approval)?);
        return Ok(());
    }
    if let Some(id) = &cli.execute {
        let receipt = engine
            .execute_operation(parse_id::<OperationId>(id)?)
            .await?;
        println!("{}", serde_json::to_string_pretty(&receipt)?);
        return Ok(());
    }

    let message = if cli.message.is_empty() {
        let mut input = String::new();
        tokio::io::stdin().read_to_string(&mut input).await?;
        input.trim().to_owned()
    } else {
        cli.message.join(" ")
    };
    if message.is_empty() {
        anyhow::bail!("provide a message or a query/approval command");
    }
    let message_id = cli.message_id.clone();
    let result = engine
        .handle(Observation {
            id: crate::core::ObservationId::new(),
            actor_id: engine.user_id(),
            content: message,
            source_type: "cli".to_owned(),
            source_ref: message_id.clone(),
            thread_id: cli.thread_id.clone(),
            message_id,
            received_at: crate::core::model::now(),
        })
        .await?;
    println!("{}", render_interaction_result(&result, cli.json)?);
    Ok(())
}

fn response_profile_json(resolution: &crate::core::ResponseProfileResolution) -> serde_json::Value {
    let profile = &resolution.profile;
    let evidence = profile
        .evidence
        .iter()
        .map(|evidence| {
            serde_json::json!({
                "memory_id": evidence.memory_id,
                "source_event_id": evidence.source_event_id,
                "key": evidence.key,
                "scope": evidence.scope,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "profile": {
            "language": profile.language,
            "verbosity": profile.verbosity,
            "step_size": profile.step_size,
            "progress_visibility": profile.progress_visibility,
            "next_action_first": profile.next_action_first,
            "technical_depth": profile.technical_depth,
            "preferred_format": profile.preferred_format,
        },
        "evidence": evidence,
        "as_of_revision": profile.as_of_revision,
        "profile_hash": profile.profile_hash,
        "report": resolution.report,
    })
}

fn render_interaction_result(result: &InteractionResult, json: bool) -> anyhow::Result<String> {
    if json {
        Ok(serde_json::to_string_pretty(result)?)
    } else {
        Ok(format!("HEKATE: {}", result.decision.message))
    }
}

fn parse_memory_kind(value: &str) -> anyhow::Result<MemoryKind> {
    match value {
        "explicit_preference" => Ok(MemoryKind::ExplicitPreference),
        "inferred_preference" => Ok(MemoryKind::InferredPreference),
        "verified_fact" => Ok(MemoryKind::VerifiedFact),
        "episode" => Ok(MemoryKind::Episode),
        "lesson" => Ok(MemoryKind::Lesson),
        other => anyhow::bail!("unknown memory kind: {other}"),
    }
}

fn parse_id<T>(value: &str) -> anyhow::Result<T>
where
    T: From<Uuid>,
{
    Ok(T::from(Uuid::parse_str(value)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Decision, Focus, ObservationId};

    fn result() -> InteractionResult {
        InteractionResult {
            observation_id: ObservationId::new(),
            focus: Focus::unattached(),
            decision: Decision::respond("I cannot agree that 2+2=5 is true."),
            revision: 1,
            operation_id: None,
            approval_id: None,
        }
    }

    #[test]
    fn default_message_output_is_readable_text() {
        let output = render_interaction_result(&result(), false).expect("render");
        assert_eq!(output, "HEKATE: I cannot agree that 2+2=5 is true.");
        assert!(!output.contains('{'));
    }

    #[test]
    fn json_message_output_keeps_full_interaction_result() {
        let output = render_interaction_result(&result(), true).expect("render");
        let value: serde_json::Value = serde_json::from_str(&output).expect("JSON output");
        assert_eq!(
            value["decision"]["message"],
            "I cannot agree that 2+2=5 is true."
        );
        assert!(value["observation_id"].is_string());
    }
}
