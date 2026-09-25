use std::io::Write;

use tokio::io::{AsyncBufReadExt, BufReader};
use uuid::Uuid;

use crate::core::{InteractionResult, Observation};
use crate::runtime::engine::Engine;
use crate::runtime::initiative::service::InitiativeService;

pub async fn run(
    engine: &Engine,
    thread_id: String,
    initiatives: InitiativeService,
) -> anyhow::Result<()> {
    let mut repl = Repl {
        engine,
        initiatives,
        thread_id,
        json: false,
        shown_initiative: None,
    };
    repl.run().await
}

struct Repl<'a> {
    engine: &'a Engine,
    initiatives: InitiativeService,
    thread_id: String,
    json: bool,
    shown_initiative: Option<uuid::Uuid>,
}

impl Repl<'_> {
    async fn run(&mut self) -> anyhow::Result<()> {
        println!("HEKATE ready");
        println!("Type :help for commands.");
        println!();
        self.show_one_initiative().await?;

        let stdin = tokio::io::stdin();
        let mut lines = BufReader::new(stdin).lines();
        loop {
            print_prompt()?;
            let Some(line) = lines.next_line().await? else {
                break;
            };
            let line = line.trim().to_owned();
            if line.is_empty() {
                continue;
            }
            if line.starts_with(':') {
                match self.command(&line).await {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) => eprintln!("error: {error}"),
                }
            } else if let Err(error) = self.message(&line).await {
                eprintln!("error: {error}");
            }
        }
        Ok(())
    }

    async fn command(&mut self, input: &str) -> anyhow::Result<bool> {
        match input {
            ":help" => {
                println!(":help        show available commands");
                println!(":status      show current runtime status");
                println!(":positions   show stored positions");
                println!(":conflicts   show stored conflicts");
                println!(":memory      show stored memories");
                println!(":json on     enable JSON output");
                println!(":json off    disable JSON output");
                println!(":quit        exit the REPL");
                println!(":exit        exit the REPL");
            }
            ":status" => self.status().await?,
            ":positions" => self.positions().await?,
            ":conflicts" => self.conflicts().await?,
            ":memory" => self.memory().await?,
            ":json on" => {
                self.json = true;
                println!("json output: on");
            }
            ":json off" => {
                self.json = false;
                println!("json output: off");
            }
            ":quit" | ":exit" => return Ok(true),
            _ => {
                println!("unknown command: {input}");
                println!("type :help for commands");
            }
        }
        Ok(false)
    }

    async fn status(&self) -> anyhow::Result<()> {
        let report = self.engine.recovery_report().await?;
        let value = serde_json::json!({
            "revision": report.revision,
            "projection_verified": report.projection_verified,
            "active_runs": report.active_runs.len(),
            "incomplete_attempts": report.incomplete_attempts.len(),
            "pending_approvals": report.pending_approvals.len(),
            "unknown_operations": report.unknown_operations.len(),
        });
        if self.json {
            print_json(&value)?;
        } else {
            println!("revision: {}", report.revision);
            println!("projection verified: {}", report.projection_verified);
            println!("active runs: {}", report.active_runs.len());
            println!("incomplete attempts: {}", report.incomplete_attempts.len());
            println!("pending approvals: {}", report.pending_approvals.len());
            println!("unknown operations: {}", report.unknown_operations.len());
        }
        Ok(())
    }

    async fn positions(&self) -> anyhow::Result<()> {
        let state = self.engine.state().await?;
        print_json(&serde_json::json!({"positions": state.positions}))
    }

    async fn conflicts(&self) -> anyhow::Result<()> {
        let state = self.engine.state().await?;
        print_json(&serde_json::json!({"conflicts": state.conflicts}))
    }

    async fn memory(&self) -> anyhow::Result<()> {
        let state = self.engine.state().await?;
        print_json(&serde_json::json!({
            "candidates": state.memory_candidates,
            "active": state.active_memories,
        }))
    }

    async fn message(&mut self, content: &str) -> anyhow::Result<()> {
        let message_id = Uuid::new_v4().to_string();
        let result = self
            .engine
            .handle(Observation {
                id: crate::core::ObservationId::new(),
                actor_id: self.engine.user_id(),
                content: content.to_owned(),
                source_type: "cli".to_owned(),
                source_ref: Some(message_id.clone()),
                thread_id: Some(self.thread_id.clone()),
                message_id: Some(message_id),
                received_at: crate::core::model::now(),
            })
            .await?;
        if self.json {
            print_json(&result)?;
        } else {
            println!("hekate> {}", response_message(&result));
        }
        self.show_one_initiative().await
    }

    async fn show_one_initiative(&mut self) -> anyhow::Result<()> {
        let Some(proposal) = self.initiatives.list().await?.into_iter().find(|proposal| {
            proposal.status == crate::core::InitiativeStatus::Ready
                && self.shown_initiative != Some(proposal.id)
        }) else {
            return Ok(());
        };
        self.shown_initiative = Some(proposal.id);
        if self.json {
            print_json(&serde_json::json!({
                "type": "initiative_proposal",
                "action_authorized": false,
                "delivery_authorized": false,
                "proposal": proposal,
            }))
        } else {
            println!("HEKATE proposal (local review only; no action or delivery authorized):");
            println!("  {}", proposal.content);
            println!("  Why: {}", proposal.rationale);
            println!("  ID: {}", proposal.id);
            Ok(())
        }
    }
}

fn print_prompt() -> anyhow::Result<()> {
    print!("you> ");
    std::io::stdout().flush()?;
    Ok(())
}

fn print_json<T: serde::Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn response_message(result: &InteractionResult) -> &str {
    if result.decision.message.trim().is_empty() {
        "no response message"
    } else {
        &result.decision.message
    }
}
