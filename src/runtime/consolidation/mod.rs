//! Coordinates Sleep, retrieval, and verified Memory/Position application.
//!
//! See `docs/consolidation-v1.md` for ownership and event flow. Sleep and
//! MemoryIntegration remain the persistence owners; there is no second cursor
//! or memory store.

use std::sync::Arc;

use crate::core::{IntegrationCandidateId, PrincipalId};
use crate::ports::{SleepCognitiveModel, Storage};
use crate::runtime::memory_integration::{
    IntegrationError, MaterializationCommit, MemoryIntegration, PositionMaterializationCommit,
};
use crate::runtime::projector::Projector;
use crate::runtime::recall::SemanticRecall;
use crate::runtime::sleep::{SleepOnceResult, SleepRuntimeError};

pub use crate::core::consolidation::ConsolidationBoundary;

pub(crate) async fn sleep_once(
    storage: &dyn Storage,
    projector: &Projector,
    model: Option<&dyn SleepCognitiveModel>,
    recall: Option<&SemanticRecall>,
    hekate_id: PrincipalId,
    user_id: PrincipalId,
) -> Result<SleepOnceResult, SleepRuntimeError> {
    crate::runtime::sleep::SleepCoordinator::new(
        storage, projector, model, recall, hekate_id, user_id,
    )
    .sleep_once()
    .await
}

pub(crate) async fn integrate_memory(
    storage: Arc<dyn Storage>,
    candidate_id: IntegrationCandidateId,
    actor_id: PrincipalId,
) -> Result<MaterializationCommit, IntegrationError> {
    MemoryIntegration::new(storage)
        .materialize_memory(candidate_id, actor_id)
        .await
}

pub(crate) async fn integrate_position(
    storage: Arc<dyn Storage>,
    candidate_id: IntegrationCandidateId,
    hekate_id: PrincipalId,
    actor_id: PrincipalId,
) -> Result<PositionMaterializationCommit, IntegrationError> {
    MemoryIntegration::new(storage)
        .materialize_position(candidate_id, hekate_id, actor_id)
        .await
}
