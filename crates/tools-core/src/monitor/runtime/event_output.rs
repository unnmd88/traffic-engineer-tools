use crate::monitor::{
    EventData, MonitorEvent, OperationFinished, ScenarioFailure, TaskTransitionCause,
};
use tokio::sync::mpsc;

pub(super) fn log_event(event: &MonitorEvent) {
    let metadata = &event.metadata;
    match &event.data {
        EventData::TaskStateChanged(transition) => match &transition.cause {
            TaskTransitionCause::Failure(failure) => match failure.as_ref() {
                ScenarioFailure::Panicked { .. } => tracing::error!(
                    application_id = %metadata.application_id,
                    task_id = metadata.task_id.0,
                    generation = ?metadata.generation,
                    revision = metadata.revision.0,
                    seq = metadata.seq,
                    ?transition,
                    "task state changed after a scenario panic"
                ),
                _ => tracing::warn!(
                    application_id = %metadata.application_id,
                    task_id = metadata.task_id.0,
                    generation = ?metadata.generation,
                    revision = metadata.revision.0,
                    seq = metadata.seq,
                    ?transition,
                    "task state changed after a scenario failure"
                ),
            },
            _ => tracing::info!(
                application_id = %metadata.application_id,
                task_id = metadata.task_id.0,
                generation = ?metadata.generation,
                revision = metadata.revision.0,
                seq = metadata.seq,
                ?transition,
                "task state changed"
            ),
        },
        EventData::OperationStarted(operation) => tracing::debug!(
            application_id = %metadata.application_id,
            task_id = metadata.task_id.0,
            generation = ?operation.context.generation,
            revision = operation.context.revision.0,
            operation_id = operation.context.operation_id.get(),
            seq = metadata.seq,
            "operation started"
        ),
        EventData::OperationFinished(OperationFinished { context, report }) => tracing::debug!(
            application_id = %metadata.application_id,
            task_id = metadata.task_id.0,
            generation = ?context.generation,
            revision = context.revision.0,
            operation_id = context.operation_id.get(),
            seq = metadata.seq,
            status = ?report.status(),
            "operation finished"
        ),
        EventData::Preparation(_) => {}
        EventData::TaskRemoved => tracing::info!(
            application_id = %metadata.application_id,
            task_id = metadata.task_id.0,
            generation = ?metadata.generation,
            revision = metadata.revision.0,
            seq = metadata.seq,
            "task removed"
        ),
        EventData::TaskRegistered(description) => tracing::info!(
            application_id = %metadata.application_id,
            task_id = metadata.task_id.0,
            generation = ?metadata.generation,
            revision = metadata.revision.0,
            seq = metadata.seq,
            task_name = description.name,
            "task registered"
        ),
        EventData::TaskConfigurationChanged(description) => tracing::info!(
            application_id = %metadata.application_id,
            task_id = metadata.task_id.0,
            generation = ?metadata.generation,
            revision = metadata.revision.0,
            seq = metadata.seq,
            task_name = description.name,
            "task configuration updated"
        ),
    }
}

pub(super) async fn log_writer(
    mut rx: mpsc::Receiver<MonitorEvent>,
    hist_tx: mpsc::Sender<MonitorEvent>,
) {
    while let Some(ev) = rx.recv().await {
        if let Err(error) = hist_tx.send(ev).await {
            let metadata = &error.0.metadata;
            tracing::warn!(
                application_id = %metadata.application_id,
                task_id = metadata.task_id.0,
                generation = ?metadata.generation,
                revision = metadata.revision.0,
                seq = metadata.seq,
                "history sink closed; event forwarding stopped"
            );
            break;
        }
    }
}
