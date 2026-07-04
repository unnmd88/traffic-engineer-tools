use tokio::sync::mpsc;
use tracing::instrument;

use crate::models::Event;
use crate::sender::EventSender;

#[instrument(skip_all)]
pub async fn handle_events(
    mut rx: mpsc::Receiver<Event>,
    mut senders: Vec<Box<dyn EventSender + Send>>,
) {
    while let Some(event) = rx.recv().await {
        for sender in &mut senders {
            if let Err(e) = sender.send(event.clone()).await {
                tracing::error!(error = %e, "handle_events: send error");
            }
        }
    }
}
