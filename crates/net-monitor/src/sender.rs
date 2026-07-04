use std::path::PathBuf;

use crate::{
    models::{Envelope, Event},
    utils::get_timestamp_fmt,
};
use async_trait::async_trait;
use tokio::fs::OpenOptions;

#[async_trait]
pub trait EventSender {
    async fn send(&self, event: Event) -> Result<(), String>;
}

pub struct JsonSender {
    path: PathBuf,
    session_id: String,
}

impl JsonSender {
    pub async fn new(path: &str, session_id: String) -> Result<Self, String> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| format!("Cannot open: {}", e))?;
        Ok(Self {
            path: PathBuf::from(path),
            session_id,
        })
    }
}

#[async_trait]
impl EventSender for JsonSender {
    async fn send(&self, event: Event) -> Result<(), String> {
        let envelope = Envelope {
            session_id: self.session_id.clone(),
            timestamp: get_timestamp_fmt(),
            event,
        };

        let line = serde_json::to_string(&envelope)
            .map_err(|e| format!("JSON error: {}", e))?;

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|e| format!("Cannot open: {}", e))?;

        tokio::io::AsyncWriteExt::write_all(
            &mut file,
            format!("{}\n", line).as_bytes(),
        )
        .await
        .map_err(|e| format!("Write error: {}", e))?;

        Ok(())
    }
}
