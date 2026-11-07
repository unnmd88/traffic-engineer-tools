use std::{future::pending, path::Path};

use anyhow::Context;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWrite, AsyncWriteExt, BufWriter},
    sync::mpsc,
    task::JoinHandle,
};
use tools_core::monitor::MonitorEvent;

pub async fn open(path: &Path) -> anyhow::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("cannot open history file {}", path.display()))
}

pub async fn write<W>(writer: W, mut events: mpsc::Receiver<MonitorEvent>) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut writer = BufWriter::new(writer);
    while let Some(event) = events.recv().await {
        let mut line = serde_json::to_vec(&event).context("cannot serialize history event")?;
        line.push(b'\n');
        writer
            .write_all(&line)
            .await
            .context("cannot write history event")?;
        writer.flush().await.context("cannot flush history file")?;
    }
    writer.flush().await.context("cannot flush history file")
}

pub async fn wait(writer: &mut Option<JoinHandle<anyhow::Result<()>>>) -> anyhow::Result<()> {
    let result = match writer.as_mut() {
        Some(writer) => writer.await,
        None => pending().await,
    };
    writer.take();
    result.context("history writer task failed")?
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, BufReader, duplex};
    use tools_core::monitor::{AppConfig, Application, ApplicationId, EventData};

    use super::*;
    use crate::commands::poll::config;

    fn event() -> MonitorEvent {
        serde_json::from_value(serde_json::json!({
            "metadata": {
                "seq": 0,
                "at": "2026-11-07T00:00:00Z",
                "application_id": ApplicationId::generate(),
                "task_id": 1,
                "generation": 0,
                "revision": 1
            },
            "data": {
                "ExecutionFailed": {
                    "error": { "Polling": { "message": "first line\nsecond line" } },
                    "retry_in": null
                }
            }
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn events_are_flushed_as_single_json_lines_before_the_channel_closes() {
        let (writer, reader) = duplex(4096);
        let (sender, receiver) = mpsc::channel(1);
        let task = tokio::spawn(write(writer, receiver));
        let expected = event();
        sender.send(expected.clone()).await.unwrap();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(line.lines().count(), 1);
        let decoded: MonitorEvent = serde_json::from_str(&line).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), serde_json::to_value(expected).unwrap());
        drop(sender);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn writer_errors_close_the_receiver_and_reach_the_caller() {
        let (writer, reader) = duplex(4096);
        drop(reader);
        let (sender, receiver) = mpsc::channel(1);
        let mut task = Some(tokio::spawn(write(writer, receiver)));
        sender.send(event()).await.unwrap();
        let error = wait(&mut task).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::BrokenPipe
        );
        assert!(sender.is_closed());
        assert!(task.is_none());
    }

    async fn record_session(path: &Path) -> ApplicationId {
        let file = open(path).await.unwrap();
        let (sender, receiver) = mpsc::channel(1);
        let mut writer = Some(tokio::spawn(write(file, receiver)));
        let running = Application::new(
            AppConfig {
                history_sink: Some(sender),
                ..AppConfig::default()
            },
            vec![],
        )
        .spawn();
        let app = running.handle();
        let application_id = *app.id();
        let mut config = config::load_from_yaml(
            "tasks:\n  - name: idle\n    interval: 1s\n    limit: 0\n    attempt_config:\n      timeout: 1s\n      retries: 0\n      retry_delay: 0s\n    query:\n      query_type: snmpget\n      host: 127.0.0.1\n      port: 161\n      community: public\n      oids: []\n",
        )
        .unwrap();
        let id = app.add_task(config.tasks.remove(0)).await.unwrap();
        app.remove_task(id).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            running.shutdown().await.unwrap();
            wait(&mut writer).await.unwrap();
        })
        .await
        .unwrap();
        application_id
    }

    #[tokio::test]
    async fn shutdown_drains_events_and_later_sessions_append_to_the_file() {
        let path =
            std::env::temp_dir().join(format!("tctl-history-{}.jsonl", ApplicationId::generate()));
        let first = record_session(&path).await;
        let second = record_session(&path).await;
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        tokio::fs::remove_file(&path).await.unwrap();
        let events: Vec<MonitorEvent> = content
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(events.len(), 4);
        assert_ne!(first, second);
        for (session, application_id) in events.chunks_exact(2).zip([first, second]) {
            assert_eq!(session[0].metadata.application_id, application_id);
            assert_eq!(session[1].metadata.application_id, application_id);
            assert_eq!(session[0].metadata.seq, 0);
            assert_eq!(session[1].metadata.seq, 1);
            assert_eq!(session[0].metadata.task_id, session[1].metadata.task_id);
            assert!(matches!(session[0].data, EventData::TaskAdded));
            assert!(matches!(session[1].data, EventData::TaskRemoved));
        }
    }
}
