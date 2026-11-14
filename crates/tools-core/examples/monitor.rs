//! Минимальный запуск «Монитора»: создать, запустить, снять снапшот, завершить.

use anyhow::Result;
use tools_core::monitor::{AppConfig, Application, TaskConfig};

#[tokio::main]
async fn main() -> Result<()> {
    let tasks: Vec<TaskConfig> = Vec::new();
    let running = Application::new(AppConfig::default(), tasks).spawn();
    let handle = running.handle();

    let snapshot = handle.get_snapshot().await?;
    println!("application: {}", handle.id());
    println!("tasks: {}", snapshot.tasks.len());

    running.shutdown().await?;
    Ok(())
}
