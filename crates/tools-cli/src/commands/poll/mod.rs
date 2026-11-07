mod config;
mod dashboard;
mod render;

use std::path::PathBuf;

use tools_core::monitor::{AppConfig, Application};

pub async fn run(config: PathBuf) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(config)?;
    let tasks = config::load_tasks_from_yaml(&content)?;
    let application = Application::new(AppConfig::default(), tasks);
    let subscription = application.subscribe_with_snapshot();
    let running = application.spawn();
    let app = running.handle();

    let result = tokio::select! {
        result = dashboard::run(&app, subscription) => result.map_err(anyhow::Error::from),
        result = tokio::signal::ctrl_c() => {
            result.map_err(anyhow::Error::from)
        }
    };
    running.shutdown().await?;
    result
}
