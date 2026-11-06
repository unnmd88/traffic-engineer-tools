mod config;
mod dashboard;
mod render;

use std::path::PathBuf;

use tools_core::monitor::runtime::{AppConfig, Application};

pub async fn run(config: PathBuf) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(config)?;
    let tasks = config::load_tasks_from_yaml(&content)?;
    let app = Application::new(AppConfig::default());
    app.start(tasks).await?;
    let subscription = app.subscribe_with_snapshot().await?;

    tokio::select! {
        result = dashboard::run(&app, subscription) => result?,
        result = tokio::signal::ctrl_c() => {
            result?;
            println!("Ctrl-C is pressed");
        }
    }
    app.shutdown().await?;
    Ok(())
}
