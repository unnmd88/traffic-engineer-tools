mod config;
mod dashboard;
mod render;

use std::path::PathBuf;

use tools_core::monitor::runtime::Application;

pub async fn run(config: PathBuf) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(config)?;
    let tasks = config::load_tasks_from_yaml(&content)?;
    let app = Application::new(None);
    app.start(tasks).await?;
    let rx = app.subscribe();

    tokio::spawn(dashboard::run(app, rx));

    tokio::signal::ctrl_c().await?;
    println!("Ctrl-C is pressed");
    Ok(())
}
