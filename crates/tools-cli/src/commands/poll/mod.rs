mod compact;
mod config;
mod dashboard;
mod history;
mod render;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use tools_core::monitor::{AppConfig, Application};

use crate::cli::PollView;

pub async fn run(config: PathBuf, view: PollView) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(&config)?;
    let config_dir = config.parent().unwrap_or_else(|| std::path::Path::new("."));
    let config = config::load_from_yaml(&content)?;
    let application = Application::new_ordered(AppConfig::default(), config.tasks);
    let mut history_writer = match config.history_sink {
        Some(path) => {
            let file = history::open(&config_dir.join(path)).await?;
            Some(tokio::spawn(history::write(file, application.subscribe())))
        }
        None => None,
    };
    let subscription = application.subscribe_with_snapshot();
    let running = application.spawn();
    let app = running.handle();

    let result = tokio::select! {
        result = dashboard::run(&app, subscription, view) => result.map_err(anyhow::Error::from),
        result = tokio::signal::ctrl_c() => {
            result.map_err(anyhow::Error::from)
        }
        result = history::wait(&mut history_writer) => result,
    };
    let shutdown_result = running.shutdown().await;
    let history_result = if history_writer.is_some() {
        history::wait(&mut history_writer).await
    } else {
        Ok(())
    };
    result?;
    history_result?;
    shutdown_result?;
    Ok(())
}
