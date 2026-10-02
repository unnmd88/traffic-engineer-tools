use std::collections::BTreeMap;

use chrono::Local;
use clap::Parser;
mod cli;
use cli::Cli;
use tools_core::{
    DT_FMT,
    monitor::{
        event::MonitorEvent,
        task::{TaskId, TaskView},
    },
};
mod logging;
mod monitor;
mod scn;

use crate::{
    cli::print_output,
    logging::init_file_logging,
    monitor::{
        app::AppBuilder,
        formatters::{constants::LINE_DOUBLE_LN, format_snapshot},
    },
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let log_dir = std::env::current_dir()
        .unwrap_or_else(|_| ".".into())
        .join("logs");
    let log_dir_str = log_dir.to_str().expect("Invalid log directory path");
    let _guard = init_file_logging(log_dir_str, "traffic")?;

    match cli.command {
        cli::Commands::Poll { config } => {
            let content = std::fs::read_to_string(config)?;
            let app = AppBuilder::from_yaml(&content).await?;
            let mut rx = app.subscribe();

            let app_created_at = Local::now();
            let app_created_at_fmt = Local::now().format(DT_FMT);

            let mut dashboard: BTreeMap<TaskId, TaskView> = BTreeMap::new();

            tokio::spawn(async move {
                let monitor_id = app.id().clone();

                let snapshot = app.get_snapshot().await.expect("Failed to get snapshot");
                for t in snapshot.tasks {
                    dashboard.insert(t.id, t);
                }

                while let Ok(ev) = rx.recv().await {
                    match ev {
                        MonitorEvent::TaskChanged {
                            task_id,
                            kind: _,
                            view,
                            at: _,
                        } => {
                            dashboard.insert(task_id, view);
                        }
                        MonitorEvent::TaskRemoved {
                            task_id,
                            view: _,
                            at: _,
                        } => {
                            dashboard.remove(&task_id);
                        }
                    }
                    let _ = clear_screen();
                    let uptime = Local::now() - app_created_at;
                    let minutes = uptime.num_minutes();
                    let seconds = uptime.num_seconds() % 60;

                    println!(
                        "{LINE_DOUBLE_LN}Monitor ID: {monitor_id}\nUptime: {minutes}m {seconds}s. Started: {app_created_at_fmt}\n{LINE_DOUBLE_LN}\n{}",
                        format_snapshot(&dashboard)
                    );
                }
            });

            tokio::signal::ctrl_c().await?;
            println!("Ctrl-C is pressed");
        }

        cli::Commands::ToScn { input, output } => {
            let res = scn::run_from_string(input)?;
            print_output(output, &res)?;
        }
        cli::Commands::FromScn { input, output } => {
            let res = scn::run_from_scn(input)?;
            print_output(output, &res)?;
        }
    }

    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn clear_screen() -> std::io::Result<()> {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    out.write_all(b"\x1b[H\x1b[2J\x1b[3J")?; // Linux: чистит и экран, и scrollback
    out.flush()
}

#[cfg(target_os = "windows")]
fn clear_screen() -> std::io::Result<()> {
    use crossterm::{
        cursor::MoveTo,
        execute,
        terminal::{Clear, ClearType},
    };
    use std::io::stdout;
    execute!(stdout(), Clear(ClearType::All), MoveTo(0, 0))
}

