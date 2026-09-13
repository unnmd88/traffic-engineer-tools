use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
};

use chrono::Local;
use clap::Parser;
mod cli;
use cli::Cli;
use tokio::time::Instant;
use tools_core::{
    DT_FMT,
    monitor::{
        event::MonitorEvent,
        task::{TaskId, TaskView},
    },
};
use tracing::{error, info};
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

use crossterm::{
    cursor::MoveTo,
    execute,
    terminal::{Clear, ClearType},
};
use std::io::stdout;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let log_dir = std::env::current_dir()
        .unwrap_or_else(|_| ".".into())
        .join("logs");
    let log_dir_str = log_dir.to_str().expect("Invalid log directory path");
    let _guard = init_file_logging(log_dir_str, "traffic")?;

    tracing::info!("Logger initialized successfully!");

    match cli.command {
        cli::Commands::Poll { config } => {
            let content = std::fs::read_to_string(config)?;
            let mut app = AppBuilder::from_yaml(&content).await?;
            let mut rx = app.subscribe();

            let app_created_at = Local::now();
            let app_created_at_fmt = Local::now().format(DT_FMT);

            let mut dashboard: BTreeMap<TaskId, TaskView> = BTreeMap::new();

            tokio::spawn(async move {
                //let app = app_builder.start().await?;
                let monitor_id = app.id().clone();

                let snapshot = app.get_snapshot().await.expect("Failed to get snapshot");
                for t in snapshot.tasks {
                    dashboard.insert(t.id, t);
                }

                let mut max_t = 0;

                while let Ok(ev) = rx.recv().await {
                    let start = Instant::now();
                    match ev {
                        MonitorEvent::TaskChanged {
                            task_id,
                            kind,
                            view,
                            at,
                        } => {
                            dashboard.insert(task_id, view);
                        }
                        MonitorEvent::TaskRemoved { task_id, view, at } => {
                            dashboard.remove(&task_id);
                        }
                    }
                    let _ = clear_screen();
                    let uptime = Local::now() - app_created_at;
                    let minutes = uptime.num_minutes();
                    let seconds = uptime.num_seconds() % 60;

                    let max = max(max_t, start.elapsed().as_micros());

                    println!(
                        "{LINE_DOUBLE_LN}\nMonitor ID: {monitor_id}\nUptime: {minutes}m {seconds}s. Started: {app_created_at_fmt}\n{LINE_DOUBLE_LN}\n{}",
                        format_snapshot(&dashboard)
                    );
                }
            });

            tokio::signal::ctrl_c().await?;
            println!("Ctrl-C is pressed");
        }

        cli::Commands::ToScn { input, output } => {
            let res = scn::run_from_string(input).await?;
            let _ = print_output(output, &res.as_pretty_string(), &res)?;
        }
        cli::Commands::FromScn { input, output } => {
            let res = scn::run_from_scn(input).await?;
            let _ = print_output(output, &res.as_pretty_string(), &res)?;
        }
        _ => {}
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
/*
fn clear_screen2() {
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("cmd")
            .args(&["/c", "cls"])
            .status();
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = std::process::Command::new("clear").status();
    }
}
*/
