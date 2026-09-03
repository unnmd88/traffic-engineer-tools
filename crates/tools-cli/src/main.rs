use chrono::Local;
use clap::Parser;
mod cli;
use cli::Cli;
use tools_core::{
    DT_FMT,
    monitor::{application::TasksRepoResponse, task::TaskId},
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
        formatters::{constants::LINE_DOUBLE_LN, format_repository},
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

    tracing::info!("Logger initialized successfully!");

    match cli.command {
        cli::Commands::Poll { config } => {
            let content = std::fs::read_to_string(config)?;
            let mut app = AppBuilder::from_yaml(&content).await?;
            let app_created_at = Local::now();
            let app_created_at_fmt = Local::now().format(DT_FMT);

            tokio::spawn(async move {
                app.start().await;
                let monitor_id = app.id();
                //let mut rx = app.subscribe().await?;
                let mut rx = app.subscribe().await.unwrap_or_else(|e| {
                    panic!("{}", e);
                });
                let ordered_tasks_ids: Vec<TaskId> = app
                    .get_snapshot()
                    .await
                    .expect("Failed to get snapshot") // TODO tracing
                    .sorted_task_ids()
                    .collect();

                while let Ok(update) = rx.recv().await {
                    clear_screen();
                    let uptime = Local::now() - app_created_at;
                    let minutes = uptime.num_minutes();
                    let seconds = uptime.num_seconds() % 60;
                    //execute!(stdout(), Clear(ClearType::All), cursor::MoveTo(0, 0))
                    //    .unwrap_or_default();
                    match update {
                        TasksRepoResponse::Update { snapshot, task_id } => {
                            println!(
                                "{LINE_DOUBLE_LN}Monitor ID: {monitor_id}\nUptime: {minutes}m {seconds}s. Started: {app_created_at_fmt}\n{LINE_DOUBLE_LN}\n{}",
                                format_repository(&snapshot)
                            );
                            /*
                                                        for task_id in ordered_tasks_ids.iter() {
                                                            println!("Задача 1:\n{:#?}", snapshot.get_task(task_id));
                                                        }
                            */
                        }
                        _ => {}
                    }
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

fn clear_screen() {
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
