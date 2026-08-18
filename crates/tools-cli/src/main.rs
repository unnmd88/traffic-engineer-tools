use clap::Parser;
mod cli;
use cli::Cli;
use tools_core::monitor::{application::TasksRepoEvent, task::TaskId};
use tracing::{error, info};
mod monior;
mod scn;

use crate::{cli::print_output, monior::app::AppBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        cli::Commands::Poll { config } => {
            let content = std::fs::read_to_string(config)?;
            let mut app = AppBuilder::from_yaml(&content).await?;

            tokio::spawn(async move {
                app.start().await;
                let mut rx = app.subscribe();
                let ordered_tasks_ids: Vec<TaskId> = app
                    .get_snapshot()
                    .await
                    .expect("Failed to get snapshot") // TODO tracing
                    .sorted_task_ids()
                    .collect();

                while let Ok(update) = rx.recv().await {
                    clear_screen();

                    //execute!(stdout(), Clear(ClearType::All), cursor::MoveTo(0, 0))
                    //    .unwrap_or_default();
                    match update {
                        TasksRepoEvent::Update { snapshot, task_id } => {
                            println!("{snapshot}");
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
