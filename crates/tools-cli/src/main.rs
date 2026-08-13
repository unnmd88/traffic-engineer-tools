use std::{
    fs,
    io::Write,
    net::{IpAddr, Ipv4Addr},
};

use anyhow::Context;
use clap::Parser;
mod cli;
use cli::Cli;
use crossterm::{
    cursor, execute,
    style::{Color, Print, ResetColor, SetForegroundColor},
    terminal::{Clear, ClearType},
};
use serde::Serialize;
use std::io::stdout;
use tokio::time::Duration;
use tools_core::{monitor::application::SnapshotEvent, snmp::SnmpQueryItem};
mod monior;
mod scn;

use crate::{
    cli::{OutputFormat, print_output},
    monior::app::AppBuilder,
};

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
                while let Ok(update) = rx.recv().await {
                    clear_screen();
                    //execute!(stdout(), Clear(ClearType::All), cursor::MoveTo(0, 0))
                    //    .unwrap_or_default();
                    match update {
                        SnapshotEvent::Update {
                            snapshot,
                            group_id,
                            task_position,
                        } => {
                            println!("{snapshot}");
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
