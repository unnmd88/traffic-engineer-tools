use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
};

use anyhow::Context;
use clap::Parser;
mod cli;
use cli::Cli;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tools_core::snmp::SnmpQueryItem;
mod monior;
mod scn;

//use tools_core::models::{poller::Poller, worker::SnmpWorker, MonitorUpdate};

use crate::{
    cli::{OutputFormat, print_output},
    monior::app_builder::AppBuilder,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        cli::Commands::Poll { config } => {
            let content = std::fs::read_to_string(config)?;
            let mut app = AppBuilder::from_yaml(&content).await?;
            println!("State: {}", app.current_state());
            app.start().await;
            println!("{}", app.current_state());
            println!("SNAPSHOT:\n\n{:#?}", app.get_snapshot().await?);
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
