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
    monior::snapshot_builder::SnapshotBuilder,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        cli::Commands::Poll { config } => {
            println!(" --> 1");
            let content = std::fs::read_to_string(config)?;
            println!(" --> 2");

            let _ = SnapshotBuilder::from_yaml(&content)?;
            println!(" --> 3");
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
