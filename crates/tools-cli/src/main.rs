use anyhow::Result;
use clap::Parser;
mod cli;
mod commands;
use cli::Cli;
use serde::Serialize;

use crate::cli::{OutputFormat, print_output};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        /*
                cli::Commands::Poll {
                    protocol,
                    host,
                    config,
                } => commands::poll::run(protocol, host, config).await?,
        */
        cli::Commands::ToScn { input, output } => {
            let res = commands::scn::run_from_string(input).await?;
            print_output(output, &res.as_pretty_string(), &res)?;
        }
        cli::Commands::FromScn { input, output } => {
            let res = commands::scn::run_from_scn(input).await?;
            print_output(output, &res.as_pretty_string(), &res)?;
        }
        _ => {}
    }

    Ok(())
}
