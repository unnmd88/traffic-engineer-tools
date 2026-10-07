use clap::Parser;

mod cli;
mod commands;
mod logging;
mod monitor;

use cli::Cli;

use crate::{cli::print_output, logging::init_file_logging};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let log_dir = std::env::current_dir()
        .unwrap_or_else(|_| ".".into())
        .join("logs");
    let log_dir_str = log_dir.to_str().expect("Invalid log directory path");
    let _guard = init_file_logging(log_dir_str, "traffic")?;

    match cli.command {
        cli::Commands::Poll { config } => commands::poll::run(config).await?,
        cli::Commands::ToScn { input, output } => {
            let res = commands::scn::run_from_string(input)?;
            print_output(output, &res)?;
        }
        cli::Commands::FromScn { input, output } => {
            let res = commands::scn::run_from_scn(input)?;
            print_output(output, &res)?;
        }
    }

    Ok(())
}
