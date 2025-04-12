use clap::{Parser, Subcommand};
use pulsar::{Pulsar, TokioExecutor};
use std::str::FromStr;
use anyhow::Error;
use colored_json::to_colored_json_auto;
use crate::pulsarctl::PulsarConfig;

mod commands;
mod io;
mod message;
mod common;
mod pulsarctl;

#[derive(Parser)]
#[command(name = "pulsar-cli", about = "A command line tool to simpify publishing and viewing Pulsar messages")]
struct CommandLineInputs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone)]
enum InitialPosition {
    Earliest,
    Latest
}

impl FromStr for InitialPosition {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.to_lowercase().as_str() {
            "earliest" => Ok(InitialPosition::Earliest),
            "latest" => Ok(InitialPosition::Latest),
            _ => Err(format!("Invalid InitialPosition: {}", input)),
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Start subscribing to the topic events
    Attach {
        #[arg(short, long)]
        topic: String,
        #[arg(long, default_value = "earliest")]
        position: InitialPosition,
        #[arg(short, long)]
        pulsarctl_env: Option<String>
    },
    /// Stop subscribing to the topic events
    Detach {
        #[arg(short, long)]
        topic: String,
        #[arg(short, long)]
        pulsarctl_env: Option<String>
    },
    /// Publish to a given topic
    Publish {
        #[arg(short, long)]
        topic: String,
        #[arg(short, long)]
        pulsarctl_env: Option<String>
    },
    /// Search events from a given topic
    Search {
        #[arg(short, long)]
        topic: String,
        #[arg(long)]
        search_term: String,
        #[arg(short, long, default_value = "false")]
        acknowledge_searched: bool,
        #[arg(long, default_value = "10")]
        seek_minutes: usize,
        #[arg(short, long, default_value = "100")]
        limit: usize,
        #[arg(short, long, default_value = "false")]
        output_only_event_data: bool,
        #[arg(long, default_value = "earliest")]
        position: InitialPosition,
        #[arg(short, long)]
        pulsarctl_env: Option<String>
    },
    /// Watch events from a given topic
    Watch {
        #[arg(short, long)]
        topic: String,
        #[arg(long, default_value = "")]
        search_term: String,
        #[arg(short, long, default_value = "true")]
        acknowledge_searched: bool,
        #[arg(long, default_value = "10")]
        seek_minutes: usize,
        #[arg(short, long, default_value = "100")]
        limit: usize,
        #[arg(short, long, default_value = "false")]
        output_only_event_data: bool,
        #[arg(long, default_value = "latest")]
        position: InitialPosition,
        #[arg(short, long)]
        pulsarctl_env: Option<String>
    }
    //TODO: View the stats for a given topic and its partitions
    //TODO: Peek command
}

struct ScanOptions {
    acknowledge_searched: bool,
    seek_minutes: usize,
    limit: usize,
    output_only_event_data: bool,
    position: InitialPosition
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = CommandLineInputs::parse();

    let addr = "pulsar://localhost:6650".to_string();
    let builder = Pulsar::builder(addr, TokioExecutor);
    let mut pulsar: Pulsar<_> = builder.build().await?;

    match cli.command {
        Commands::Attach { topic, position, pulsarctl_env } => {
            let config: PulsarConfig = pulsarctl::read_config()?;
            println!("{:#?}", config);
            commands::attach::execute(&mut pulsar, &topic, &position).await?;
            Ok(())
        },
        Commands::Detach { topic, pulsarctl_env } => {
            println!("Unsubscribing from {:?}", topic);
            commands::detach::execute(&mut pulsar, &topic).await?;
            Ok(())
        }
        Commands::Publish { topic, pulsarctl_env } => {
            println!("Publishing to {:?}", topic);
            commands::publish::execute(&mut pulsar, &topic).await?;
            println!("Successfully published to {:?}", topic);
            Ok(())
        }
        Commands::Search { topic, search_term, acknowledge_searched, seek_minutes, limit, output_only_event_data, position, pulsarctl_env } =>{
            println!("Searching for events using search term {}", search_term);
            let found_events = commands::search::execute(&mut pulsar, &topic, &search_term, &ScanOptions {
                acknowledge_searched,
                seek_minutes,
                limit,
                output_only_event_data,
                position
            }).await?;
            if found_events.len() > 0 {
                println!("{}", to_colored_json_auto(&found_events)?);
            } else {
                println!("No events found");
            }
            Ok(())
        },
        Commands::Watch { topic, search_term, acknowledge_searched, seek_minutes, limit, output_only_event_data, position, pulsarctl_env } =>{
            println!("Watching events on the topic {}", search_term);
            commands::watch::execute(&mut pulsar, &topic, &search_term, &ScanOptions {
                acknowledge_searched,
                seek_minutes,
                limit,
                output_only_event_data,
                position
            }).await?;
            Ok(())
        },
        _ => {
            println!("Not yet implemented");
            Ok(())
        }
    }
}
