use clap::{Parser, Subcommand};
use std::str::FromStr;
use anyhow::{Error, Result};
use colored_json::to_colored_json_auto;

mod commands;
mod io;
mod message;
mod common;
mod pulsarctl;
mod client;

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
    /// Start subscribing to the topic messages
    Attach {
        #[arg(short, long)]
        topic: String,
        #[arg(long, default_value = "earliest")]
        position: InitialPosition,
        #[arg(short, long)]
        pulsarctl_env: Option<String>
    },
    /// Stop subscribing to the topic messages
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
        //TODO: Add option to add a delay between publishing each individual message
    },
    /// Search messages from a given topic
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
        output_only_message_data: bool,
        #[arg(long, default_value = "earliest")]
        position: InitialPosition,
        #[arg(short, long)]
        pulsarctl_env: Option<String>
    },
    /// Watch messages from a given topic
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
        output_only_message_data: bool,
        #[arg(long, default_value = "latest")]
        position: InitialPosition,
        #[arg(short, long)]
        pulsarctl_env: Option<String>
    }
    //TODO: View the stats for a given topic and its partitions
    //TODO: Peek command
}

impl Commands {
    fn get_pulsarctl_env(&self) -> Option<String> {
        match self {
            Commands::Attach { pulsarctl_env, .. } => pulsarctl_env.clone(),
            Commands::Detach { pulsarctl_env, .. } => pulsarctl_env.clone(),
            Commands::Publish { pulsarctl_env, .. } => pulsarctl_env.clone(),
            Commands::Search { pulsarctl_env, .. } => pulsarctl_env.clone(),
            Commands::Watch { pulsarctl_env, .. } => pulsarctl_env.clone()
        }
    }
}

struct ScanOptions {
    acknowledge_searched: bool,
    seek_minutes: usize,
    limit: usize,
    output_only_message_data: bool,
    position: InitialPosition
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = CommandLineInputs::parse();
    let pulsarctl_env = cli.command.get_pulsarctl_env();
    let mut pulsar = client::build_client(pulsarctl_env).await?;

    match cli.command {
        Commands::Attach { topic, position, .. } => {
            println!("Attaching to {:?}", topic);
            commands::attach::execute(&mut pulsar, &topic, &position).await?;
            Ok(())
        },
        Commands::Detach { topic, .. } => {
            println!("Detaching from {:?}", topic);
            commands::detach::execute(&mut pulsar, &topic).await?;
            Ok(())
        }
        Commands::Publish { topic, .. } => {
            println!("Publishing to {:?}", topic);
            commands::publish::execute(&mut pulsar, &topic).await?;
            println!("Successfully published to {:?}", topic);
            Ok(())
        }
        Commands::Search { topic, search_term, acknowledge_searched, seek_minutes, limit, output_only_message_data, position, .. } =>{
            println!("Searching for messages using search term {}", search_term);
            let found_messages = commands::search::execute(&mut pulsar, &topic, &search_term, &ScanOptions {
                acknowledge_searched,
                seek_minutes,
                limit,
                output_only_message_data,
                position
            }).await?;
            if found_messages.len() > 0 {
                println!("{}", to_colored_json_auto(&found_messages)?);
            } else {
                println!("No messages found");
            }
            Ok(())
        },
        Commands::Watch { topic, search_term, acknowledge_searched, seek_minutes, limit, output_only_message_data, position, .. } =>{
            println!("Watching messages on the topic {}", search_term);
            commands::watch::execute(&mut pulsar, &topic, &search_term, &ScanOptions {
                acknowledge_searched,
                seek_minutes,
                limit,
                output_only_message_data,
                position
            }).await?;
            Ok(())
        }
    }
}
