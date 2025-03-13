use clap::{Command, Parser, Subcommand};
use pulsar::{Pulsar, TokioExecutor};
use anyhow::Error;

mod commands;
mod io;

#[derive(Parser)]
#[command(name = "pulsar-cli", about = "A command line tool to simpify publishing and viewing Pulsar messages")]
struct CommandLineInputs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start subscribing to the topic events
    Attach {
        #[arg(short, long)]
        topic: String
    },
    /// Stop subscribing to the topic events
    Detach {
        #[arg(short, long)]
        topic: String
    },
    /// Publish
    Publish {
        #[arg(short, long)]
        topic: String
    },
    Search {
        #[arg(short, long)]
        topic: String,
        #[arg(short, long)]
        search_term: String
        //TODO: Limit, ability to output both properties and messages, or just messages
    }
    //TODO: View the stats for a given topic and its partitions
    //TODO: Peek command
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = CommandLineInputs::parse();

    let addr = "pulsar://localhost:6650".to_string();
    let builder = Pulsar::builder(addr, TokioExecutor);
    let mut pulsar: Pulsar<_> = builder.build().await?;

    match cli.command {
        Commands::Attach { topic } => {
            println!("Subscribing to {:?}", topic);
            commands::attach::execute(&mut pulsar, &topic).await?;
            Ok(())
        },
        Commands::Detach { topic } => {
            println!("Unsubscribing from {:?}", topic);
            commands::detach::execute(&mut pulsar, &topic).await?;
            Ok(())
        }
        Commands::Publish { topic } => {
            println!("Publishing to {:?}", topic);
            commands::publish::execute(&mut pulsar, &topic).await?;
            println!("Successfully published to {:?}", topic);
            Ok(())
        }
        _ => {
            println!("Not yet implemented");
            Ok(())
        }
    }
}
