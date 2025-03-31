use futures::StreamExt;
use pulsar::{Consumer, Pulsar};
use tokio::signal;
use crate::message;
use crate::message::FoundMessage;
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;
use tokio::time::timeout; 
use anyhow::Error;
use crate::SearchOptions;
use colored_json::to_colored_json_auto;
use crate::common::subscribe_to_topic;

const TIMEOUT_TO_READ_NEXT_EVENT_MILLIS: u64 = 5000;

//TODO: Re-factor duplication with search
async fn receive_next_message_in_loop<T: pulsar::Executor>(consumer: &mut Consumer<String, T>, search_term: &str, options: &SearchOptions) -> Result<(), Error> {
    let mut found_events_count: usize = 0;
    loop {
        if let Some(msg) = timeout(Duration::from_millis(TIMEOUT_TO_READ_NEXT_EVENT_MILLIS), consumer.next()).await.ok().flatten() {
            let event = msg?;
            if options.acknowledge_searched {
                consumer.ack(&event).await?;
            }
            let payload = &event.deserialize()?;
            let properties = message::get_properties(&event)?;
            if payload.contains(search_term) || serde_json::to_string(&properties)?.contains(search_term) {
                let full_found_event = FoundMessage {
                    payload: payload.to_owned(),
                    properties
                };
                let found_event = if options.output_only_event_data {
                    serde_json::from_str(payload)?
                } else {
                    full_found_event.to_json()?
                };
                println!("{}", to_colored_json_auto(&found_event)?);
                found_events_count = found_events_count + 1;
            }
            if found_events_count >= options.limit {
                break;
            }
        }
    }
    Ok(())
}

pub(crate) async fn execute<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str, search_term: &str, options: &SearchOptions) -> Result<(), Error> {
    let mut consumer = subscribe_to_topic(pulsar, topic, crate::commands::DEFAULT_SUBSCRIPTION_NAME, &options.position).await?;

    let seek_offset = (SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() - Duration::from_secs((options.seek_minutes * 60) as u64).as_millis()) as u64;
    consumer.seek(None, None, Some(seek_offset), pulsar.clone()).await?;

    tokio::select! {
        result = receive_next_message_in_loop(&mut consumer, search_term, options) => result,
        _ = async {
            signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
        } => {
            println!("\nFinished watching...");
            Ok(())
        }
    }?;
    consumer.close().await.expect("Unable to close consumer");
    Ok(())
}