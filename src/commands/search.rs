use futures::StreamExt;
use pulsar::Pulsar;
use crate::message;
use crate::message::FoundMessage;
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;
use std::io::Write;
use serde_json::Value;
use tokio::time::timeout; 
use anyhow::Error;
use crate::SearchOptions;
use crate::common::subscribe_to_topic;

pub(crate) async fn execute<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str, search_term: &str, options: &SearchOptions) -> Result<Vec<Value>, Error> {
    let mut consumer = subscribe_to_topic(pulsar, topic, crate::commands::DEFAULT_SUBSCRIPTION_NAME, &options.position).await?;

    let seek_offset = (SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() - Duration::from_secs((options.seek_minutes * 60) as u64).as_millis()) as u64;
    consumer.seek(None, None, Some(seek_offset), pulsar.clone()).await?;

    let timeout_to_read_next_event_millis = 5000;
    let mut scanned_events: usize = 0;
    let mut found_events: Vec<Value> = Vec::new();
    let mut found_events_count: usize = 0;
    while let Some(msg) = timeout(Duration::from_millis(timeout_to_read_next_event_millis), consumer.next()).await.ok().flatten() {
        scanned_events = scanned_events + 1;
        print!("\rSearched {} items...", scanned_events);
        std::io::stdout().flush()?; // Ensure immediate output

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
            found_events.push(found_event);
            found_events_count = found_events_count + 1;
        }
        if found_events_count >= options.limit {
            break;
        }
    }
    print!("\r{}\r", " ".repeat(100));
    std::io::stdout().flush()?;

    consumer.close().await.expect("Unable to close consumer");
    Ok(found_events)
}