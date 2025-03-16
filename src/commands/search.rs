use futures::StreamExt;
use pulsar::{Pulsar, SubType, Consumer, ConsumerOptions, consumer::InitialPosition};
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;
use crate::InitialPosition as RequestedInitialPosition;
use serde_json::Value;
use tokio::time::timeout; 
use anyhow::{Error, anyhow};
use crate::SearchOptions;

// TODO: Implement also support for acknowledge_searched: bool, limit: u32, output_only_event_data: bool
pub(crate) async fn execute<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str, search_term: &str, options: &SearchOptions) -> Result<Vec<Value>, Error> {

    //TODO: Extract common command parts which are shared with, for example, "attach" and can be shared
    let initial_position = match options.position {
        RequestedInitialPosition::Earliest =>InitialPosition::Earliest,
        RequestedInitialPosition::Latest => InitialPosition::Latest
    };
    let mut consumer: Consumer<String, _> = pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription_type(SubType::Shared)
        .with_subscription(super::DEFAULT_SUBSCRIPTION_NAME)
        .with_options(ConsumerOptions::default()
            .with_initial_position(initial_position))
        .build()
        .await?;

    let seek_offset = (SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() - Duration::from_secs((options.seek_minutes * 60) as u64).as_millis()) as u64;
    consumer.seek(None, None, Some(seek_offset), pulsar.clone()).await?;

    let timeout_to_read_next_event_millis = 5000;
    //TODO: Print stats of how many events are scanned, i.e. "Scanned x events" where x would change
    let mut found_events: Vec<Value> = Vec::new();
    let mut found_events_count: usize = 0;
    while let Some(msg) = timeout(Duration::from_millis(timeout_to_read_next_event_millis), consumer.next()).await.ok().flatten() {
        let event = msg?;
        if options.acknowledge_searched {
            consumer.ack(&event).await?;
        }
        let payload = &event.deserialize()?;
        let properties = &event.payload.metadata.properties;

        //TODO: Serialize properties to JSON
        println!("Properties {:?}", properties);
        println!("Received message '{:?}' id='{:?}'", payload, &event.message_id());
        //TODO: Also search by event properties, not only data
        //TODO: Also push properties of the message into the result, not only the payload
        if payload.contains(search_term) {
            println!("Found message id='{:?}'", event.message_id());
            let json_payload = serde_json::from_str(&payload)?;
            found_events.push(json_payload);
            found_events_count = found_events_count + 1;
        }

        if found_events_count > options.limit {
            println!("Found all events");
            consumer.close().await.expect("Unable to close consumer");
            break;
        }
    }

    Ok(found_events)
}