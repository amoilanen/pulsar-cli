use futures::StreamExt;
use pulsar::{Pulsar, SubType, Consumer, ConsumerOptions, consumer::InitialPosition};
use pulsar::message::proto::{MessageMetadata, KeyValue};
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;
use crate::InitialPosition as RequestedInitialPosition;
use serde_json::{Value, json};
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

        println!("Received message '{:?}' id='{:?}'", payload, &event.message_id());
        //TODO: Also search by event properties, not only data
        if payload.contains(search_term) {
            println!("Found message id='{:?}'", event.message_id());
            let properties = &event.payload.metadata.properties;
            let mut event_properties = serde_json::Map::new();
            for key_value in properties.into_iter() {
                print!("properties: key = {:?}, value = {:?}", &key_value.key, &key_value.value);
                event_properties.insert(key_value.key.clone(), json!(key_value.value));
            }

            let json_payload = serde_json::from_str(&payload)?;
            let mut found_event = serde_json::Map::new();
            found_event.insert("properties".to_owned(), serde_json::Value::Object(event_properties));
            found_event.insert("data".to_owned(), json_payload);

            //let found_event = json_payload;
            found_events.push(serde_json::Value::Object(found_event));
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