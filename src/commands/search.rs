use pulsar::Pulsar;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use serde_json::Value;
use anyhow::Error;
use crate::ScanOptions;
use crate::common::{self, MessageConsumptionOptions};

pub(crate) async fn execute<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str, search_term: &str, options: &ScanOptions) -> Result<Vec<Value>, Error> {
    let mut consumer = common::subscribe_to_topic(pulsar, topic, crate::commands::DEFAULT_SUBSCRIPTION_NAME, &options.position).await?;

    let seek_offset = (SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() - Duration::from_secs((options.seek_minutes * 60) as u64).as_millis()) as u64;
    let consumer_ids = consumer.consumer_id().iter().map(|id| id.to_string()).collect();
    consumer.seek(Some(consumer_ids), None, Some(seek_offset), pulsar.clone()).await?;

    let timeout_to_read_next_message_millis = 5000;
    let mut found_messages: Vec<Value> = Vec::new();

    let scan_options = MessageConsumptionOptions {
        only_message_data: options.output_only_message_data,
        acknowledge_consumed: options.acknowledge_searched,
        break_on_no_message: true,
        output_progress: true,
        limit: options.limit
    };
    common::scan_messages(&mut consumer, timeout_to_read_next_message_millis, &scan_options, |message, message_json| {
        let is_interested_in_message = message.payload.contains(search_term) || message.properties.contains(search_term);
        if is_interested_in_message {
            found_messages.push(message_json);
        }
        Ok(is_interested_in_message)
    }).await?;
    consumer.close().await.expect("Unable to close consumer");
    Ok(found_messages)
}
