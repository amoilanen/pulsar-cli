use pulsar::{Consumer, Pulsar};
use tokio::signal;
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;
use anyhow::Error;
use crate::SearchOptions;
use colored_json::to_colored_json_auto;
use crate::common::subscribe_to_topic;
use crate::common::{self, MessageConsumptionOptions};

const TIMEOUT_TO_READ_NEXT_EVENT_MILLIS: u64 = 5000;

async fn scan_messages<T: pulsar::Executor>(consumer: &mut Consumer<String, T>, search_term: &str, options: &SearchOptions) -> Result<(), Error> {
    let scan_options = MessageConsumptionOptions {
        only_message_data: options.output_only_event_data,
        acknowledge_consumed: options.acknowledge_searched,
        break_on_no_message: false,
        output_progress: false,
        limit: options.limit
    };
    common::scan_messages(consumer, TIMEOUT_TO_READ_NEXT_EVENT_MILLIS, &scan_options, |message, message_json| {
        let is_interested_in_message = message.payload.contains(search_term) || message.properties.contains(search_term);
        if is_interested_in_message {
            println!("{}", to_colored_json_auto(&message_json)?);
        }
        Ok(is_interested_in_message)
    }).await?;
    Ok(())
}

pub(crate) async fn execute<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str, search_term: &str, options: &SearchOptions) -> Result<(), Error> {
    let mut consumer = subscribe_to_topic(pulsar, topic, crate::commands::DEFAULT_SUBSCRIPTION_NAME, &options.position).await?;

    let seek_offset = (SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() - Duration::from_secs((options.seek_minutes * 60) as u64).as_millis()) as u64;
    consumer.seek(None, None, Some(seek_offset), pulsar.clone()).await?;

    tokio::select! {
        result = scan_messages(&mut consumer, search_term, options) => result,
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