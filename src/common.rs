use pulsar::{Consumer, ConsumerOptions, Pulsar, SubType};
use pulsar::consumer::InitialPosition;
use std::io::Write;
use serde_json::Value;
use anyhow::Error;

use futures::StreamExt;
use std::time::Duration;
use tokio::time::timeout;

use crate::InitialPosition as RequestedInitialPosition;
use crate::message;
use crate::message::PulsarMessage;

pub(crate) struct MessageConsumptionOptions {
    pub(crate) only_message_data: bool,
    pub(crate) acknowledge_consumed: bool,
    pub(crate) break_on_no_message: bool,
    pub(crate) output_progress: bool,
    pub(crate) limit: usize
}

pub(crate) async fn scan_messages<F, T: pulsar::Executor>(consumer: &mut Consumer<String, T>, timeout_millis: u64, options: &MessageConsumptionOptions, mut handle_message: F) -> Result<(), Error>
where F:  FnMut(PulsarMessage, Value) -> Result<bool, Error> {
    let mut scanned_messages_count: usize = 0;
    let mut consumed_messages_count: usize = 0;
    loop {
        if let Some(msg) = timeout(Duration::from_millis(timeout_millis), consumer.next()).await.ok().flatten() {
            scanned_messages_count = scanned_messages_count + 1;
            if options.output_progress {
                print!("\rScanned {} messages...", scanned_messages_count);
                std::io::stdout().flush()?; // Ensure immediate output
            }
            let message = msg?;
            if options.acknowledge_consumed {
                consumer.ack(&message).await?;
            }
            let payload = &message.deserialize()?;
            let properties = message::get_properties(&message)?;
            let message_with_properties = PulsarMessage {
                payload: payload.to_owned(),
                properties
            };
            let consumed_event = if options.only_message_data {
                serde_json::from_str(payload)?
            } else {
                message_with_properties.to_json()?
            };
            if handle_message(message_with_properties, consumed_event)? {
                consumed_messages_count = consumed_messages_count + 1;
            }
            if consumed_messages_count >= options.limit {
                break;
            }
        } else if options.break_on_no_message {
            break;
        }
    }
    if options.output_progress {
        print!("\r{}\r", " ".repeat(100));
        std::io::stdout().flush()?;
    }
    Ok(())
}

pub(crate) async fn subscribe_to_topic<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str, subscription_name: &str, position: &RequestedInitialPosition) -> Result<Consumer<String, T>, Error> {
    let initial_position = match position {
        RequestedInitialPosition::Earliest =>InitialPosition::Earliest,
        RequestedInitialPosition::Latest => InitialPosition::Latest
    };
    let consumer: Consumer<String, T> = pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription_type(SubType::Shared)
        .with_subscription(subscription_name)
        .with_options(ConsumerOptions::default()
            .with_initial_position(initial_position))
        .build()
        .await?;
    Ok(consumer)
}

pub(crate) async fn unsubscribe_from_topic<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str, subscription_name: &str) -> Result<(), Error> {
    let mut consumer: Consumer<String, _> = pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription_type(SubType::Shared)
        .with_subscription(subscription_name)
        .with_options(ConsumerOptions::default()
            .with_initial_position(InitialPosition::Earliest))
        .build()
        .await?;
    consumer.unsubscribe().await?;
    Ok(())

}