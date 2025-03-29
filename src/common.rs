use pulsar::{Consumer, ConsumerOptions, Pulsar, SubType};
use pulsar::consumer::InitialPosition;
use crate::InitialPosition as RequestedInitialPosition;
use anyhow::Error;

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
        .with_subscription_type(SubType::Exclusive)
        .with_subscription(subscription_name)
        .with_options(ConsumerOptions::default()
            .with_initial_position(InitialPosition::Earliest))
        .build()
        .await?;
    consumer.unsubscribe().await?;
    Ok(())

}