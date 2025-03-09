use pulsar::{Consumer, ConsumerOptions, Pulsar, SubType};
use pulsar::consumer::InitialPosition;
use anyhow::Error;

pub(crate) async fn execute<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str) -> Result<(), Error> {
    let mut consumer: Consumer<String, _> = pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription_type(SubType::Exclusive)
        .with_subscription(super::DEFAULT_SUBSCRIPTION_NAME)
        .with_options(ConsumerOptions::default()
            .with_initial_position(InitialPosition::Earliest))
        .build()
        .await?;
    consumer.unsubscribe().await?;
    Ok(())
}