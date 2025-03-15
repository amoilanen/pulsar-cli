use pulsar::{Consumer, ConsumerOptions, Pulsar, SubType};
use pulsar::consumer::InitialPosition;
use crate::InitialPosition as RequestedInitialPosition;
use anyhow::Error;

pub(crate) async fn execute<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str, position: &RequestedInitialPosition) -> Result<(), Error> {
    let initial_position = match position {
        RequestedInitialPosition::Earliest =>InitialPosition::Earliest,
        RequestedInitialPosition::Latest => InitialPosition::Latest
    };
    let _: Consumer<String, _> = pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription_type(SubType::Exclusive)
        .with_subscription(super::DEFAULT_SUBSCRIPTION_NAME)
        .with_options(ConsumerOptions::default()
            .with_initial_position(initial_position))
        .build()
        .await?;
    Ok(())
}