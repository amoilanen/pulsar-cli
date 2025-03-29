use pulsar::Pulsar;
use anyhow::Error;
use crate::InitialPosition as RequestedInitialPosition;
use crate::common::subscribe_to_topic;

pub(crate) async fn execute<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str, position: &RequestedInitialPosition) -> Result<(), Error> {
    subscribe_to_topic(pulsar, topic, crate::commands::DEFAULT_SUBSCRIPTION_NAME, position).await?;
    Ok(())
}