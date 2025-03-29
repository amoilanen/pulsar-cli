use pulsar::Pulsar;
use anyhow::Error;
use crate::common::unsubscribe_from_topic;

pub(crate) async fn execute<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str) -> Result<(), Error> {
    unsubscribe_from_topic(pulsar, topic, crate::commands::DEFAULT_SUBSCRIPTION_NAME).await
}