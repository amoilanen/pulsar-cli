use pulsar::{Pulsar, producer::MessageBuilder};
use serde_json::Value;
use anyhow::{Error, anyhow};
use crate::io::read_from_input;

pub(crate) async fn execute<T: pulsar::Executor>(pulsar: &mut Pulsar<T>, topic: &str) -> Result<(), Error> {
    let input = read_from_input()?;
    let parsed: Value = serde_json::from_str(&input)?;
    let properties = &parsed["properties"];
    let data = &parsed["data"];

    let mut producer = pulsar
        .producer()
        .with_topic(topic)
        .build()
        .await?;

    let mut message = MessageBuilder::new(&mut producer)
        .with_content(serde_json::to_string(data)?.as_bytes().to_vec());

    if let Value::Object(obj) = properties {
        for (key, value) in obj {
            message = message.with_property(key, value.as_str().ok_or(anyhow!("Value {:?} cannot be converted to a String", value))?);
        }
    }
    message.send().await?.await?;
    Ok(())
}