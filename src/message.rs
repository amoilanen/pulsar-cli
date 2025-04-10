use pulsar::consumer::message::Message;
use serde_json::json;
use anyhow::Error;

pub(crate) struct PulsarMessage {
    pub(crate) payload: String,
    pub(crate) properties: String
}

impl PulsarMessage {

    pub(crate) fn to_json(&self) -> Result<serde_json::Value, Error> {
        let mut result = serde_json::Map::new();
        result.insert("properties".to_owned(), serde_json::from_str(&self.properties)?);
        result.insert("data".to_owned(), serde_json::from_str(&self.payload)?);
        Ok(serde_json::Value::Object(result))
    }
}

pub(crate) fn get_properties<T>(event: &Message<T>) -> Result<String, Error> {
    let properties = &event.payload.metadata.properties;
    let mut event_properties = serde_json::Map::new();
    for key_value in properties.into_iter() {
        event_properties.insert(key_value.key.clone(), json!(key_value.value));
    }
    serde_json::to_string(&serde_json::Value::Object(event_properties)).map_err(Error::from)
}