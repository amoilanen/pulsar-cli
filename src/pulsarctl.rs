use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::fs::File;
use std::io::BufReader;
use anyhow::{Error, Result};

#[derive(Debug, Deserialize)]
pub(crate) struct PulsarConfig {
    #[serde(rename = "auth-info")]
    pub(crate) auth_info: HashMap<String, PulsarContext>,
    pub(crate) contexts: HashMap<String, PulsarContextSettings>,
    #[serde(rename = "current-context")]
    pub(crate) current_context: String
}

#[derive(Debug, Deserialize)]
pub(crate) struct PulsarContextSettings {
    #[serde(rename = "admin-service-url")]
    pub(crate) admin_service_url: String,
    #[serde(rename = "bookie-service-url")]
    pub(crate) bookie_service_url: String
}

#[derive(Debug, Deserialize)]
pub(crate) struct PulsarContext {
    #[serde(rename = "locationoforigin")]
    pub(crate) location_of_origin: String,
    pub(crate) tls_trust_certs_file_path: String,
    pub(crate) tls_allow_insecure_connection: bool,
    pub(crate) token: String,
    #[serde(rename = "tokenFile")]
    pub(crate) token_file: String,
    pub(crate) issuer_endpoint: String,
    pub(crate) client_id: String,
    pub(crate) audience: String,
    pub(crate) scope: String,
    pub(crate) key_file: String
}

pub(crate) fn read_config() -> Result<PulsarConfig, Error> {
    let home_dir = env::var("HOME")
        .or_else(|_| env::var("USERPROFILE"))
        .expect("Could not determine home directory");

    let mut path = PathBuf::from(home_dir);
    path.push(".config/pulsar/config");
    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let config: PulsarConfig = serde_yaml::from_reader(reader)?;
    Ok(config)
}