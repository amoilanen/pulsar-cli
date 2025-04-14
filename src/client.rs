use pulsar::{Pulsar, TokioExecutor};
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use anyhow::{anyhow, Error, Result};
use crate::pulsarctl::{self, PulsarConfig};

async fn build_local_client() -> Result<Pulsar<TokioExecutor>, Error> {
    let addr = std::env::var("PULSAR_ADDRESS").unwrap_or_else(|_| "pulsar://localhost:6650".to_string());
    let builder = Pulsar::builder(addr, TokioExecutor);
    builder.build().await.map_err(anyhow::Error::from)
}

async fn build_pulsarctl_client(pulsarctl_env: String) -> Result<Pulsar<TokioExecutor>, Error> {
    let config: PulsarConfig = pulsarctl::read_config()?;
    let context_settings = config.contexts.get(&pulsarctl_env).ok_or(anyhow!("Could not find environment {} in local pulsarctl config", pulsarctl_env))?;
    let context = config.auth_info.get(&pulsarctl_env).ok_or(anyhow!("Could not find environment {} in local pulsarctl config", pulsarctl_env))?;
    let addr = context_settings.bookie_service_url.replace("https", "pulsar+ssl");
    let builder = Pulsar::builder(addr, TokioExecutor);
    builder.with_auth_provider(OAuth2Authentication::client_credentials(OAuth2Params {
        issuer_url: context.issuer_endpoint.to_string(),
        credentials_url: if context.key_file.starts_with("file://") {
            context.key_file.to_string()
        } else {
            format!("file://{}", context.key_file).to_string()
        },
        audience: Some(context.audience.to_string()),
        scope: None,
    })).build().await.map_err(anyhow::Error::from)
}

pub(crate) async fn build_client(pulsarctl_env: Option<String>) -> Result<Pulsar<TokioExecutor>, Error> {
    match pulsarctl_env {
        Some(env) => {
            build_pulsarctl_client(env).await
        },
        None => {
            build_local_client().await
        }
    }
}