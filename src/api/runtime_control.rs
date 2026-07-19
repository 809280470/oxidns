// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Domain-specific runtime controls exposed through the management API.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http::{Request, StatusCode};
use serde::Serialize;

use crate::api::{ApiHandler, ApiRegister, json_error, json_ok};
use crate::infra::error::Result as DnsResult;
use crate::plugin::PluginRuntime;
use crate::plugin::matcher::MatcherRuntimeControl;
use crate::plugin::provider::{ProviderReloadError, ProviderRuntimeControl};
use crate::plugin::runtime_control::PluginRuntimeControl;

#[derive(Debug, Serialize)]
struct MatcherStatusResponse {
    ok: bool,
    matcher: String,
    enabled: bool,
}

#[derive(Debug)]
struct MatcherStatusHandler {
    tag: String,
    control: Arc<MatcherRuntimeControl>,
    desired: Option<bool>,
}

#[async_trait]
impl ApiHandler for MatcherStatusHandler {
    async fn handle(&self, _request: Request<Bytes>) -> crate::api::ApiResponse {
        if let Some(enabled) = self.desired {
            self.control.set_enabled(enabled);
            tracing::info!(
                matcher = %self.tag,
                enabled,
                "matcher runtime control updated"
            );
        }
        json_ok(
            StatusCode::OK,
            &MatcherStatusResponse {
                ok: true,
                matcher: self.tag.clone(),
                enabled: self.control.enabled(),
            },
        )
    }
}

#[derive(Debug, Serialize)]
struct ProviderReloadResponse {
    ok: bool,
    action: &'static str,
    provider: String,
    status: &'static str,
}

#[derive(Debug)]
struct ProviderReloadHandler {
    tag: String,
    control: Arc<ProviderRuntimeControl>,
}

#[async_trait]
impl ApiHandler for ProviderReloadHandler {
    async fn handle(&self, _request: Request<Bytes>) -> crate::api::ApiResponse {
        match self.control.reload().await {
            Ok(()) => json_ok(
                StatusCode::OK,
                &ProviderReloadResponse {
                    ok: true,
                    action: "reload_provider",
                    provider: self.tag.clone(),
                    status: "reloaded",
                },
            ),
            Err(error @ ProviderReloadError::Busy { .. }) => json_error(
                StatusCode::CONFLICT,
                "provider_reload_busy",
                error.to_string(),
            ),
            Err(ProviderReloadError::Failed(error)) => json_error(
                StatusCode::BAD_REQUEST,
                "provider_reload_failed",
                error.to_string(),
            ),
        }
    }
}

pub(crate) fn register_plugin_runtime_control_routes(
    register: &ApiRegister,
    runtime: &PluginRuntime,
) -> DnsResult<()> {
    for (tag, control) in runtime.runtime_controls() {
        let plugin = register.plugin(&tag)?;
        match control {
            PluginRuntimeControl::Matcher(control) => {
                plugin.get(
                    "/status",
                    Arc::new(MatcherStatusHandler {
                        tag: tag.clone(),
                        control: control.clone(),
                        desired: None,
                    }),
                )?;
                plugin.post(
                    "/enable",
                    Arc::new(MatcherStatusHandler {
                        tag: tag.clone(),
                        control: control.clone(),
                        desired: Some(true),
                    }),
                )?;
                plugin.post(
                    "/disable",
                    Arc::new(MatcherStatusHandler {
                        tag,
                        control,
                        desired: Some(false),
                    }),
                )?;
            }
            PluginRuntimeControl::Provider(control) => {
                plugin.post("/reload", Arc::new(ProviderReloadHandler { tag, control }))?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use http_body_util::BodyExt;
    use tokio::sync::Notify;

    use super::*;
    use crate::plugin::Plugin;
    use crate::plugin::provider::Provider;

    #[derive(Debug)]
    struct BlockingProvider {
        started: Notify,
        release: Notify,
    }

    #[async_trait]
    impl Plugin for BlockingProvider {
        fn tag(&self) -> &str {
            "blocking"
        }
    }

    #[async_trait]
    impl Provider for BlockingProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn reload(&self) -> DnsResult<()> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn provider_handler_maps_concurrent_reload_to_conflict() {
        let provider = Arc::new(BlockingProvider {
            started: Notify::new(),
            release: Notify::new(),
        });
        let control = Arc::new(ProviderRuntimeControl::new(provider.clone()));
        let first_control = control.clone();
        let first = tokio::spawn(async move { first_control.reload().await });
        provider.started.notified().await;

        let response = ProviderReloadHandler {
            tag: "blocking".to_string(),
            control,
        }
        .handle(Request::new(Bytes::new()))
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body should collect")
            .to_bytes();
        let payload = serde_json::from_slice::<serde_json::Value>(&body)
            .expect("response should be valid json");
        assert_eq!(payload["code"], "provider_reload_busy");

        provider.release.notify_one();
        first
            .await
            .expect("first reload task should finish")
            .unwrap();
    }
}
