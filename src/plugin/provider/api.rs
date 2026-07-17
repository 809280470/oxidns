// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Provider management API integration.

#[cfg(not(feature = "api"))]
use std::sync::Arc;

#[cfg(not(feature = "api"))]
use crate::infra::error::Result as DnsResult;
#[cfg(not(feature = "api"))]
use crate::plugin::PluginRegistry;

#[cfg(feature = "api")]
mod enabled {
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use http::{Request, StatusCode};
    use serde::Serialize;

    use crate::api::{ApiHandler, json_error, json_ok};
    use crate::infra::error::Result as DnsResult;
    use crate::plugin::{self, PluginRegistry};
    use crate::register_plugin_api;

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
    }

    #[async_trait]
    impl ApiHandler for ProviderReloadHandler {
        async fn handle(&self, _request: Request<Bytes>) -> crate::api::ApiResponse {
            match plugin::reload_provider(&self.tag).await {
                Ok(()) => json_ok(
                    StatusCode::OK,
                    &ProviderReloadResponse {
                        ok: true,
                        action: "reload_provider",
                        provider: self.tag.clone(),
                        status: "reloaded",
                    },
                ),
                Err(err) => json_error(
                    StatusCode::BAD_REQUEST,
                    "provider_reload_failed",
                    err.to_string(),
                ),
            }
        }
    }

    pub(crate) fn register_reload_api_route(
        _registry: Arc<PluginRegistry>,
        tag: &str,
    ) -> DnsResult<()> {
        register_plugin_api!(
            tag,
            POST "/reload" => ProviderReloadHandler {
                tag: tag.to_string(),
            },
        )
    }
}

#[cfg(feature = "api")]
pub(crate) use enabled::register_reload_api_route;

#[cfg(not(feature = "api"))]
pub(crate) fn register_reload_api_route(
    _registry: Arc<PluginRegistry>,
    _tag: &str,
) -> DnsResult<()> {
    Ok(())
}
