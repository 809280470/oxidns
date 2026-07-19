// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Serialized runtime reload control for provider instances.

use std::sync::Arc;

use thiserror::Error;
use tokio::sync::Mutex;

use crate::infra::error::DnsError;
use crate::plugin::provider::Provider;

#[derive(Debug, Error)]
pub(crate) enum ProviderReloadError {
    #[error("provider '{tag}' reload is already in progress")]
    Busy { tag: String },
    #[error(transparent)]
    Failed(#[from] DnsError),
}

impl ProviderReloadError {
    pub(crate) fn into_dns_error(self) -> DnsError {
        match self {
            Self::Busy { .. } => DnsError::plugin(self.to_string()),
            Self::Failed(error) => error,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ProviderRuntimeControl {
    provider: Arc<dyn Provider>,
    reload_lock: Mutex<()>,
}

impl ProviderRuntimeControl {
    pub(crate) fn new(provider: Arc<dyn Provider>) -> Self {
        Self {
            provider,
            reload_lock: Mutex::new(()),
        }
    }

    pub(crate) async fn reload(&self) -> Result<(), ProviderReloadError> {
        let _guard = self
            .reload_lock
            .try_lock()
            .map_err(|_| ProviderReloadError::Busy {
                tag: self.provider.tag().to_string(),
            })?;
        self.provider.reload().await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use tokio::sync::Notify;

    use super::*;
    use crate::infra::error::Result as DnsResult;
    use crate::plugin::Plugin;

    #[derive(Debug)]
    struct BlockingProvider {
        reloads: AtomicUsize,
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
            let reload = self.reloads.fetch_add(1, Ordering::Relaxed);
            if reload == 0 {
                self.started.notify_one();
                self.release.notified().await;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn concurrent_reload_is_rejected_instead_of_queued() {
        let provider = Arc::new(BlockingProvider {
            reloads: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let control = Arc::new(ProviderRuntimeControl::new(provider.clone()));
        let first_control = control.clone();
        let first = tokio::spawn(async move { first_control.reload().await });

        provider.started.notified().await;
        let second = control.reload().await;
        assert!(matches!(
            second,
            Err(ProviderReloadError::Busy { ref tag }) if tag == "blocking"
        ));
        assert_eq!(provider.reloads.load(Ordering::Relaxed), 1);

        provider.release.notify_one();
        first
            .await
            .expect("first reload task should finish")
            .unwrap();
        control.reload().await.unwrap();
        assert_eq!(provider.reloads.load(Ordering::Relaxed), 2);
    }
}
