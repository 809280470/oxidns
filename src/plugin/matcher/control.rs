// SPDX-FileCopyrightText: 2025 Sven Shi
// SPDX-License-Identifier: GPL-3.0-or-later

//! Runtime switch shared by every configured matcher instance.

#[cfg(any(feature = "api", test))]
use std::sync::Arc;
#[cfg(any(feature = "api", test))]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(any(feature = "api", test))]
use async_trait::async_trait;

#[cfg(any(feature = "api", test))]
use crate::core::context::DnsContext;
#[cfg(any(feature = "api", test))]
use crate::infra::error::Result as DnsResult;
#[cfg(any(feature = "api", test))]
use crate::plugin::Plugin;
#[cfg(any(feature = "api", test))]
use crate::plugin::matcher::Matcher;

#[cfg(any(feature = "api", test))]
#[derive(Debug)]
pub(crate) struct MatcherRuntimeControl {
    enabled: AtomicBool,
}

#[cfg(any(feature = "api", test))]
impl MatcherRuntimeControl {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
        }
    }

    #[inline]
    pub(crate) fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    #[cfg(any(feature = "api", test))]
    pub(crate) fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }
}

#[cfg(any(feature = "api", test))]
#[derive(Debug)]
struct ControlledMatcher {
    inner: Arc<dyn Matcher>,
    control: Arc<MatcherRuntimeControl>,
}

#[cfg(any(feature = "api", test))]
#[async_trait]
impl Plugin for ControlledMatcher {
    fn tag(&self) -> &str {
        self.inner.tag()
    }

    async fn destroy(&self) -> DnsResult<()> {
        self.inner.destroy().await
    }
}

#[cfg(any(feature = "api", test))]
impl Matcher for ControlledMatcher {
    #[inline]
    fn is_match(&self, context: &mut DnsContext) -> bool {
        self.control.enabled() && self.inner.is_match(context)
    }
}

#[cfg(any(feature = "api", test))]
pub(crate) fn attach_runtime_control(
    matcher: Arc<dyn Matcher>,
) -> (Arc<dyn Matcher>, Arc<MatcherRuntimeControl>) {
    let control = Arc::new(MatcherRuntimeControl::new());
    (
        Arc::new(ControlledMatcher {
            inner: matcher,
            control: control.clone(),
        }),
        control,
    )
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::plugin::matcher::MatcherRef;
    use crate::proto::Message;

    #[derive(Debug)]
    struct CountingMatcher {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Plugin for CountingMatcher {
        fn tag(&self) -> &str {
            "counting"
        }
    }

    impl Matcher for CountingMatcher {
        fn is_match(&self, _context: &mut DnsContext) -> bool {
            self.calls.fetch_add(1, Ordering::Relaxed);
            true
        }
    }

    fn context() -> DnsContext {
        DnsContext::new(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 5300)),
            Message::new(),
        )
    }

    #[test]
    fn disabled_matcher_short_circuits_and_reverse_still_applies() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (matcher, control) = attach_runtime_control(Arc::new(CountingMatcher {
            calls: calls.clone(),
        }));
        let normal = MatcherRef::new(matcher.clone(), false);
        let reversed = MatcherRef::new(matcher, true);
        let mut context = context();

        assert!(normal.is_match(&mut context));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        control.set_enabled(false);
        assert!(!normal.is_match(&mut context));
        assert!(reversed.is_match(&mut context));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        control.set_enabled(true);
        assert!(normal.is_match(&mut context));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }
}
