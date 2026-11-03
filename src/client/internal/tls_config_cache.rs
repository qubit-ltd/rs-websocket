// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use crate::error::WsError;
use crate::error::WsErrorKind;
use crate::options::WsTrustRoots;

/// Per-client cache for a TLS configuration and its resumption state.
///
/// The cache is lazy so building a client and using `ws` never initializes TLS.
/// Its entry is keyed by the active provider allocation: if the process has no
/// installed provider on one connection and installs one later, the next WSS
/// connection replaces the fallback entry. Configurations are not shared
/// between independently built clients, which keeps custom trust policies and
/// TLS resumption state isolated. Cloned clients share this cache through an
/// `Arc`.
#[derive(Default)]
pub(crate) struct TlsConfigCache {
    /// The single cached config, protected while checking, building, or
    /// replacing it.
    entry: Mutex<Option<CachedConfig>>,
    /// Lazily initialized fallback provider, shared by clones of this client
    /// cache.
    fallback_provider: OnceLock<Arc<rustls::crypto::CryptoProvider>>,
}

/// One successfully constructed config, associated with the provider allocation
/// used to build it. Provider identity is compared with `Arc::ptr_eq`.
struct CachedConfig {
    /// Keeps the provider allocation alive and records the config's provider
    /// identity.
    provider: Arc<rustls::crypto::CryptoProvider>,
    /// Config built for `provider` and this cache's fixed trust-root policy.
    config: Arc<rustls::ClientConfig>,
}

impl TlsConfigCache {
    /// Returns a cached TLS configuration for the currently effective provider.
    ///
    /// The entry mutex is held while selecting the provider and checking or
    /// building the config. Construction is synchronous; the mutex is released
    /// before the caller awaits the network handshake. Failed construction is
    /// not stored, so a later WSS attempt can retry. The `trust_roots` policy
    /// must remain fixed for this cache instance's lifetime. This holds for
    /// `WsClient`: its options are fixed after `build`, and independently built
    /// clients have independent caches. The cache key therefore needs only
    /// provider identity; changing trust roots within one cache is unsupported.
    ///
    /// # Parameters
    ///
    /// - `trust_roots`: Immutable trust-root policy captured by the client.
    ///
    /// # Returns
    ///
    /// A shared TLS client configuration, or `InvalidConfig` when the selected
    /// provider cannot build a safe TLS configuration.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when the selected provider has no safe protocol
    /// versions.
    pub(crate) fn get(&self, trust_roots: &WsTrustRoots) -> Result<Arc<rustls::ClientConfig>, WsError> {
        let mut entry = self.entry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let provider = self.effective_provider();
        Self::get_with_entry(&mut entry, trust_roots, provider)
    }

    /// Reuses or builds a config for a provider identity already selected by
    /// the caller. Kept separate so provider transitions can be tested without
    /// mutating rustls's process-global provider in the test process.
    #[cfg(test)]
    fn get_for_provider(
        &self,
        trust_roots: &WsTrustRoots,
        provider: Arc<rustls::crypto::CryptoProvider>,
    ) -> Result<Arc<rustls::ClientConfig>, WsError> {
        let mut entry = self.entry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::get_with_entry(&mut entry, trust_roots, provider)
    }

    /// Reuses the entry when its provider allocation matches, or builds and
    /// stores a config for the supplied provider.
    ///
    /// The caller must hold the mutex that protects `entry` for this entire
    /// call. Config construction is synchronous and occurs while that lock is
    /// held. If construction fails, `entry` is left unchanged so a later call
    /// can retry. The trust-root policy is fixed for the lifetime of the cache
    /// containing this entry; the provider-only key depends on each built
    /// client's immutable options and its private cache.
    ///
    /// # Parameters
    ///
    /// - `entry`: The locked cache slot to inspect or replace.
    /// - `trust_roots`: The cache owner's fixed root-certificate policy.
    /// - `provider`: The provider allocation used to identify or build the
    ///   config.
    ///
    /// # Returns
    ///
    /// The cached or newly built shared client config.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` if the provider cannot supply safe protocol
    /// versions. On error, no new entry is cached.
    fn get_with_entry(
        entry: &mut Option<CachedConfig>,
        trust_roots: &WsTrustRoots,
        provider: Arc<rustls::crypto::CryptoProvider>,
    ) -> Result<Arc<rustls::ClientConfig>, WsError> {
        if let Some(cached) = entry.as_ref()
            && Arc::ptr_eq(&cached.provider, &provider)
        {
            return Ok(Arc::clone(&cached.config));
        }

        let config = Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
                .with_safe_default_protocol_versions()
                .map_err(|_| WsError::new(WsErrorKind::InvalidConfig))?
                .with_root_certificates(build_root_store(trust_roots))
                .with_no_client_auth(),
        );
        *entry = Some(CachedConfig {
            provider,
            config: Arc::clone(&config),
        });
        Ok(config)
    }

    /// Selects the process-installed provider, or this cache's lazily
    /// initialized stable aws-lc fallback without changing the process
    /// default. The fallback allocation is shared by clones of this client
    /// cache.
    fn effective_provider(&self) -> Arc<rustls::crypto::CryptoProvider> {
        rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| {
                Arc::clone(
                    self.fallback_provider
                        .get_or_init(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider())),
                )
            })
    }
}

/// Builds the effective TLS trust store from a client-specific policy.
pub(super) fn build_root_store(trust_roots: &WsTrustRoots) -> rustls::RootCertStore {
    match trust_roots {
        WsTrustRoots::Default => webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
        WsTrustRoots::Additional(additional) => {
            let mut roots: rustls::RootCertStore = webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect();
            roots.roots.extend(additional.roots.iter().cloned());
            roots
        }
        WsTrustRoots::Exclusive(roots) => roots.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::process::Stdio;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use super::TlsConfigCache;
    use crate::options::WsTrustRoots;

    const PROVIDER_TRANSITION_CHILD_ENV: &str = "QUBIT_WEBSOCKET_PROVIDER_TRANSITION_CHILD";

    #[test]
    fn test_cache_reuses_configuration() {
        let cache = TlsConfigCache::default();
        let roots = WsTrustRoots::Default;

        let first = cache.get(&roots).expect("build TLS config");
        let second = cache.get(&roots).expect("reuse TLS config");

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn test_independent_caches_keep_configs_separate() {
        let roots = WsTrustRoots::Default;
        let first_cache = TlsConfigCache::default();
        let second_cache = TlsConfigCache::default();

        let first = first_cache.get(&roots).expect("build first TLS config");
        let second = second_cache.get(&roots).expect("build second TLS config");

        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn test_provider_change_replaces_cached_configuration() {
        let cache = TlsConfigCache::default();
        let roots = WsTrustRoots::Default;
        let first_provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let next_provider = Arc::new(rustls::crypto::ring::default_provider());
        let first = cache
            .get_for_provider(&roots, Arc::clone(&first_provider))
            .expect("build fallback-provider TLS config");
        let next = cache
            .get_for_provider(&roots, Arc::clone(&next_provider))
            .expect("replace with installed-provider TLS config");
        let repeated = cache
            .get_for_provider(&roots, next_provider)
            .expect("reuse installed-provider TLS config");

        assert!(!Arc::ptr_eq(&first, &next));
        assert!(Arc::ptr_eq(&next, &repeated));
    }

    #[test]
    fn test_global_provider_installed_after_fallback_replaces_cached_configuration() {
        let mut child = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--ignored",
                "--exact",
                "client::internal::tls_config_cache::tests::test_provider_transition_child",
                "--nocapture",
            ])
            .env(PROVIDER_TRANSITION_CHILD_ENV, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start isolated provider transition test");
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if child.try_wait().expect("poll provider transition child").is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output().expect("collect timed out child output");
                panic!(
                    "provider transition child timed out\nstdout: {}\nstderr: {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            thread::sleep(Duration::from_millis(20));
        }
        let output = child.wait_with_output().expect("collect provider transition output");
        assert!(
            output.status.success(),
            "provider transition child failed with {}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "run only in an isolated provider transition subprocess"]
    fn test_provider_transition_child() {
        assert_eq!(std::env::var(PROVIDER_TRANSITION_CHILD_ENV).as_deref(), Ok("1"));
        assert!(rustls::crypto::CryptoProvider::get_default().is_none());

        let cache = TlsConfigCache::default();
        let roots = WsTrustRoots::Default;
        let fallback_config = cache.get(&roots).expect("build fallback TLS config");
        let fallback_provider = Arc::clone(
            &cache
                .entry
                .lock()
                .expect("cache lock")
                .as_ref()
                .expect("fallback config cached")
                .provider,
        );

        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("install process ring provider after fallback");
        let installed_provider = rustls::crypto::CryptoProvider::get_default().expect("installed provider");
        assert!(!Arc::ptr_eq(&fallback_provider, installed_provider));

        let installed_config = cache.get(&roots).expect("switch to installed-provider config");
        let cached_provider = Arc::clone(
            &cache
                .entry
                .lock()
                .expect("cache lock")
                .as_ref()
                .expect("installed config cached")
                .provider,
        );
        assert!(Arc::ptr_eq(installed_provider, &cached_provider));
        assert!(!Arc::ptr_eq(&fallback_config, &installed_config));
        assert!(Arc::ptr_eq(
            &installed_config,
            &cache.get(&roots).expect("reuse installed-provider config")
        ));
    }

    #[test]
    fn test_failed_build_is_not_cached() {
        let cache = TlsConfigCache::default();
        let roots = WsTrustRoots::Default;
        let mut invalid = rustls::crypto::aws_lc_rs::default_provider();
        invalid.cipher_suites.clear();
        let invalid = Arc::new(invalid);
        assert!(cache.get_for_provider(&roots, invalid).is_err());

        let valid = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        assert!(cache.get_for_provider(&roots, valid).is_ok());
    }

    #[test]
    fn test_concurrent_first_gets_share_configuration() {
        let cache = Arc::new(TlsConfigCache::default());
        let workers = (0..8)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || cache.get(&WsTrustRoots::Default).expect("build TLS config"))
            })
            .collect::<Vec<_>>();
        let configs = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker completed"))
            .collect::<Vec<_>>();

        assert!(configs[1..].iter().all(|config| Arc::ptr_eq(&configs[0], config)));
    }
}
