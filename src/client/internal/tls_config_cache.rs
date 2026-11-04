// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;

use rustls::crypto::CryptoProvider;

use super::cached_config::CachedConfig;
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
pub(crate) struct TlsConfigCache {
    /// The single cached config, protected while checking, building, or
    /// replacing it.
    entry: StdMutex<Option<CachedConfig>>,
    /// Immutable trust policy captured when the client is built.
    trust_roots: Arc<WsTrustRoots>,
    /// Keeps submitted blocking work serialized after its async caller cancels.
    gate: Arc<tokio::sync::Mutex<()>>,
    /// Lazily initialized fallback provider, shared by clones of this client
    /// cache.
    fallback_provider: OnceLock<Arc<rustls::crypto::CryptoProvider>>,
    #[cfg(test)]
    build_hook: StdMutex<Option<Box<dyn Fn() + Send>>>,
}

impl TlsConfigCache {
    pub(crate) fn new(trust_roots: WsTrustRoots) -> Self {
        Self {
            entry: StdMutex::new(None),
            trust_roots: Arc::new(trust_roots),
            gate: Arc::default(),
            fallback_provider: OnceLock::new(),
            #[cfg(test)]
            build_hook: StdMutex::new(None),
        }
    }

    #[cfg(test)]
    pub(super) fn set_build_hook(&self, hook: impl Fn() + Send + 'static) {
        *self
            .build_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(hook));
    }

    /// Returns a cached TLS configuration without blocking an async executor
    /// thread.
    ///
    /// A per-cache async gate limits submitted blocking work to one operation.
    /// Its owned guard moves into the blocking closure, so cancellation of this
    /// future cannot permit duplicate work while a previous operation runs.
    ///
    /// # Returns
    ///
    /// A shared TLS client configuration, or `InvalidConfig` when the selected
    /// provider cannot build a safe TLS configuration.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when the selected provider has no safe protocol
    /// versions, or `Internal` if the blocking task panics.
    pub(crate) async fn get(self: &Arc<Self>) -> Result<Arc<rustls::ClientConfig>, WsError> {
        if let Some(config) = self.cached_for_current_provider() {
            return Ok(config);
        }

        let permit = Arc::clone(&self.gate).lock_owned().await;
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            cache.get_sync()
        })
        .await
        .map_err(|_| WsError::new(WsErrorKind::Internal))?
    }

    /// Checks for a cached config belonging to the provider that is already
    /// available, without initializing the fallback provider or waiting for
    /// the entry mutex. Returns `None` when no cached config exists, the
    /// provider identity differs, or the entry mutex is contended or poisoned.
    fn cached_for_current_provider(&self) -> Option<Arc<rustls::ClientConfig>> {
        let provider = CryptoProvider::get_default().or_else(|| self.fallback_provider.get())?;
        let entry = self.entry.try_lock().ok()?;
        let cached = entry.as_ref()?;
        if Arc::ptr_eq(&cached.provider, provider) {
            Some(Arc::clone(&cached.config))
        } else {
            None
        }
    }

    fn get_sync(&self) -> Result<Arc<rustls::ClientConfig>, WsError> {
        let mut entry = self.entry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let provider = self.effective_provider();
        #[cfg(test)]
        if !entry
            .as_ref()
            .is_some_and(|cached| Arc::ptr_eq(&cached.provider, &provider))
            && let Some(hook) = self
                .build_hook
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
        {
            hook();
        }
        Self::get_with_entry(&mut entry, &self.trust_roots, provider)
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

impl Default for TlsConfigCache {
    fn default() -> Self {
        Self::new(WsTrustRoots::Default)
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use super::TlsConfigCache;
    use crate::options::WsTrustRoots;

    const PROVIDER_TRANSITION_CHILD_ENV: &str = "QUBIT_WEBSOCKET_PROVIDER_TRANSITION_CHILD";

    #[test]
    fn test_cache_reuses_configuration() {
        let cache = TlsConfigCache::default();

        let first = cache.get_sync().expect("build TLS config");
        let second = cache.get_sync().expect("reuse TLS config");

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn test_cache_probe_misses_when_cold() {
        let cache = TlsConfigCache::default();

        assert!(cache.cached_for_current_provider().is_none());
    }

    #[test]
    fn test_cache_probe_hits_same_configuration() {
        let cache = TlsConfigCache::default();
        let warm = cache.get_sync().expect("build TLS config");

        let cached = cache.cached_for_current_provider().expect("matching warm cache entry");
        assert!(Arc::ptr_eq(&warm, &cached));
    }

    #[test]
    fn test_cache_probe_misses_when_entry_is_contended() {
        let cache = TlsConfigCache::default();
        cache.get_sync().expect("build TLS config");
        let _entry = cache.entry.lock().expect("cache entry lock");

        assert!(cache.cached_for_current_provider().is_none());
    }

    #[test]
    fn test_cache_probe_misses_for_different_provider() {
        let cache = TlsConfigCache::default();
        cache.get_sync().expect("build TLS config");
        let other_provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut entry = cache.entry.lock().expect("cache entry lock");
        entry.as_mut().expect("warm cache entry").provider = other_provider;
        drop(entry);

        assert!(cache.cached_for_current_provider().is_none());
    }

    #[test]
    fn test_warm_cache_hit_does_not_wait_for_blocking_pool() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .expect("build controlled runtime");

        runtime.block_on(async {
            let cache = Arc::new(TlsConfigCache::default());
            let warm = cache.get().await.expect("prewarm TLS config");
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                entered_tx.send(()).expect("notify blocker entered");
                release_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("watchdog releases blocking-pool task");
            });

            let entered = tokio::time::timeout(Duration::from_secs(2), entered_rx).await;
            let observed = if entered.is_ok() {
                Some(tokio::time::timeout(Duration::from_millis(500), cache.get()).await)
            } else {
                None
            };

            let _ = release_tx.send(());
            tokio::time::timeout(Duration::from_secs(2), blocker)
                .await
                .expect("blocker exits")
                .expect("blocker joined");

            let cached_after_release = cache.get().await.expect("cache remains valid");
            assert!(Arc::ptr_eq(&warm, &cached_after_release));
            assert!(entered.is_ok(), "blocker entered the sole blocking thread");
            let cached_under_contention = observed
                .expect("blocker entered before the lookup")
                .expect("warm lookup must not require a blocking worker")
                .expect("warm config remains valid");
            assert!(Arc::ptr_eq(&warm, &cached_under_contention));
        });
    }

    #[test]
    fn test_independent_caches_keep_configs_separate() {
        let first_cache = TlsConfigCache::default();
        let second_cache = TlsConfigCache::default();

        let first = first_cache.get_sync().expect("build first TLS config");
        let second = second_cache.get_sync().expect("build second TLS config");

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
        let fallback_config = cache.get_sync().expect("build fallback TLS config");
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

        let installed_config = cache.get_sync().expect("switch to installed-provider config");
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
            &cache.get_sync().expect("reuse installed-provider config")
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
                thread::spawn(move || cache.get_sync().expect("build TLS config"))
            })
            .collect::<Vec<_>>();
        let configs = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker completed"))
            .collect::<Vec<_>>();

        assert!(configs[1..].iter().all(|config| Arc::ptr_eq(&configs[0], config)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_blocking_build_does_not_block_current_thread_runtime() {
        let cache = Arc::new(TlsConfigCache::default());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *cache.build_hook.lock().expect("build hook lock") = Some(Box::new(move || {
            entered_tx.send(()).expect("notify build entered");
            release_rx
                .recv_timeout(Duration::from_secs(3))
                .expect("watchdog releases paused config build");
        }));

        let build = tokio::spawn({
            let cache = Arc::clone(&cache);
            async move { cache.get().await }
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(2)))
            .await
            .expect("entered waiter task")
            .expect("config build entered before deadline");

        let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = observed_tx.send(());
        });
        let observed = tokio::time::timeout(Duration::from_millis(200), observed_rx).await;
        let _ = release_tx.send(());
        assert!(observed.is_ok(), "async observer must run while config build is paused");
        tokio::time::timeout(Duration::from_secs(2), build)
            .await
            .expect("config build completes after release")
            .expect("get task completes")
            .expect("valid provider builds config");
    }

    #[tokio::test]
    async fn test_cancelled_get_keeps_blocking_build_serialized() {
        let cache = Arc::new(TlsConfigCache::default());
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let active_hook = Arc::clone(&active);
        let maximum_hook = Arc::clone(&maximum);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *cache.build_hook.lock().expect("build hook lock") = Some(Box::new(move || {
            let current = active_hook.fetch_add(1, Ordering::SeqCst) + 1;
            maximum_hook.fetch_max(current, Ordering::SeqCst);
            let _ = entered_tx.send(());
            release_rx
                .recv_timeout(Duration::from_secs(3))
                .expect("watchdog releases serialized build");
            active_hook.fetch_sub(1, Ordering::SeqCst);
        }));

        let first = tokio::spawn({
            let cache = Arc::clone(&cache);
            async move { cache.get().await }
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(2)))
            .await
            .expect("entered waiter task")
            .expect("first build entered");
        first.abort();
        tokio::time::timeout(Duration::from_secs(2), first)
            .await
            .expect("cancelled caller joined")
            .expect_err("first async caller was aborted");

        let workers = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                tokio::spawn(async move { cache.get().await })
            })
            .collect::<Vec<_>>();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(maximum.load(Ordering::SeqCst), 1, "one build may be active per cache");
        let _ = release_tx.send(());

        let mut configs = Vec::new();
        for worker in workers {
            configs.push(
                tokio::time::timeout(Duration::from_secs(2), worker)
                    .await
                    .expect("queued get completes")
                    .expect("get task completes")
                    .expect("config is valid"),
            );
        }
        assert!(configs[1..].iter().all(|config| Arc::ptr_eq(&configs[0], config)));
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_blocking_panic_maps_to_internal_without_panic_text() {
        let cache = Arc::new(TlsConfigCache::default());
        *cache.build_hook.lock().expect("build hook lock") = Some(Box::new(|| panic!("sensitive panic detail")));
        let error = cache.get().await.expect_err("blocking panic becomes an internal error");
        assert_eq!(error.kind(), crate::error::WsErrorKind::Internal);
        assert!(!error.to_string().contains("sensitive panic detail"));
    }
}
