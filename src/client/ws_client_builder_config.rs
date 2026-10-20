// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::time::Duration;

use qubit_config::Config;
use qubit_config::ConfigError;
use qubit_config::ConfigReader;
use qubit_config::ConfigSection;

use crate::client::WsClient;
use crate::client::WsClientBuilder;
use crate::config_keys;
use crate::options::WsSessionLimits;

impl WsClientBuilder {
    /// Creates a builder from a configuration subtree.
    ///
    /// `prefix` selects the subtree to read; an empty string selects the root
    /// configuration. The selected subtree must contain at least one
    /// property. Missing supported keys retain their builder defaults.
    ///
    /// This method only reads and maps configuration. It performs no network
    /// I/O. Configuration path, missing subtree, unknown property, and value
    /// conversion errors are returned as [`ConfigError`]. WebSocket-specific
    /// constraints, including zero values and inconsistent limits, are checked
    /// later by [`WsClientBuilder::build`].
    ///
    /// # Arguments
    ///
    /// * `config` - Root configuration to read.
    /// * `prefix` - Canonical subtree path, or an empty string for the root.
    ///
    /// # Returns
    ///
    /// A builder with the configured values applied and omitted values left at
    /// their defaults.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidPath`] for an invalid prefix,
    /// [`ConfigError::PropertyNotFound`] when the selected subtree is empty,
    /// [`ConfigError::UnknownProperties`] for unrecognized keys, and the
    /// corresponding configuration error with its path when a value cannot be
    /// read or converted. Builder validation errors are deferred to
    /// [`WsClientBuilder::build`].
    pub fn from_config(config: &Config, prefix: &str) -> Result<Self, ConfigError> {
        let section = config.section(prefix)?;
        if section.is_empty() {
            return Err(ConfigError::PropertyNotFound(prefix.to_owned()));
        }

        let mut unknown_paths = section
            .keys()
            .into_iter()
            .filter(|key| !config_keys::KNOWN_KEYS.contains(&key.as_str()))
            .map(|key| section.resolve_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        if !unknown_paths.is_empty() {
            unknown_paths.sort();
            return Err(ConfigError::UnknownProperties { paths: unknown_paths });
        }

        let mut builder = WsClient::builder();
        if let Some(value) = section.get_optional::<u64>(config_keys::CONNECT_TIMEOUT_MS)? {
            builder = builder.connect_timeout(Duration::from_millis(value));
        }
        if let Some(value) = section.get_optional::<u64>(config_keys::HANDSHAKE_TIMEOUT_MS)? {
            builder = builder.handshake_timeout(Duration::from_millis(value));
        }
        if let Some(value) = section.get_optional_list::<String>(config_keys::SUBPROTOCOLS)? {
            builder = builder.subprotocols(value);
        }

        let mut limits = WsSessionLimits::default();
        if let Some(value) = read_usize(&section, config_keys::MAX_INBOUND_FRAME_BYTES)? {
            limits.max_inbound_frame_bytes = value;
        }
        if let Some(value) = read_usize(&section, config_keys::MAX_INBOUND_MESSAGE_BYTES)? {
            limits.max_inbound_message_bytes = value;
        }
        if let Some(value) = read_usize(&section, config_keys::INBOUND_MESSAGES)? {
            limits.inbound_messages = value;
        }
        if let Some(value) = read_usize(&section, config_keys::INBOUND_BYTES)? {
            limits.inbound_bytes = value;
        }
        if let Some(value) = read_usize(&section, config_keys::OUTBOUND_MESSAGES)? {
            limits.outbound_messages = value;
        }
        if let Some(value) = read_usize(&section, config_keys::OUTBOUND_BYTES)? {
            limits.outbound_bytes = value;
        }
        if let Some(value) = section.get_optional::<u64>(config_keys::NO_PROGRESS_TIMEOUT_MS)? {
            limits.no_progress_timeout = Duration::from_millis(value);
        }
        if let Some(value) = section.get_optional::<u64>(config_keys::CLOSE_TIMEOUT_MS)? {
            limits.close_timeout = Duration::from_millis(value);
        }
        Ok(builder.limits(limits))
    }
}

fn read_usize(section: &ConfigSection<'_>, key: &str) -> Result<Option<usize>, ConfigError> {
    let Some(value) = section.get_optional::<u128>(key)? else {
        return Ok(None);
    };
    let path = section.resolve_key(key)?;
    usize::try_from(value)
        .map(Some)
        .map_err(|_| ConfigError::Other(format!("configuration value at '{path}' does not fit usize")))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use qubit_config::Config;

    use crate::client::WsClientBuilder;
    use crate::config_keys;

    #[test]
    fn test_from_config_maps_all_supported_fields() {
        let mut config = Config::new();
        config
            .set(format!("websocket.{}", config_keys::CONNECT_TIMEOUT_MS), 2300_u64)
            .unwrap();
        config
            .set(format!("websocket.{}", config_keys::HANDSHAKE_TIMEOUT_MS), 3400_u64)
            .unwrap();
        config
            .set(format!("websocket.{}", config_keys::SUBPROTOCOLS), vec!["jobs.v1"])
            .unwrap();
        config
            .set(format!("websocket.{}", config_keys::MAX_INBOUND_FRAME_BYTES), 4096_u64)
            .unwrap();
        config
            .set(
                format!("websocket.{}", config_keys::MAX_INBOUND_MESSAGE_BYTES),
                8192_u64,
            )
            .unwrap();
        config
            .set(format!("websocket.{}", config_keys::INBOUND_MESSAGES), 8_u64)
            .unwrap();
        config
            .set(format!("websocket.{}", config_keys::INBOUND_BYTES), 16384_u64)
            .unwrap();
        config
            .set(format!("websocket.{}", config_keys::OUTBOUND_MESSAGES), 16_u64)
            .unwrap();
        config
            .set(format!("websocket.{}", config_keys::OUTBOUND_BYTES), 32768_u64)
            .unwrap();
        config
            .set(format!("websocket.{}", config_keys::NO_PROGRESS_TIMEOUT_MS), 1700_u64)
            .unwrap();
        config
            .set(format!("websocket.{}", config_keys::CLOSE_TIMEOUT_MS), 800_u64)
            .unwrap();

        let builder = WsClientBuilder::from_config(&config, "websocket").unwrap();
        let options = builder.options;
        assert_eq!(options.connect_timeout, Duration::from_millis(2300));
        assert_eq!(options.handshake_timeout, Duration::from_millis(3400));
        assert_eq!(options.subprotocols, ["jobs.v1"]);
        assert_eq!(options.limits.max_inbound_frame_bytes, 4096);
        assert_eq!(options.limits.max_inbound_message_bytes, 8192);
        assert_eq!(options.limits.inbound_messages, 8);
        assert_eq!(options.limits.inbound_bytes, 16384);
        assert_eq!(options.limits.outbound_messages, 16);
        assert_eq!(options.limits.outbound_bytes, 32768);
        assert_eq!(options.limits.no_progress_timeout, Duration::from_millis(1700));
        assert_eq!(options.limits.close_timeout, Duration::from_millis(800));
        assert!(WsClientBuilder { options }.build().is_ok());
    }

    #[test]
    fn test_from_config_preserves_defaults_for_omitted_fields() {
        let mut config = Config::new();
        config
            .set(format!("websocket.{}", config_keys::CONNECT_TIMEOUT_MS), 2300_u64)
            .unwrap();

        let builder = WsClientBuilder::from_config(&config, "websocket").unwrap();
        let defaults = WsClientBuilder::default().options;
        assert_eq!(builder.options.connect_timeout, Duration::from_millis(2300));
        assert_eq!(builder.options.handshake_timeout, defaults.handshake_timeout);
        assert_eq!(builder.options.subprotocols, defaults.subprotocols);
        assert_eq!(builder.options.limits, defaults.limits);
    }
}
