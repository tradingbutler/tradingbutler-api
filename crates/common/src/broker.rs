use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Broker {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "nm")]
    pub name: String,
    #[serde(rename = "ak")]
    pub api_key: String,
    /// Broker-specific alias → canonical symbol code (e.g. `"BITCOIN" -> "BTCUSD"`).
    /// Not part of the wire protocol — populated from the `symbol_map` field on
    /// the broker's Redis hash after registration.
    #[serde(skip)]
    pub symbol_map: HashMap<String, String>,
}

impl Broker {
    /// Looks up the canonical symbol code for a broker-reported symbol string.
    /// Returns `None` if this broker has no mapping for it.
    pub fn canonical_symbol(&self, raw: &str) -> Option<&str> {
        self.symbol_map.get(raw).map(String::as_str)
    }
}
