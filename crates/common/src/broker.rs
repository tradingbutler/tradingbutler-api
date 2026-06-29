use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Broker {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "nm")]
    pub name: String,
    #[serde(rename = "ak")]
    pub api_key: String,
}
