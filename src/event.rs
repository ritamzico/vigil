use crate::value::Value;
use chrono::DateTime;
use chrono::Utc;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Debug, PartialEq, Eq)]
pub struct Event {
    timestamp: Option<DateTime<Utc>>,
    raw: String,
    fields: HashMap<String, Value>,
}

impl Event {
    pub fn new(
        timestamp: Option<DateTime<Utc>>,
        raw: String,
        fields: HashMap<String, Value>,
    ) -> Event {
        Event {
            timestamp,
            raw,
            fields,
        }
    }

    pub fn get_timestamp(&self) -> &Option<DateTime<Utc>> {
        &self.timestamp
    }

    pub fn get_raw(&self) -> &String {
        &self.raw
    }

    pub fn get_fields(&self) -> &HashMap<String, Value> {
        &self.fields
    }
}

impl Hash for Event {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.raw.hash(state);
    }
}
