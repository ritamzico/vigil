use crate::value::Value;
use chrono::DateTime;
use chrono::Utc;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub timestamp: Option<DateTime<Utc>>,
    pub raw: String,
    pub fields: HashMap<String, Value>,
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
}

impl Hash for Event {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.raw.hash(state);
    }
}

#[derive(Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct PersistedEvent {
    timestamp: Option<i64>,
    raw: String,
    fields: HashMap<String, Value>,
}

impl PersistedEvent {
    pub fn new(
        timestamp: Option<i64>,
        raw: String,
        fields: HashMap<String, Value>,
    ) -> PersistedEvent {
        PersistedEvent {
            timestamp,
            raw,
            fields,
        }
    }

    pub fn from_event(event: &Event) -> PersistedEvent {
        let timestamp = match event.timestamp {
            Some(timestamp) => Some(timestamp.timestamp_millis()),
            None => None,
        };

        Self::new(timestamp, event.raw.clone(), event.fields.clone())
    }

    pub fn into_event(self) -> Event {
        let timestamp = match self.timestamp {
            Some(timestamp) => DateTime::from_timestamp_millis(timestamp),
            None => None,
        };

        Event::new(timestamp, self.raw, self.fields)
    }
}
