use std::hash::{Hash, Hasher};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub enum Value {
    String(String),
    Number(f32),
    Bool(bool),
}

impl Value {
    pub fn from_string(v: &str) -> Value {
        if let Ok(float_value) = v.parse::<f32>() {
            return Value::Number(float_value);
        }

        if let Ok(bool_value) = v.parse::<bool>() {
            return Value::Bool(bool_value);
        }

        Value::String(v.to_string())
    }

    pub fn from_json(v: serde_json::Value) -> Option<Value> {
        match v {
            serde_json::Value::String(s) => Some(Value::String(s)),
            serde_json::Value::Number(n) => n.as_f64().map(|f| Value::Number(f as f32)),
            serde_json::Value::Bool(b) => Some(Value::Bool(b)),
            _ => None,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Number(a), Value::Number(b)) => a.to_bits() == b.to_bits(),
            (Value::Bool(a), Value::Bool(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Value) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Number(a), Value::Number(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            _ => None, // incompatible types
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::String(s) => {
                0u8.hash(state);
                s.hash(state);
            }
            Value::Number(n) => {
                1u8.hash(state);
                n.to_bits().hash(state);
            }
            Value::Bool(b) => {
                2u8.hash(state);
                b.hash(state);
            }
        }
    }
}
