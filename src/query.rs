use crate::event::Event;
use crate::value::Value;
use chrono::DateTime;
use chrono::Utc;
use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Query {
    FieldComparison {
        field: String,
        op: ComparisonOp,
        value: Value,
    },
    TimeRange {
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    },
    And(Vec<Query>),
    Or(Vec<Query>),
    All(),
}

#[derive(Debug, PartialEq)]
pub enum QueryResult<'a> {
    Events(Vec<&'a Event>),
    Count(usize),
    CountBy(usize),
    Average(f32),
    Percentage(f32),
}

pub struct QueryPlan {
    pub query: Query,
    pub aggregation: Option<Aggregation>,
}

pub enum Aggregation {
    Count,
    CountBy(String),
    Average(String),
    Percentage(String, f32),
}

impl fmt::Display for QueryResult<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryResult::Events(events) => {
                let s = events
                    .iter()
                    .map(|e| e.raw.as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                write!(f, "{}", s)
            }
            QueryResult::Count(count) | QueryResult::CountBy(count) => write!(f, "{}", count),
            QueryResult::Average(avg) => write!(f, "{}", avg),
            QueryResult::Percentage(p) => write!(f, "{}", p),
        }
    }
}

impl QueryPlan {
    pub fn new(query: Query, aggregation: Option<Aggregation>) -> QueryPlan {
        QueryPlan { query, aggregation }
    }
}
