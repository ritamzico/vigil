use crate::event::Event;
use crate::query::{Aggregation, ComparisonOp, Limit, Query, QueryPlan, QueryResult};
use crate::value::Value;
use chrono::DateTime;
use chrono::Utc;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Bound::{Included, Unbounded};
use thiserror::Error;

const SLACK: usize = 1024;
const DEFAULT_MAX_EVENTS: usize = 2_000_000;

#[derive(Debug, Error)]
pub enum AggregationError {
    #[error("field '{0}' not found in any matching event")]
    FieldNotFound(String),
    #[error("field '{0}' has incompatible type for numeric aggregation")]
    IncompatibleType(String),
    #[error("no matching events to aggregate")]
    NoMatchingEvents,
    #[error("percentile out of range (p1–p100)")]
    InvalidPercentile,
}

pub struct Index {
    pub events: Vec<Event>,
    field_index: HashMap<String, HashMap<Value, Vec<usize>>>,
    time_index: BTreeMap<DateTime<Utc>, Vec<usize>>,
    max_events: usize,
}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for event in &self.events {
            writeln!(f, "{}", event.raw)?;
        }
        Ok(())
    }
}

impl Index {
    pub fn new(max_events: Option<usize>) -> Index {
        Index {
            events: vec![],
            field_index: HashMap::new(),
            time_index: BTreeMap::new(),
            max_events: max_events.unwrap_or(DEFAULT_MAX_EVENTS),
        }
    }

    pub fn push_event(&mut self, event: Event) {
        self.push_to_field_and_time_index(self.events.len(), &event);
        self.events.push(event);

        if self.events.len() > self.max_events + SLACK {
            self.events.drain(0..self.events.len() - self.max_events);
            self.reindex();
        }
    }

    #[cfg(test)]
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    pub fn apply_query_plan<'a>(
        &'a self,
        query_plan: &QueryPlan,
    ) -> Result<QueryResult<'a>, AggregationError> {
        let mut events = self.apply_query(&query_plan.query);
        let Some(aggregation) = &query_plan.aggregation else {
            // Events are in ascending chronological (insertion) order, so
            // Head keeps the first n and Tail keeps the last n, still ascending.
            match query_plan.limit {
                Some(Limit::Head(n)) => events.truncate(n),
                Some(Limit::Tail(n)) if events.len() > n => {
                    events.drain(..events.len() - n);
                }
                _ => {}
            }
            return Ok(QueryResult::Events(events));
        };

        match aggregation {
            Aggregation::Count => Ok(QueryResult::Count(events.len())),
            Aggregation::Average(field) => {
                let values = numeric_values(&events, field)?;
                let total: f32 = values.iter().sum();

                Ok(QueryResult::Scalar(total / values.len() as f32))
            }
            Aggregation::Percentage(field, p) => {
                if *p <= 0.0 || *p > 1.0 {
                    return Err(AggregationError::InvalidPercentile);
                }

                let mut values = numeric_values(&events, field)?;

                values.sort_by(f32::total_cmp);
                let i = (*p * values.len() as f32).ceil() as usize - 1;

                Ok(QueryResult::Scalar(values[i]))
            }
            Aggregation::Sum(field) => {
                let values = numeric_values(&events, field)?;

                Ok(QueryResult::Scalar(values.iter().sum()))
            }
            Aggregation::Min(field) => {
                let values = numeric_values(&events, field)?;

                // numeric_values guarantees at least one value
                Ok(QueryResult::Scalar(
                    values
                        .into_iter()
                        .reduce(|a, b| if b.total_cmp(&a).is_lt() { b } else { a })
                        .unwrap(),
                ))
            }
            Aggregation::Max(field) => {
                let values = numeric_values(&events, field)?;

                Ok(QueryResult::Scalar(
                    values
                        .into_iter()
                        .reduce(|a, b| if b.total_cmp(&a).is_gt() { b } else { a })
                        .unwrap(),
                ))
            }
            Aggregation::CountBy(field) => Ok(QueryResult::CountBy(
                events
                    .iter()
                    .filter(|event| event.fields.contains_key(field))
                    .collect::<Vec<&&Event>>()
                    .len(),
            )),
        }
    }

    pub fn apply_query<'a>(&'a self, query: &Query) -> Vec<&'a Event> {
        self.matching_indices(query)
            .into_iter()
            .map(|i| &self.events[i])
            .collect()
    }

    /// Positions into `events` matching `query`, sorted and de-duplicated —
    /// results are always in insertion (chronological) order.
    pub fn matching_indices(&self, query: &Query) -> Vec<usize> {
        let mut indices = self.collect_indices(query);
        indices.sort_unstable();
        indices.dedup();
        indices
    }

    fn collect_indices(&self, query: &Query) -> Vec<usize> {
        match query {
            Query::FieldComparison { field, op, value } => {
                self.field_comparison_indices(field, op, value)
            }
            Query::TimeRange { start, end } => self.time_range_indices(start, end),
            Query::And(queries) => self.and_indices(queries),
            Query::Or(queries) => self.or_indices(queries),
            Query::Not(query) => self.not_indices(query),
            Query::All() => (0..self.events.len()).collect(),
        }
    }

    fn push_to_field_and_time_index(&mut self, i: usize, event: &Event) {
        for (field, value) in &event.fields {
            let field_map = self.field_index.entry(field.clone()).or_default();
            field_map.entry(value.clone()).or_default().push(i);
        }

        if let Some(timestamp) = event.timestamp {
            self.time_index.entry(timestamp).or_default().push(i);
        }
    }

    fn reindex(&mut self) {
        self.field_index.clear();
        self.time_index.clear();

        let events = std::mem::take(&mut self.events);
        for (i, event) in events.iter().enumerate() {
            self.push_to_field_and_time_index(i, event);
        }
        self.events = events;
    }

    fn field_comparison_indices(
        &self,
        field: &String,
        op: &ComparisonOp,
        value: &Value,
    ) -> Vec<usize> {
        let Some(field_map) = self.field_index.get(field) else {
            return vec![];
        };

        field_map
            .iter()
            .filter(|(candidate, _)| compare(op, candidate, value))
            .flat_map(|(_, indices)| indices.iter().copied())
            .collect()
    }

    fn time_range_indices(
        &self,
        start: &Option<DateTime<Utc>>,
        end: &Option<DateTime<Utc>>,
    ) -> Vec<usize> {
        let range = (
            match start {
                Some(s) => Included(s),
                _ => Unbounded,
            },
            match end {
                Some(e) => Included(e),
                _ => Unbounded,
            },
        );

        self.time_index
            .range(range)
            .flat_map(|(_, indices)| indices.iter().copied())
            .collect()
    }

    fn and_indices(&self, queries: &[Query]) -> Vec<usize> {
        let all_indices: Vec<Vec<usize>> = queries
            .par_iter()
            .map(|query| self.collect_indices(query))
            .collect();

        let sets: Vec<HashSet<usize>> = all_indices
            .iter()
            .map(|v| v.iter().copied().collect())
            .collect();
        let first = &sets[0];

        first
            .iter()
            .copied()
            .filter(|i| sets[1..].iter().all(|s| s.contains(i)))
            .collect()
    }

    fn not_indices(&self, query: &Query) -> Vec<usize> {
        let matched: HashSet<usize> = self.collect_indices(query).into_iter().collect();

        (0..self.events.len())
            .filter(|i| !matched.contains(i))
            .collect()
    }

    fn or_indices(&self, queries: &[Query]) -> Vec<usize> {
        queries
            .par_iter()
            .map(|query| self.collect_indices(query))
            .flatten()
            .collect()
    }
}

pub fn compare(op: &ComparisonOp, candidate: &Value, value: &Value) -> bool {
    match *op {
        ComparisonOp::Eq => candidate == value,
        ComparisonOp::Ne => candidate != value,
        ComparisonOp::Lt => candidate < value,
        ComparisonOp::Le => candidate <= value,
        ComparisonOp::Gt => candidate > value,
        ComparisonOp::Ge => candidate >= value,
        ComparisonOp::Contains => matches!(
            (candidate, value),
            (Value::String(c), Value::String(v)) if c.contains(v.as_str())
        ),
    }
}

fn numeric_values(events: &[&Event], field: &str) -> Result<Vec<f32>, AggregationError> {
    if events.is_empty() {
        return Err(AggregationError::NoMatchingEvents);
    }

    let values: Vec<f32> = events
        .iter()
        .filter_map(|event| event.fields.get(field))
        .map(|value| match value {
            Value::Number(n) => Ok(*n),
            _ => Err(AggregationError::IncompatibleType(field.to_string())),
        })
        .collect::<Result<_, _>>()?;

    if values.is_empty() {
        return Err(AggregationError::FieldNotFound(field.to_string()));
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ComparisonOp;
    use chrono::TimeZone;

    fn make_event(ts: Option<DateTime<Utc>>, fields: Vec<(&str, Value)>) -> Event {
        let map = fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        Event::new(ts, "raw".to_string(), map)
    }

    fn ts(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap()
    }

    fn result_count(r: Vec<&Event>) -> usize {
        r.len()
    }

    #[test]
    fn test_event_count_empty() {
        let idx = Index::new(None);
        assert_eq!(idx.event_count(), 0);
    }

    #[test]
    fn test_event_count_after_push() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(None, vec![]));
        idx.push_event(make_event(None, vec![]));
        assert_eq!(idx.event_count(), 2);
    }

    #[test]
    fn test_field_eq_match() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(
            None,
            vec![("level", Value::String("error".into()))],
        ));
        idx.push_event(make_event(
            None,
            vec![("level", Value::String("info".into()))],
        ));

        let q = Query::FieldComparison {
            field: "level".into(),
            op: ComparisonOp::Eq,
            value: Value::String("error".into()),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 1);
    }

    #[test]
    fn test_field_eq_no_match() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(
            None,
            vec![("level", Value::String("info".into()))],
        ));

        let q = Query::FieldComparison {
            field: "level".into(),
            op: ComparisonOp::Eq,
            value: Value::String("error".into()),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 0);
    }

    #[test]
    fn test_field_missing_returns_empty() {
        let idx = Index::new(None);
        let q = Query::FieldComparison {
            field: "nonexistent".into(),
            op: ComparisonOp::Eq,
            value: Value::String("x".into()),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 0);
    }

    #[test]
    fn test_field_ne() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(
            None,
            vec![("level", Value::String("error".into()))],
        ));
        idx.push_event(make_event(
            None,
            vec![("level", Value::String("info".into()))],
        ));

        let q = Query::FieldComparison {
            field: "level".into(),
            op: ComparisonOp::Ne,
            value: Value::String("error".into()),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 1);
    }

    #[test]
    fn test_field_gt_number() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(None, vec![("code", Value::Number(200.0))]));
        idx.push_event(make_event(None, vec![("code", Value::Number(500.0))]));
        idx.push_event(make_event(None, vec![("code", Value::Number(404.0))]));

        let q = Query::FieldComparison {
            field: "code".into(),
            op: ComparisonOp::Gt,
            value: Value::Number(400.0),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 2);
    }

    #[test]
    fn test_field_lt_number() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(None, vec![("code", Value::Number(100.0))]));
        idx.push_event(make_event(None, vec![("code", Value::Number(200.0))]));
        idx.push_event(make_event(None, vec![("code", Value::Number(300.0))]));

        let q = Query::FieldComparison {
            field: "code".into(),
            op: ComparisonOp::Lt,
            value: Value::Number(200.0),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 1);
    }

    #[test]
    fn test_field_ge_number() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(None, vec![("code", Value::Number(100.0))]));
        idx.push_event(make_event(None, vec![("code", Value::Number(200.0))]));
        idx.push_event(make_event(None, vec![("code", Value::Number(300.0))]));

        let q = Query::FieldComparison {
            field: "code".into(),
            op: ComparisonOp::Ge,
            value: Value::Number(200.0),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 2);
    }

    #[test]
    fn test_field_le_number() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(None, vec![("code", Value::Number(100.0))]));
        idx.push_event(make_event(None, vec![("code", Value::Number(200.0))]));
        idx.push_event(make_event(None, vec![("code", Value::Number(300.0))]));

        let q = Query::FieldComparison {
            field: "code".into(),
            op: ComparisonOp::Le,
            value: Value::Number(200.0),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 2);
    }

    #[test]
    fn test_field_contains_substring() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(
            None,
            vec![(
                "message",
                Value::String("connection timeout after 30s".into()),
            )],
        ));
        idx.push_event(make_event(
            None,
            vec![("message", Value::String("request ok".into()))],
        ));

        let q = Query::FieldComparison {
            field: "message".into(),
            op: ComparisonOp::Contains,
            value: Value::String("timeout".into()),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 1);
    }

    #[test]
    fn test_field_contains_no_match() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(
            None,
            vec![("message", Value::String("request ok".into()))],
        ));

        let q = Query::FieldComparison {
            field: "message".into(),
            op: ComparisonOp::Contains,
            value: Value::String("timeout".into()),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 0);
    }

    #[test]
    fn test_field_contains_non_string_field_no_match() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(None, vec![("status", Value::Number(500.0))]));

        let q = Query::FieldComparison {
            field: "status".into(),
            op: ComparisonOp::Contains,
            value: Value::String("500".into()),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 0);
    }

    #[test]
    fn test_time_range_both_bounds() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(Some(ts(2024, 1, 1)), vec![]));
        idx.push_event(make_event(Some(ts(2024, 6, 1)), vec![]));
        idx.push_event(make_event(Some(ts(2024, 12, 31)), vec![]));

        let q = Query::TimeRange {
            start: Some(ts(2024, 1, 1)),
            end: Some(ts(2024, 6, 1)),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 2);
    }

    #[test]
    fn test_time_range_no_bounds() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(Some(ts(2023, 1, 1)), vec![]));
        idx.push_event(make_event(Some(ts(2024, 1, 1)), vec![]));

        let q = Query::TimeRange {
            start: None,
            end: None,
        };
        assert_eq!(result_count(idx.apply_query(&q)), 2);
    }

    #[test]
    fn test_time_range_no_matches() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(Some(ts(2020, 1, 1)), vec![]));
        idx.push_event(make_event(Some(ts(2021, 1, 1)), vec![]));

        let q = Query::TimeRange {
            start: Some(ts(2024, 1, 1)),
            end: Some(ts(2024, 12, 31)),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 0);
    }

    #[test]
    fn test_time_range_open_start() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(Some(ts(2022, 1, 1)), vec![]));
        idx.push_event(make_event(Some(ts(2024, 1, 1)), vec![]));

        let q = Query::TimeRange {
            start: None,
            end: Some(ts(2023, 1, 1)),
        };
        assert_eq!(result_count(idx.apply_query(&q)), 1);
    }

    #[test]
    fn test_time_range_open_end() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(Some(ts(2022, 1, 1)), vec![]));
        idx.push_event(make_event(Some(ts(2024, 1, 1)), vec![]));

        let q = Query::TimeRange {
            start: Some(ts(2023, 1, 1)),
            end: None,
        };
        assert_eq!(result_count(idx.apply_query(&q)), 1);
    }

    #[test]
    fn test_event_without_timestamp_excluded_from_time_range() {
        let mut idx = Index::new(None);
        idx.push_event(make_event(None, vec![]));

        let q = Query::TimeRange {
            start: None,
            end: None,
        };
        assert_eq!(result_count(idx.apply_query(&q)), 0);
    }

    fn make_event_raw(raw: &str, fields: Vec<(&str, Value)>) -> Event {
        let map = fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        Event::new(None, raw.to_string(), map)
    }

    // --- And ---

    #[test]
    fn test_and_two_field_match() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![
                ("level", Value::String("error".into())),
                ("service", Value::String("auth".into())),
            ],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![
                ("level", Value::String("error".into())),
                ("service", Value::String("api".into())),
            ],
        ));
        idx.push_event(make_event_raw(
            "e3",
            vec![
                ("level", Value::String("info".into())),
                ("service", Value::String("auth".into())),
            ],
        ));

        let q = Query::And(vec![
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("error".into()),
            },
            Query::FieldComparison {
                field: "service".into(),
                op: ComparisonOp::Eq,
                value: Value::String("auth".into()),
            },
        ]);
        assert_eq!(result_count(idx.apply_query(&q)), 1);
    }

    #[test]
    fn test_and_no_matches() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![
                ("level", Value::String("error".into())),
                ("service", Value::String("api".into())),
            ],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![
                ("level", Value::String("info".into())),
                ("service", Value::String("auth".into())),
            ],
        ));

        let q = Query::And(vec![
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("error".into()),
            },
            Query::FieldComparison {
                field: "service".into(),
                op: ComparisonOp::Eq,
                value: Value::String("auth".into()),
            },
        ]);
        assert_eq!(result_count(idx.apply_query(&q)), 0);
    }

    #[test]
    fn test_and_all_match() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![
                ("level", Value::String("error".into())),
                ("service", Value::String("auth".into())),
            ],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![
                ("level", Value::String("error".into())),
                ("service", Value::String("auth".into())),
            ],
        ));

        let q = Query::And(vec![
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("error".into()),
            },
            Query::FieldComparison {
                field: "service".into(),
                op: ComparisonOp::Eq,
                value: Value::String("auth".into()),
            },
        ]);
        assert_eq!(result_count(idx.apply_query(&q)), 2);
    }

    #[test]
    fn test_and_field_and_time_range() {
        let mut idx = Index::new(None);
        idx.push_event(Event::new(
            Some(ts(2024, 6, 1)),
            "e1".to_string(),
            vec![("level".to_string(), Value::String("error".into()))]
                .into_iter()
                .collect(),
        ));
        idx.push_event(Event::new(
            Some(ts(2024, 6, 1)),
            "e2".to_string(),
            vec![("level".to_string(), Value::String("info".into()))]
                .into_iter()
                .collect(),
        ));
        idx.push_event(Event::new(
            Some(ts(2025, 1, 1)),
            "e3".to_string(),
            vec![("level".to_string(), Value::String("error".into()))]
                .into_iter()
                .collect(),
        ));

        let q = Query::And(vec![
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("error".into()),
            },
            Query::TimeRange {
                start: Some(ts(2024, 1, 1)),
                end: Some(ts(2024, 12, 31)),
            },
        ]);
        assert_eq!(result_count(idx.apply_query(&q)), 1);
    }

    // --- Or ---

    #[test]
    fn test_or_disjoint_sets() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("level", Value::String("error".into()))],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![("level", Value::String("warn".into()))],
        ));
        idx.push_event(make_event_raw(
            "e3",
            vec![("level", Value::String("info".into()))],
        ));

        let q = Query::Or(vec![
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("error".into()),
            },
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("warn".into()),
            },
        ]);
        assert_eq!(result_count(idx.apply_query(&q)), 2);
    }

    #[test]
    fn test_or_overlapping_sets_no_dedup() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![
                ("level", Value::String("error".into())),
                ("service", Value::String("auth".into())),
            ],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![
                ("level", Value::String("info".into())),
                ("service", Value::String("auth".into())),
            ],
        ));

        // e1 matches both sub-queries — should appear once, not twice
        let q = Query::Or(vec![
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("error".into()),
            },
            Query::FieldComparison {
                field: "service".into(),
                op: ComparisonOp::Eq,
                value: Value::String("auth".into()),
            },
        ]);
        assert_eq!(result_count(idx.apply_query(&q)), 2);
    }

    #[test]
    fn test_or_no_matches() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("level", Value::String("info".into()))],
        ));

        let q = Query::Or(vec![
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("error".into()),
            },
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("warn".into()),
            },
        ]);
        assert_eq!(result_count(idx.apply_query(&q)), 0);
    }

    #[test]
    fn test_or_all_match() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("level", Value::String("error".into()))],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![("level", Value::String("warn".into()))],
        ));

        let q = Query::Or(vec![
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("error".into()),
            },
            Query::FieldComparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: Value::String("warn".into()),
            },
        ]);
        assert_eq!(result_count(idx.apply_query(&q)), 2);
    }

    // --- Not ---

    #[test]
    fn test_not_excludes_matches() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("status", Value::Number(500.0))]));
        idx.push_event(make_event_raw("e2", vec![("status", Value::Number(200.0))]));
        idx.push_event(make_event_raw("e3", vec![("status", Value::Number(500.0))]));

        let q = Query::Not(Box::new(eq("status", Value::Number(500.0))));
        let raws: Vec<&str> = idx.apply_query(&q).iter().map(|e| e.raw.as_str()).collect();
        assert_eq!(raws, vec!["e2"]);
    }

    #[test]
    fn test_not_includes_events_missing_field() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("status", Value::Number(500.0))]));
        idx.push_event(make_event_raw(
            "e2",
            vec![("level", Value::String("info".into()))],
        ));

        // The complement is over all events, so events without the field match too.
        let q = Query::Not(Box::new(eq("status", Value::Number(500.0))));
        let raws: Vec<&str> = idx.apply_query(&q).iter().map(|e| e.raw.as_str()).collect();
        assert_eq!(raws, vec!["e2"]);
    }

    #[test]
    fn test_not_no_matches_returns_all() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("status", Value::Number(200.0))]));
        idx.push_event(make_event_raw("e2", vec![("status", Value::Number(404.0))]));

        let q = Query::Not(Box::new(eq("status", Value::Number(500.0))));
        assert_eq!(result_count(idx.apply_query(&q)), 2);
    }

    #[test]
    fn test_and_with_not() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![
                ("status", Value::Number(500.0)),
                ("level", Value::String("info".into())),
            ],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![
                ("status", Value::Number(500.0)),
                ("level", Value::String("error".into())),
            ],
        ));
        idx.push_event(make_event_raw(
            "e3",
            vec![
                ("status", Value::Number(200.0)),
                ("level", Value::String("error".into())),
            ],
        ));

        let q = Query::And(vec![
            eq("status", Value::Number(500.0)),
            Query::Not(Box::new(eq("level", Value::String("info".into())))),
        ]);
        let raws: Vec<&str> = idx.apply_query(&q).iter().map(|e| e.raw.as_str()).collect();
        assert_eq!(raws, vec!["e2"]);
    }

    // --- Ordering & matching_indices ---

    #[test]
    fn test_results_in_insertion_order() {
        let mut idx = Index::new(None);
        for (raw, level) in [
            ("e1", "error"),
            ("e2", "info"),
            ("e3", "error"),
            ("e4", "info"),
            ("e5", "error"),
        ] {
            idx.push_event(make_event_raw(
                raw,
                vec![("level", Value::String(level.into()))],
            ));
        }

        let q = Query::FieldComparison {
            field: "level".into(),
            op: ComparisonOp::Ne,
            value: Value::String("info".into()),
        };
        let raws: Vec<&str> = idx.apply_query(&q).iter().map(|e| e.raw.as_str()).collect();
        assert_eq!(raws, vec!["e1", "e3", "e5"]);
    }

    #[test]
    fn test_or_containing_and() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("a", Value::Number(1.0)), ("b", Value::Number(2.0))],
        ));
        idx.push_event(make_event_raw("e2", vec![("c", Value::Number(3.0))]));
        idx.push_event(make_event_raw("e3", vec![("a", Value::Number(1.0))]));

        let q = Query::Or(vec![
            Query::And(vec![
                eq("a", Value::Number(1.0)),
                eq("b", Value::Number(2.0)),
            ]),
            eq("c", Value::Number(3.0)),
        ]);
        let raws: Vec<&str> = idx.apply_query(&q).iter().map(|e| e.raw.as_str()).collect();
        assert_eq!(raws, vec!["e1", "e2"]);
    }

    #[test]
    fn test_matching_indices_sorted_deduped() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![
                ("level", Value::String("error".into())),
                ("service", Value::String("auth".into())),
            ],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![("level", Value::String("error".into()))],
        ));

        // e1 matches both arms of the OR — must appear once
        let q = Query::Or(vec![
            eq("level", Value::String("error".into())),
            eq("service", Value::String("auth".into())),
        ]);
        assert_eq!(idx.matching_indices(&q), vec![0, 1]);
    }

    // --- Aggregations ---

    fn make_plan(query: Query, aggregation: Option<Aggregation>) -> QueryPlan {
        QueryPlan::new(query, aggregation, None)
    }

    fn eq(field: &str, value: Value) -> Query {
        Query::FieldComparison {
            field: field.to_string(),
            op: ComparisonOp::Eq,
            value,
        }
    }

    #[test]
    fn test_count_basic() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("status", Value::Number(500.0))]));
        idx.push_event(make_event_raw("e2", vec![("status", Value::Number(500.0))]));
        idx.push_event(make_event_raw("e3", vec![("status", Value::Number(200.0))]));

        let plan = make_plan(eq("status", Value::Number(500.0)), Some(Aggregation::Count));
        assert_eq!(idx.apply_query_plan(&plan).unwrap(), QueryResult::Count(2));
    }

    #[test]
    fn test_count_no_matches() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("status", Value::Number(200.0))]));

        let plan = make_plan(eq("status", Value::Number(500.0)), Some(Aggregation::Count));
        assert_eq!(idx.apply_query_plan(&plan).unwrap(), QueryResult::Count(0));
    }

    #[test]
    fn test_avg_basic() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("latency", Value::Number(100.0))],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![("latency", Value::Number(200.0))],
        ));
        idx.push_event(make_event_raw(
            "e3",
            vec![("latency", Value::Number(300.0))],
        ));

        let plan = make_plan(
            Query::FieldComparison {
                field: "latency".into(),
                op: ComparisonOp::Gt,
                value: Value::Number(0.0),
            },
            Some(Aggregation::Average("latency".into())),
        );
        assert_eq!(
            idx.apply_query_plan(&plan).unwrap(),
            QueryResult::Scalar(200.0)
        );
    }

    #[test]
    fn test_avg_skips_events_missing_field() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("latency", Value::Number(100.0))],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![("latency", Value::Number(300.0))],
        ));
        idx.push_event(make_event_raw("e3", vec![])); // no latency field

        let plan = make_plan(
            Query::FieldComparison {
                field: "latency".into(),
                op: ComparisonOp::Gt,
                value: Value::Number(0.0),
            },
            Some(Aggregation::Average("latency".into())),
        );
        // average of 100 and 300 only, e3 skipped
        assert_eq!(
            idx.apply_query_plan(&plan).unwrap(),
            QueryResult::Scalar(200.0)
        );
    }

    #[test]
    fn test_avg_error_no_matching_events() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("status", Value::Number(200.0))]));

        let plan = make_plan(
            eq("status", Value::Number(500.0)),
            Some(Aggregation::Average("latency".into())),
        );
        assert!(matches!(
            idx.apply_query_plan(&plan),
            Err(AggregationError::NoMatchingEvents)
        ));
    }

    #[test]
    fn test_avg_error_field_not_found() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("status", Value::Number(500.0))]));

        let plan = make_plan(
            eq("status", Value::Number(500.0)),
            Some(Aggregation::Average("latency".into())),
        );
        assert!(matches!(
            idx.apply_query_plan(&plan),
            Err(AggregationError::FieldNotFound(_))
        ));
    }

    #[test]
    fn test_avg_error_incompatible_type_string() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("level", Value::String("ERROR".into()))],
        ));

        let plan = make_plan(
            eq("level", Value::String("ERROR".into())),
            Some(Aggregation::Average("level".into())),
        );
        assert!(matches!(
            idx.apply_query_plan(&plan),
            Err(AggregationError::IncompatibleType(_))
        ));
    }

    #[test]
    fn test_avg_error_incompatible_type_bool() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("active", Value::Bool(true))]));

        let plan = make_plan(
            eq("active", Value::Bool(true)),
            Some(Aggregation::Average("active".into())),
        );
        assert!(matches!(
            idx.apply_query_plan(&plan),
            Err(AggregationError::IncompatibleType(_))
        ));
    }

    fn latency_index() -> Index {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("latency", Value::Number(100.0))],
        ));
        idx.push_event(make_event_raw(
            "e2",
            vec![("latency", Value::Number(300.0))],
        ));
        idx.push_event(make_event_raw(
            "e3",
            vec![("latency", Value::Number(200.0))],
        ));
        idx
    }

    #[test]
    fn test_sum_basic() {
        let idx = latency_index();

        let plan = make_plan(Query::All(), Some(Aggregation::Sum("latency".into())));
        assert_eq!(
            idx.apply_query_plan(&plan).unwrap(),
            QueryResult::Scalar(600.0)
        );
    }

    #[test]
    fn test_min_basic() {
        let idx = latency_index();

        let plan = make_plan(Query::All(), Some(Aggregation::Min("latency".into())));
        assert_eq!(
            idx.apply_query_plan(&plan).unwrap(),
            QueryResult::Scalar(100.0)
        );
    }

    #[test]
    fn test_max_basic() {
        let idx = latency_index();

        let plan = make_plan(Query::All(), Some(Aggregation::Max("latency".into())));
        assert_eq!(
            idx.apply_query_plan(&plan).unwrap(),
            QueryResult::Scalar(300.0)
        );
    }

    #[test]
    fn test_sum_error_no_matching_events() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("status", Value::Number(200.0))]));

        let plan = make_plan(
            eq("status", Value::Number(500.0)),
            Some(Aggregation::Sum("latency".into())),
        );
        assert!(matches!(
            idx.apply_query_plan(&plan),
            Err(AggregationError::NoMatchingEvents)
        ));
    }

    #[test]
    fn test_min_error_no_matching_events() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("status", Value::Number(200.0))]));

        let plan = make_plan(
            eq("status", Value::Number(500.0)),
            Some(Aggregation::Min("latency".into())),
        );
        assert!(matches!(
            idx.apply_query_plan(&plan),
            Err(AggregationError::NoMatchingEvents)
        ));
    }

    #[test]
    fn test_max_error_no_matching_events() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw("e1", vec![("status", Value::Number(200.0))]));

        let plan = make_plan(
            eq("status", Value::Number(500.0)),
            Some(Aggregation::Max("latency".into())),
        );
        assert!(matches!(
            idx.apply_query_plan(&plan),
            Err(AggregationError::NoMatchingEvents)
        ));
    }

    #[test]
    fn test_sum_error_incompatible_type() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("level", Value::String("ERROR".into()))],
        ));

        let plan = make_plan(
            eq("level", Value::String("ERROR".into())),
            Some(Aggregation::Sum("level".into())),
        );
        assert!(matches!(
            idx.apply_query_plan(&plan),
            Err(AggregationError::IncompatibleType(_))
        ));
    }

    #[test]
    fn test_min_error_incompatible_type() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("level", Value::String("ERROR".into()))],
        ));

        let plan = make_plan(
            eq("level", Value::String("ERROR".into())),
            Some(Aggregation::Min("level".into())),
        );
        assert!(matches!(
            idx.apply_query_plan(&plan),
            Err(AggregationError::IncompatibleType(_))
        ));
    }

    #[test]
    fn test_max_error_incompatible_type() {
        let mut idx = Index::new(None);
        idx.push_event(make_event_raw(
            "e1",
            vec![("level", Value::String("ERROR".into()))],
        ));

        let plan = make_plan(
            eq("level", Value::String("ERROR".into())),
            Some(Aggregation::Max("level".into())),
        );
        assert!(matches!(
            idx.apply_query_plan(&plan),
            Err(AggregationError::IncompatibleType(_))
        ));
    }

    // --- Limits ---

    fn level_index() -> Index {
        let mut idx = Index::new(None);
        for (raw, level) in [
            ("e1", "error"),
            ("e2", "info"),
            ("e3", "error"),
            ("e4", "error"),
            ("e5", "error"),
        ] {
            idx.push_event(make_event_raw(
                raw,
                vec![("level", Value::String(level.into()))],
            ));
        }
        idx
    }

    fn result_raws<'a>(result: QueryResult<'a>) -> Vec<&'a str> {
        match result {
            QueryResult::Events(events) => events.iter().map(|e| e.raw.as_str()).collect(),
            other => panic!("expected events, got {:?}", other),
        }
    }

    #[test]
    fn test_limit_head_truncates_to_first_n() {
        let idx = level_index();
        let plan = QueryPlan::new(
            eq("level", Value::String("error".into())),
            None,
            Some(Limit::Head(2)),
        );
        assert_eq!(
            result_raws(idx.apply_query_plan(&plan).unwrap()),
            vec!["e1", "e3"]
        );
    }

    #[test]
    fn test_limit_tail_keeps_last_n_ascending() {
        let idx = level_index();
        let plan = QueryPlan::new(
            eq("level", Value::String("error".into())),
            None,
            Some(Limit::Tail(2)),
        );
        assert_eq!(
            result_raws(idx.apply_query_plan(&plan).unwrap()),
            vec!["e4", "e5"]
        );
    }

    #[test]
    fn test_limit_larger_than_results_returns_all() {
        let idx = level_index();
        for limit in [Limit::Head(10), Limit::Tail(10)] {
            let plan = QueryPlan::new(
                eq("level", Value::String("error".into())),
                None,
                Some(limit),
            );
            assert_eq!(
                result_raws(idx.apply_query_plan(&plan).unwrap()),
                vec!["e1", "e3", "e4", "e5"]
            );
        }
    }

    #[test]
    fn test_no_limit_returns_all_matches() {
        let idx = level_index();
        let plan = QueryPlan::new(eq("level", Value::String("error".into())), None, None);
        assert_eq!(
            result_raws(idx.apply_query_plan(&plan).unwrap()),
            vec!["e1", "e3", "e4", "e5"]
        );
    }

    // --- Bounded memory / retention ---

    fn numbered_event(i: usize) -> Event {
        let fields = vec![("n".to_string(), Value::Number(i as f32))]
            .into_iter()
            .collect();
        Event::new(
            Some(Utc.timestamp_opt(i as i64 + 1, 0).unwrap()),
            format!("e{i}"),
            fields,
        )
    }

    #[test]
    fn test_no_eviction_at_cap_plus_slack_boundary() {
        let max = 3;
        let mut idx = Index::new(Some(max));
        for i in 0..(max + SLACK) {
            idx.push_event(numbered_event(i));
        }

        // Compaction only triggers strictly past max + SLACK, so nothing is evicted yet.
        assert_eq!(idx.event_count(), max + SLACK);
    }

    #[test]
    fn test_eviction_past_cap_evicts_oldest() {
        let max = 3;
        let total = max + SLACK + 1;
        let mut idx = Index::new(Some(max));
        for i in 0..total {
            idx.push_event(numbered_event(i));
        }

        // One push past the boundary compacts back down to exactly `max`.
        assert_eq!(idx.event_count(), max);

        // The survivors are the most recent `max` events, in order.
        let survivors: Vec<String> = idx.events.iter().map(|e| e.raw.clone()).collect();
        let expected: Vec<String> = ((total - max)..total).map(|i| format!("e{i}")).collect();
        assert_eq!(survivors, expected);
    }

    #[test]
    fn test_queries_see_only_recent_after_eviction() {
        let max = 3;
        let total = max + SLACK + 1;
        let mut idx = Index::new(Some(max));
        for i in 0..total {
            idx.push_event(numbered_event(i));
        }

        // An evicted event's field value is no longer queryable.
        let evicted = Query::FieldComparison {
            field: "n".into(),
            op: ComparisonOp::Eq,
            value: Value::Number(0.0),
        };
        assert_eq!(result_count(idx.apply_query(&evicted)), 0);

        // A retained event still matches exactly once and resolves to the right event.
        let retained = Query::FieldComparison {
            field: "n".into(),
            op: ComparisonOp::Eq,
            value: Value::Number((total - 1) as f32),
        };
        let hits = idx.apply_query(&retained);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].raw, format!("e{}", total - 1));
    }

    #[test]
    fn test_indexes_have_no_stale_positions_after_reindex() {
        let max = 5;
        // Two full compaction cycles, landing exactly on a boundary so the count is `max`.
        let total = max + 2 * (SLACK + 1);
        let mut idx = Index::new(Some(max));
        for i in 0..total {
            idx.push_event(numbered_event(i));
        }
        assert_eq!(idx.event_count(), max);

        // Every position in field_index points into the live events vec and back
        // to the event that actually holds that value.
        for value_map in idx.field_index.values() {
            for (value, positions) in value_map {
                for &pos in positions {
                    assert!(pos < idx.events.len());
                    assert_eq!(&idx.events[pos].fields["n"], value);
                }
            }
        }

        // Same for the time index.
        for (timestamp, positions) in &idx.time_index {
            for &pos in positions {
                assert!(pos < idx.events.len());
                assert_eq!(idx.events[pos].timestamp, Some(*timestamp));
            }
        }
    }

    #[test]
    fn test_pushes_after_compaction_are_indexed_correctly() {
        let max = 3;
        let total = max + SLACK + 1;
        let mut idx = Index::new(Some(max));
        for i in 0..total {
            idx.push_event(numbered_event(i));
        }

        // Events added after a compaction must be positioned and indexed correctly.
        idx.push_event(numbered_event(total));
        idx.push_event(numbered_event(total + 1));

        let q = Query::FieldComparison {
            field: "n".into(),
            op: ComparisonOp::Eq,
            value: Value::Number((total + 1) as f32),
        };
        let hits = idx.apply_query(&q);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].raw, format!("e{}", total + 1));
    }
}
