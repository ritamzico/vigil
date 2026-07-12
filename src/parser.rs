use crate::parser::ParseError::{IncorrectFormat, UnknownOperator};
use crate::query::{Aggregation, ComparisonOp, Query, QueryPlan};
use crate::value::Value;
use chrono::DateTime;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ParseError {
    #[error("incorrect format: {0}")]
    IncorrectFormat(String),

    #[error("unknown operator: {0}")]
    UnknownOperator(String),
}

fn unexpected_token(tokens: &[&str], pos: usize) -> ParseError {
    IncorrectFormat(format!("unexpected token '{}'", tokens[pos]))
}

// Splits on whitespace, with '(' and ')' as their own tokens even when not
// surrounded by spaces, e.g. "(a = 1)" -> ["(", "a", "=", "1", ")"].
fn tokenize(query_string: &str) -> Vec<String> {
    let mut tokens = Vec::new();

    for word in query_string.split_whitespace() {
        let mut current = String::new();
        for c in word.chars() {
            if c == '(' || c == ')' {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(c.to_string());
            } else {
                current.push(c);
            }
        }
        if !current.is_empty() {
            tokens.push(current);
        }
    }

    tokens
}

pub fn parse_query(query_string: &str, time_field: Option<&str>) -> Result<QueryPlan, ParseError> {
    let owned_tokens = tokenize(query_string);
    let tokens: Vec<&str> = owned_tokens.iter().map(String::as_str).collect();

    if *tokens
        .get(0)
        .ok_or_else(|| IncorrectFormat(String::from("empty query")))?
        == "|"
    {
        let aggregation = parse_aggregation(&tokens, 1)?;
        return Ok(QueryPlan::new(Query::All(), aggregation));
    }

    let (query, pos) = parse_or(&tokens, 0, time_field)?;

    if pos == tokens.len() {
        return Ok(QueryPlan::new(query, None));
    }

    if tokens[pos] != "|" {
        return Err(unexpected_token(&tokens, pos));
    }

    let aggregation = parse_aggregation(&tokens, pos + 1)?;

    Ok(QueryPlan::new(query, aggregation))
}

fn parse_aggregation(tokens: &[&str], pos: usize) -> Result<Option<Aggregation>, ParseError> {
    match tokens
        .get(pos)
        .ok_or_else(|| IncorrectFormat(String::from("no aggregator after '|'")))?
        .to_lowercase()
        .as_str()
    {
        "count" => match tokens.get(pos + 1).map(|t| t.to_lowercase()).as_deref() {
            Some("by") => {
                let field = tokens
                    .get(pos + 2)
                    .ok_or_else(|| IncorrectFormat(String::from("no field to count by")))?;
                Ok(Some(Aggregation::CountBy(field.to_string())))
            }
            Some(_) => Err(unexpected_token(&tokens, pos + 1)),
            None => Ok(Some(Aggregation::Count)),
        },
        "avg" => {
            let field = *&tokens
                .get(pos + 1)
                .ok_or_else(|| IncorrectFormat(String::from("'avg' requires a field name, e.g.: avg latency_ms")))?;

            if tokens.get(pos + 2).is_some() {
                return Err(unexpected_token(&tokens, pos + 2));
            }

            Ok(Some(Aggregation::Average(field.to_string())))
        }
        token => {
            if let Some(digits) = token.strip_prefix('p') {
                let n: u8 = digits
                    .parse()
                    .map_err(|_| IncorrectFormat(format!("invalid percentile '{}'", token)))?;
                if n == 0 || n > 100 {
                    return Err(IncorrectFormat(format!(
                        "percentile must be 1-100, got {}",
                        n
                    )));
                }
                let field = tokens
                    .get(pos + 1)
                    .ok_or_else(|| {
                        IncorrectFormat(format!("no field for '{}' aggregation", token))
                    })?
                    .to_string();
                if tokens.get(pos + 2).is_some() {
                    return Err(unexpected_token(&tokens, pos + 2));
                }
                return Ok(Some(Aggregation::Percentage(field, n as f32 / 100.0)));
            }
            Err(IncorrectFormat(format!(
                "unknown aggregator '{}'; expected: count, avg, or p<N> (e.g. p99)",
                token
            )))
        }
    }
}

// or_expr = and_expr ("OR" and_expr)*
fn parse_or(tokens: &[&str], pos: usize, time_field: Option<&str>) -> Result<(Query, usize), ParseError> {
    let (first, mut pos) = parse_and(tokens, pos, time_field)?;
    let mut parts = vec![first];

    while tokens.get(pos) == Some(&"OR") {
        let (next, new_pos) = parse_and(tokens, pos + 1, time_field)?;
        parts.push(next);
        pos = new_pos;
    }

    if parts.len() == 1 {
        Ok((parts.remove(0), pos))
    } else {
        Ok((Query::Or(parts), pos))
    }
}

// and_expr = simple_expr ("AND" simple_expr)*
fn parse_and(tokens: &[&str], pos: usize, time_field: Option<&str>) -> Result<(Query, usize), ParseError> {
    let (first, mut pos) = parse_simple(tokens, pos, time_field)?;
    let mut parts = vec![first];

    while tokens.get(pos) == Some(&"AND") {
        let (next, new_pos) = parse_simple(tokens, pos + 1, time_field)?;
        parts.push(next);
        pos = new_pos;
    }

    if parts.len() == 1 {
        Ok((parts.remove(0), pos))
    } else {
        Ok((Query::And(parts), pos))
    }
}

// simple_expr = field op value  (always 3 tokens)
fn parse_simple(tokens: &[&str], pos: usize, time_field: Option<&str>) -> Result<(Query, usize), ParseError> {
    let field = tokens
        .get(pos)
        .ok_or_else(|| IncorrectFormat(String::from("expected field name")))?;

    if time_field == Some(field) {
        parse_time_range(tokens, pos)
    } else {
        parse_field_comparison(tokens, pos)
    }
}

fn parse_field_comparison(tokens: &[&str], pos: usize) -> Result<(Query, usize), ParseError> {
    let field = tokens
        .get(pos)
        .ok_or_else(|| IncorrectFormat(String::from("expected field name")))?
        .to_string();

    let op = match *tokens
        .get(pos + 1)
        .ok_or_else(|| IncorrectFormat(String::from("expected operator")))?
    {
        "=" => ComparisonOp::Eq,
        "!=" => ComparisonOp::Ne,
        "<" => ComparisonOp::Lt,
        "<=" => ComparisonOp::Le,
        ">" => ComparisonOp::Gt,
        ">=" => ComparisonOp::Ge,
        "~" => ComparisonOp::Contains,
        op => return Err(UnknownOperator(String::from(op))),
    };

    let value_token = tokens
        .get(pos + 2)
        .ok_or_else(|| IncorrectFormat(String::from("expected value")))?;

    // Substring search is always over strings — don't auto-type the value.
    let value = if op == ComparisonOp::Contains {
        Value::String(value_token.to_string())
    } else {
        Value::from_string(value_token)
    };

    Ok((Query::FieldComparison { field, op, value }, pos + 3))
}

fn parse_time_range(tokens: &[&str], pos: usize) -> Result<(Query, usize), ParseError> {
    let op = match *tokens
        .get(pos + 1)
        .ok_or_else(|| IncorrectFormat(String::from("expected operator")))?
    {
        "<" => ComparisonOp::Lt,
        ">" => ComparisonOp::Gt,
        op => return Err(UnknownOperator(String::from(op))),
    };

    let time_value = DateTime::parse_from_rfc3339(
        tokens
            .get(pos + 2)
            .ok_or_else(|| IncorrectFormat(String::from("expected datetime value")))?,
    )
    .map_err(|_| IncorrectFormat(String::from("invalid datetime, expected RFC 3339 (e.g. 2026-01-15T00:00:00Z)")))?
    .to_utc();

    let (start, end) = match op {
        ComparisonOp::Lt => (None, Some(time_value)),
        ComparisonOp::Gt => (Some(time_value), None),
        _ => panic!("unreachable"),
    };

    Ok((Query::TimeRange { start, end }, pos + 3))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;
    use chrono::Utc;

    fn dt(s: &str) -> chrono::DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().to_utc()
    }

    fn field(field: &str, op: ComparisonOp, value: Value) -> Query {
        Query::FieldComparison {
            field: field.to_string(),
            op,
            value,
        }
    }

    fn parse_filter(s: &str) -> Result<Query, ParseError> {
        parse_query(s, None).map(|plan| plan.query.clone())
    }

    fn parse_filter_with_time(s: &str) -> Result<Query, ParseError> {
        parse_query(s, Some("time")).map(|plan| plan.query.clone())
    }

    // --- Tokenizer ---

    #[test]
    fn test_tokenize_splits_parens() {
        assert_eq!(
            tokenize("(a = 1)"),
            vec!["(", "a", "=", "1", ")"]
        );
    }

    #[test]
    fn test_tokenize_plain_whitespace() {
        assert_eq!(tokenize("status = 500"), vec!["status", "=", "500"]);
    }

    // --- Field comparisons ---

    #[test]
    fn test_field_eq_number() {
        assert_eq!(
            parse_filter("status = 500"),
            Ok(field("status", ComparisonOp::Eq, Value::Number(500.0)))
        );
    }

    #[test]
    fn test_field_eq_string() {
        assert_eq!(
            parse_filter("level = ERROR"),
            Ok(field(
                "level",
                ComparisonOp::Eq,
                Value::String("ERROR".into())
            ))
        );
    }

    #[test]
    fn test_field_eq_bool() {
        assert_eq!(
            parse_filter("active = true"),
            Ok(field("active", ComparisonOp::Eq, Value::Bool(true)))
        );
    }

    #[test]
    fn test_field_ne() {
        assert_eq!(
            parse_filter("status != 200"),
            Ok(field("status", ComparisonOp::Ne, Value::Number(200.0)))
        );
    }

    #[test]
    fn test_field_lt() {
        assert_eq!(
            parse_filter("latency_ms < 100"),
            Ok(field("latency_ms", ComparisonOp::Lt, Value::Number(100.0)))
        );
    }

    #[test]
    fn test_field_le() {
        assert_eq!(
            parse_filter("latency_ms <= 100"),
            Ok(field("latency_ms", ComparisonOp::Le, Value::Number(100.0)))
        );
    }

    #[test]
    fn test_field_gt() {
        assert_eq!(
            parse_filter("latency_ms > 500"),
            Ok(field("latency_ms", ComparisonOp::Gt, Value::Number(500.0)))
        );
    }

    #[test]
    fn test_field_ge() {
        assert_eq!(
            parse_filter("latency_ms >= 500"),
            Ok(field("latency_ms", ComparisonOp::Ge, Value::Number(500.0)))
        );
    }

    #[test]
    fn test_field_contains() {
        assert_eq!(
            parse_filter("message ~ timeout"),
            Ok(field(
                "message",
                ComparisonOp::Contains,
                Value::String("timeout".into())
            ))
        );
    }

    #[test]
    fn test_field_contains_numeric_token_stays_string() {
        // '~' always searches string values, so the term is not auto-typed.
        assert_eq!(
            parse_filter("message ~ 500"),
            Ok(field(
                "message",
                ComparisonOp::Contains,
                Value::String("500".into())
            ))
        );
    }

    // --- Time range ---

    #[test]
    fn test_time_gt_sets_start() {
        assert_eq!(
            parse_filter_with_time("time > 2026-01-01T00:00:00Z"),
            Ok(Query::TimeRange {
                start: Some(dt("2026-01-01T00:00:00Z")),
                end: None,
            })
        );
    }

    #[test]
    fn test_time_lt_sets_end() {
        assert_eq!(
            parse_filter_with_time("time < 2026-01-01T00:00:00Z"),
            Ok(Query::TimeRange {
                start: None,
                end: Some(dt("2026-01-01T00:00:00Z")),
            })
        );
    }

    #[test]
    fn test_configured_field_parses_as_time_range() {
        // Any field name can be the time field — it's whatever the daemon was started with.
        assert!(matches!(
            parse_query("created_at > 2026-01-01T00:00:00Z", Some("created_at")),
            Ok(ref plan) if matches!(plan.query, Query::TimeRange { .. })
        ));
    }

    #[test]
    fn test_unconfigured_field_parses_as_field_comparison() {
        // Without a time_field configured, "time" is just a regular field.
        assert!(matches!(
            parse_filter("time > 2026-01-01T00:00:00Z"),
            Ok(Query::FieldComparison { .. })
        ));
    }

    // --- AND ---

    #[test]
    fn test_and_two_fields() {
        assert_eq!(
            parse_filter("status = 500 AND level = ERROR"),
            Ok(Query::And(vec![
                field("status", ComparisonOp::Eq, Value::Number(500.0)),
                field("level", ComparisonOp::Eq, Value::String("ERROR".into())),
            ]))
        );
    }

    #[test]
    fn test_and_three_fields() {
        assert_eq!(
            parse_filter("status = 500 AND level = ERROR AND method = POST"),
            Ok(Query::And(vec![
                field("status", ComparisonOp::Eq, Value::Number(500.0)),
                field("level", ComparisonOp::Eq, Value::String("ERROR".into())),
                field("method", ComparisonOp::Eq, Value::String("POST".into())),
            ]))
        );
    }

    // --- OR ---

    #[test]
    fn test_or_two_fields() {
        assert_eq!(
            parse_filter("status = 500 OR status = 404"),
            Ok(Query::Or(vec![
                field("status", ComparisonOp::Eq, Value::Number(500.0)),
                field("status", ComparisonOp::Eq, Value::Number(404.0)),
            ]))
        );
    }

    #[test]
    fn test_or_three_fields() {
        assert_eq!(
            parse_filter("status = 200 OR status = 201 OR status = 204"),
            Ok(Query::Or(vec![
                field("status", ComparisonOp::Eq, Value::Number(200.0)),
                field("status", ComparisonOp::Eq, Value::Number(201.0)),
                field("status", ComparisonOp::Eq, Value::Number(204.0)),
            ]))
        );
    }

    // --- Precedence: AND binds tighter than OR ---

    #[test]
    fn test_and_before_or_right_side() {
        // a=1 OR b=2 AND c=3  =>  a=1 OR (AND[b=2, c=3])
        assert_eq!(
            parse_filter("a = 1 OR b = 2 AND c = 3"),
            Ok(Query::Or(vec![
                field("a", ComparisonOp::Eq, Value::Number(1.0)),
                Query::And(vec![
                    field("b", ComparisonOp::Eq, Value::Number(2.0)),
                    field("c", ComparisonOp::Eq, Value::Number(3.0)),
                ]),
            ]))
        );
    }

    #[test]
    fn test_and_before_or_left_side() {
        // a=1 AND b=2 OR c=3  =>  (AND[a=1, b=2]) OR c=3
        assert_eq!(
            parse_filter("a = 1 AND b = 2 OR c = 3"),
            Ok(Query::Or(vec![
                Query::And(vec![
                    field("a", ComparisonOp::Eq, Value::Number(1.0)),
                    field("b", ComparisonOp::Eq, Value::Number(2.0)),
                ]),
                field("c", ComparisonOp::Eq, Value::Number(3.0)),
            ]))
        );
    }

    #[test]
    fn test_mixed_and_or() {
        // a=1 AND b=2 OR c=3 AND d=4  =>  (AND[a,b]) OR (AND[c,d])
        assert_eq!(
            parse_filter("a = 1 AND b = 2 OR c = 3 AND d = 4"),
            Ok(Query::Or(vec![
                Query::And(vec![
                    field("a", ComparisonOp::Eq, Value::Number(1.0)),
                    field("b", ComparisonOp::Eq, Value::Number(2.0)),
                ]),
                Query::And(vec![
                    field("c", ComparisonOp::Eq, Value::Number(3.0)),
                    field("d", ComparisonOp::Eq, Value::Number(4.0)),
                ]),
            ]))
        );
    }

    // --- Errors ---

    #[test]
    fn test_error_empty_string() {
        assert!(matches!(
            parse_query("", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_missing_operator() {
        assert!(matches!(
            parse_query("status", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_missing_value() {
        assert!(matches!(
            parse_query("status =", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_unknown_operator() {
        assert!(matches!(
            parse_query("status ?? 500", None),
            Err(ParseError::UnknownOperator(_))
        ));
    }

    #[test]
    fn test_error_trailing_tokens() {
        assert!(matches!(
            parse_query("status = 500 garbage", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_trailing_and() {
        assert!(matches!(
            parse_query("status = 500 AND", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_trailing_or() {
        assert!(matches!(
            parse_query("status = 500 OR", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_time_unsupported_operator() {
        assert!(matches!(
            parse_query("time = 2026-01-01T00:00:00Z", Some("time")),
            Err(ParseError::UnknownOperator(_))
        ));
    }

    #[test]
    fn test_error_time_invalid_datetime() {
        assert!(matches!(
            parse_query("time > not-a-date", Some("time")),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_time_missing_value() {
        assert!(matches!(
            parse_query("time >", Some("time")),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    // --- Aggregations ---

    #[test]
    fn test_count_aggregation() {
        let plan = parse_query("status = 500 | count", None).unwrap();
        assert_eq!(
            plan.query,
            field("status", ComparisonOp::Eq, Value::Number(500.0))
        );
        assert!(matches!(plan.aggregation, Some(Aggregation::Count)));
    }

    #[test]
    fn test_avg_aggregation() {
        let plan = parse_query("status = 500 | avg latency_ms", None).unwrap();
        assert_eq!(
            plan.query,
            field("status", ComparisonOp::Eq, Value::Number(500.0))
        );
        assert!(
            matches!(plan.aggregation, Some(Aggregation::Average(f)) if f == "latency_ms")
        );
    }

    #[test]
    fn test_count_with_and_filter() {
        let plan = parse_query("status = 500 AND level = ERROR | count", None).unwrap();
        assert!(matches!(plan.aggregation, Some(Aggregation::Count)));
    }

    #[test]
    fn test_no_aggregation() {
        let plan = parse_query("status = 500", None).unwrap();
        assert!(plan.aggregation.is_none());
    }

    #[test]
    fn test_error_pipe_no_aggregator() {
        assert!(matches!(
            parse_query("status = 500 |", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_unknown_aggregator() {
        assert!(matches!(
            parse_query("status = 500 | sum", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_count_with_extra_token() {
        assert!(matches!(
            parse_query("status = 500 | count extra", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_avg_missing_field() {
        assert!(matches!(
            parse_query("status = 500 | avg", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_avg_with_extra_token() {
        assert!(matches!(
            parse_query("status = 500 | avg latency_ms extra", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_p99_aggregation() {
        let plan = parse_query("status = 500 | p99 latency_ms", None).unwrap();
        assert!(
            matches!(plan.aggregation, Some(Aggregation::Percentage(f, p)) if f == "latency_ms" && (p - 0.99).abs() < 1e-6)
        );
    }

    #[test]
    fn test_p50_aggregation() {
        let plan = parse_query("status = 500 | p50 latency_ms", None).unwrap();
        assert!(
            matches!(plan.aggregation, Some(Aggregation::Percentage(f, p)) if f == "latency_ms" && (p - 0.50).abs() < 1e-6)
        );
    }

    #[test]
    fn test_p1_aggregation() {
        let plan = parse_query("status = 500 | p1 latency_ms", None).unwrap();
        assert!(
            matches!(plan.aggregation, Some(Aggregation::Percentage(_, p)) if (p - 0.01).abs() < 1e-6)
        );
    }

    #[test]
    fn test_p100_aggregation() {
        let plan = parse_query("status = 500 | p100 latency_ms", None).unwrap();
        assert!(
            matches!(plan.aggregation, Some(Aggregation::Percentage(_, p)) if (p - 1.0).abs() < 1e-6)
        );
    }

    #[test]
    fn test_error_p0_out_of_range() {
        assert!(matches!(
            parse_query("status = 500 | p0 latency_ms", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_percentile_missing_field() {
        assert!(matches!(
            parse_query("status = 500 | p99", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_percentile_extra_token() {
        assert!(matches!(
            parse_query("status = 500 | p99 latency_ms extra", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }

    #[test]
    fn test_error_p_no_digits() {
        assert!(matches!(
            parse_query("status = 500 | p latency_ms", None),
            Err(ParseError::IncorrectFormat(_))
        ));
    }
}
