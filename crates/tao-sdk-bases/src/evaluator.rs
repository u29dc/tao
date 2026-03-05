use std::cmp::Ordering;

use serde_json::Value as JsonValue;

use crate::ast::{BaseFilterOp, BaseNullOrder};

/// Evaluator failures for typed comparator/filter execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BaseEvalError {
    TypeMismatch {
        op: BaseFilterOp,
        expected: &'static str,
    },
}

/// Evaluate one filter predicate over an optional row value.
pub fn evaluate_filter(
    row_value: Option<&JsonValue>,
    op: BaseFilterOp,
    filter_value: &JsonValue,
) -> Result<bool, BaseEvalError> {
    let matched = match op {
        BaseFilterOp::Eq => row_value.is_some_and(|value| value == filter_value),
        BaseFilterOp::NotEq => row_value.is_none_or(|value| value != filter_value),
        BaseFilterOp::Gt => {
            row_value.is_some_and(|value| compare_json_values(value, filter_value).is_gt())
        }
        BaseFilterOp::Gte => {
            row_value.is_some_and(|value| compare_json_values(value, filter_value).is_ge())
        }
        BaseFilterOp::Lt => {
            row_value.is_some_and(|value| compare_json_values(value, filter_value).is_lt())
        }
        BaseFilterOp::Lte => {
            row_value.is_some_and(|value| compare_json_values(value, filter_value).is_le())
        }
        BaseFilterOp::Contains => {
            row_value.is_some_and(|value| value_contains(value, filter_value))
        }
        BaseFilterOp::In => {
            row_value.is_some_and(|value| filter_contains_value(filter_value, value))
        }
        BaseFilterOp::NotIn => {
            row_value.is_none_or(|value| !filter_contains_value(filter_value, value))
        }
        BaseFilterOp::Exists => {
            let expected_exists = filter_value.as_bool().unwrap_or(true);
            row_value.is_some() == expected_exists
        }
        BaseFilterOp::StartsWith => {
            row_value
                .and_then(json_scalar_to_string)
                .is_some_and(|value| {
                    json_scalar_to_string(filter_value)
                        .is_some_and(|prefix| value.starts_with(&prefix))
                })
        }
        BaseFilterOp::NotStartsWith => {
            row_value
                .and_then(json_scalar_to_string)
                .is_none_or(|value| {
                    json_scalar_to_string(filter_value)
                        .is_none_or(|prefix| !value.starts_with(&prefix))
                })
        }
        BaseFilterOp::EndsWith => row_value
            .and_then(json_scalar_to_string)
            .is_some_and(|value| {
                json_scalar_to_string(filter_value).is_some_and(|suffix| value.ends_with(&suffix))
            }),
    };

    if matches!(
        op,
        BaseFilterOp::StartsWith | BaseFilterOp::NotStartsWith | BaseFilterOp::EndsWith
    ) && json_scalar_to_string(filter_value).is_none()
    {
        return Err(BaseEvalError::TypeMismatch {
            op,
            expected: "scalar filter value",
        });
    }

    Ok(matched)
}

/// Compare two optional json values with explicit null ordering.
pub fn compare_optional_json_values(
    left: Option<&JsonValue>,
    right: Option<&JsonValue>,
    null_order: BaseNullOrder,
) -> Ordering {
    let left = left.filter(|value| !value.is_null());
    let right = right.filter(|value| !value.is_null());
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => match null_order {
            BaseNullOrder::First => Ordering::Less,
            BaseNullOrder::Last => Ordering::Greater,
        },
        (Some(_), None) => match null_order {
            BaseNullOrder::First => Ordering::Greater,
            BaseNullOrder::Last => Ordering::Less,
        },
        (Some(left), Some(right)) => compare_json_values(left, right),
    }
}

/// Compare two json values with deterministic type ranking.
pub fn compare_json_values(left: &JsonValue, right: &JsonValue) -> Ordering {
    let left_rank = json_type_rank(left);
    let right_rank = json_type_rank(right);
    if left_rank != right_rank {
        return left_rank.cmp(&right_rank);
    }

    match (left, right) {
        (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left.cmp(right),
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            let left = left.as_f64().unwrap_or(0.0);
            let right = right.as_f64().unwrap_or(0.0);
            left.partial_cmp(&right).unwrap_or(Ordering::Equal)
        }
        (JsonValue::String(left), JsonValue::String(right)) => left.cmp(right),
        (JsonValue::Array(left), JsonValue::Array(right)) => left.len().cmp(&right.len()),
        (JsonValue::Object(left), JsonValue::Object(right)) => left.len().cmp(&right.len()),
        _ => left.to_string().cmp(&right.to_string()),
    }
}

/// Return deterministic scalar string representation for comparisons.
pub fn json_scalar_to_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_contains(value: &JsonValue, filter_value: &JsonValue) -> bool {
    let Some(needle) = json_scalar_to_string(filter_value) else {
        return false;
    };
    let needle = needle.to_lowercase();

    match value {
        JsonValue::Array(values) => values
            .iter()
            .any(|entry| value_contains(entry, filter_value)),
        _ => json_scalar_to_string(value)
            .unwrap_or_else(|| value.to_string())
            .to_lowercase()
            .contains(&needle),
    }
}

fn filter_contains_value(filter_value: &JsonValue, row_value: &JsonValue) -> bool {
    match filter_value {
        JsonValue::Array(values) => values.iter().any(|value| value == row_value),
        _ => false,
    }
}

fn json_type_rank(value: &JsonValue) -> u8 {
    match value {
        JsonValue::Null => 0,
        JsonValue::Bool(_) => 1,
        JsonValue::Number(_) => 2,
        JsonValue::String(_) => 3,
        JsonValue::Array(_) => 4,
        JsonValue::Object(_) => 5,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{compare_optional_json_values, evaluate_filter};
    use crate::ast::{BaseFilterOp, BaseNullOrder};

    #[test]
    fn filter_operators_cover_full_set() {
        assert!(evaluate_filter(Some(&json!(5)), BaseFilterOp::Eq, &json!(5)).expect("eq"));
        assert!(evaluate_filter(Some(&json!(5)), BaseFilterOp::NotEq, &json!(4)).expect("neq"));
        assert!(evaluate_filter(Some(&json!(5)), BaseFilterOp::Gt, &json!(4)).expect("gt"));
        assert!(evaluate_filter(Some(&json!(5)), BaseFilterOp::Gte, &json!(5)).expect("gte"));
        assert!(evaluate_filter(Some(&json!(5)), BaseFilterOp::Lt, &json!(6)).expect("lt"));
        assert!(evaluate_filter(Some(&json!(5)), BaseFilterOp::Lte, &json!(5)).expect("lte"));
        assert!(
            evaluate_filter(Some(&json!("han")), BaseFilterOp::Contains, &json!("ha"))
                .expect("contains")
        );
        assert!(
            evaluate_filter(Some(&json!("a")), BaseFilterOp::In, &json!(["a", "b"])).expect("in")
        );
        assert!(
            evaluate_filter(Some(&json!("prod")), BaseFilterOp::StartsWith, &json!("pr"))
                .expect("starts")
        );
        assert!(
            evaluate_filter(Some(&json!("prod")), BaseFilterOp::EndsWith, &json!("od"))
                .expect("ends")
        );
    }

    #[test]
    fn starts_with_type_mismatch_is_reported() {
        let error = evaluate_filter(
            Some(&json!("abc")),
            BaseFilterOp::StartsWith,
            &json!({ "bad": 1 }),
        )
        .expect_err("type mismatch");
        assert!(matches!(error, super::BaseEvalError::TypeMismatch { .. }));
    }

    #[test]
    fn null_ordering_is_respected() {
        assert_eq!(
            compare_optional_json_values(None, Some(&json!(1)), BaseNullOrder::First),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_optional_json_values(None, Some(&json!(1)), BaseNullOrder::Last),
            std::cmp::Ordering::Greater
        );
    }
}
