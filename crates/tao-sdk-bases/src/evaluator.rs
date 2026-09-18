use std::cmp::Ordering;

use serde_json::Value as JsonValue;

use crate::ast::{BaseFilterOp, BaseNullOrder};
pub use tao_sdk_core::{compare_json_values, compare_predicate_values, json_values_equal};

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
    validate_filter_operand(op, filter_value)?;
    if matches!(
        op,
        BaseFilterOp::Gt | BaseFilterOp::Gte | BaseFilterOp::Lt | BaseFilterOp::Lte
    ) && !filter_value.is_null()
        && row_value.is_some_and(|value| {
            !value.is_null() && compare_predicate_values(value, filter_value).is_none()
        })
    {
        return Err(BaseEvalError::TypeMismatch {
            op,
            expected: "matching scalar operand types",
        });
    }
    let matched = match op {
        BaseFilterOp::Eq => row_value.is_some_and(|value| json_values_equal(value, filter_value)),
        BaseFilterOp::NotEq => {
            row_value.is_none_or(|value| !json_values_equal(value, filter_value))
        }
        BaseFilterOp::Gt => row_value
            .and_then(|value| compare_predicate_values(value, filter_value))
            .is_some_and(|order| order.is_gt()),
        BaseFilterOp::Gte => row_value
            .and_then(|value| compare_predicate_values(value, filter_value))
            .is_some_and(|order| order.is_ge()),
        BaseFilterOp::Lt => row_value
            .and_then(|value| compare_predicate_values(value, filter_value))
            .is_some_and(|order| order.is_lt()),
        BaseFilterOp::Lte => row_value
            .and_then(|value| compare_predicate_values(value, filter_value))
            .is_some_and(|order| order.is_le()),
        BaseFilterOp::Contains => {
            row_value.is_some_and(|value| value_contains(value, filter_value))
        }
        BaseFilterOp::In => {
            row_value.is_some_and(|value| filter_contains_value(filter_value, value))
        }
        BaseFilterOp::NotIn => {
            row_value.is_none_or(|value| !filter_contains_value(filter_value, value))
        }
        BaseFilterOp::IsEmpty => {
            let empty = row_value.is_none_or(|value| match value {
                JsonValue::Null => true,
                JsonValue::String(value) => value.is_empty(),
                JsonValue::Array(value) => value.is_empty(),
                JsonValue::Object(value) => value.is_empty(),
                _ => false,
            });
            empty == filter_value.as_bool().unwrap_or(true)
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

/// Validate the operand once before evaluating a query against rows.
pub fn validate_filter_operand(op: BaseFilterOp, value: &JsonValue) -> Result<(), BaseEvalError> {
    let expected = match op {
        BaseFilterOp::In | BaseFilterOp::NotIn if !value.is_array() => Some("array filter value"),
        BaseFilterOp::Exists | BaseFilterOp::IsEmpty if !value.is_boolean() => {
            Some("boolean filter value")
        }
        BaseFilterOp::Gt | BaseFilterOp::Gte | BaseFilterOp::Lt | BaseFilterOp::Lte
            if !value.is_null() && json_scalar_to_string(value).is_none() =>
        {
            Some("scalar or null filter value")
        }
        BaseFilterOp::StartsWith
        | BaseFilterOp::NotStartsWith
        | BaseFilterOp::EndsWith
        | BaseFilterOp::Contains
            if json_scalar_to_string(value).is_none() =>
        {
            Some("non-null scalar filter value")
        }
        _ => None,
    };
    expected.map_or(Ok(()), |expected| {
        Err(BaseEvalError::TypeMismatch { op, expected })
    })
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
        JsonValue::Array(values) => values
            .iter()
            .any(|value| json_values_equal(value, row_value)),
        _ => false,
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

#[cfg(test)]
mod semantic_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn real_ingestion_number_representations_match_equality_and_membership() {
        assert!(evaluate_filter(Some(&json!(1.0)), BaseFilterOp::Eq, &json!(1)).unwrap());
        assert!(evaluate_filter(Some(&json!(1.0)), BaseFilterOp::In, &json!([1])).unwrap());
        assert!(!evaluate_filter(Some(&json!(1.0)), BaseFilterOp::NotEq, &json!(1)).unwrap());
    }

    #[test]
    fn empty_missing_null_and_invalid_comparisons_are_explicit() {
        for value in [
            None,
            Some(json!(null)),
            Some(json!("")),
            Some(json!([])),
            Some(json!({})),
        ] {
            assert!(evaluate_filter(value.as_ref(), BaseFilterOp::IsEmpty, &json!(true)).unwrap());
        }
        for value in [json!(false), json!(0), json!(" "), json!([null])] {
            assert!(!evaluate_filter(Some(&value), BaseFilterOp::IsEmpty, &json!(true)).unwrap());
        }
        assert!(!evaluate_filter(Some(&json!(null)), BaseFilterOp::Lt, &json!(1)).unwrap());
        assert!(!evaluate_filter(None, BaseFilterOp::Lt, &json!(1)).unwrap());
        assert!(!evaluate_filter(Some(&json!(1)), BaseFilterOp::Lt, &json!(null)).unwrap());
        assert!(evaluate_filter(Some(&json!("")), BaseFilterOp::Gt, &json!(1)).is_err());
        assert!(evaluate_filter(None, BaseFilterOp::In, &json!(1)).is_err());
        assert!(evaluate_filter(None, BaseFilterOp::Exists, &json!("yes")).is_err());
    }
}
