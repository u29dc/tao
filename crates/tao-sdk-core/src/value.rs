//! Shared JSON value equality and ordering for structured queries.

use serde_json::Value as JsonValue;
use std::cmp::Ordering;

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
        (JsonValue::Number(left), JsonValue::Number(right)) => compare_numbers(left, right),
        (JsonValue::String(left), JsonValue::String(right)) => left.cmp(right),
        (JsonValue::Array(left), JsonValue::Array(right)) => left
            .iter()
            .zip(right)
            .map(|(a, b)| compare_json_values(a, b))
            .find(|order| !order.is_eq())
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        (JsonValue::Object(left), JsonValue::Object(right)) => left
            .iter()
            .zip(right)
            .map(|((ak, av), (bk, bv))| ak.cmp(bk).then_with(|| compare_json_values(av, bv)))
            .find(|order| !order.is_eq())
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        _ => left.to_string().cmp(&right.to_string()),
    }
}

/// Semantic equality, including equivalent integer and floating point representations.
pub fn json_values_equal(left: &JsonValue, right: &JsonValue) -> bool {
    compare_json_values(left, right).is_eq()
}

/// Ordered predicates compare only non-null scalar values of the same type.
pub fn compare_predicate_values(left: &JsonValue, right: &JsonValue) -> Option<Ordering> {
    match (left, right) {
        (JsonValue::Number(_), JsonValue::Number(_))
        | (JsonValue::String(_), JsonValue::String(_))
        | (JsonValue::Bool(_), JsonValue::Bool(_)) => Some(compare_json_values(left, right)),
        _ => None,
    }
}

fn integer(number: &serde_json::Number) -> Option<i128> {
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

fn compare_numbers(left: &serde_json::Number, right: &serde_json::Number) -> Ordering {
    match (integer(left), integer(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(left), None) => compare_integer_float(left, right.as_f64().unwrap_or_default()),
        (None, Some(right)) => {
            compare_integer_float(right, left.as_f64().unwrap_or_default()).reverse()
        }
        (None, None) => left
            .as_f64()
            .partial_cmp(&right.as_f64())
            .unwrap_or(Ordering::Equal),
    }
}

fn compare_integer_float(integer: i128, float: f64) -> Ordering {
    // JSON integers fit in i64/u64, well inside i128. A saturating float cast
    // handles larger finite floats; comparison with the truncated integer avoids
    // rounding integer values above 2^53 through f64.
    integer.cmp(&(float as i128)).then_with(|| {
        if float.fract() > 0.0 {
            Ordering::Less
        } else if float.fract() < 0.0 {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    })
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
    use super::*;
    use serde_json::json;

    #[test]
    fn numeric_equality_preserves_large_integer_distinctions() {
        assert!(json_values_equal(&json!(1), &json!(1.0)));
        assert!(json_values_equal(
            &json!([1, {"n": 2}]),
            &json!([1.0, {"n": 2.0}])
        ));
        assert!(!json_values_equal(
            &json!(9_007_199_254_740_993_u64),
            &json!(9_007_199_254_740_992.0)
        ));
        assert!(
            compare_json_values(&json!(u64::MAX), &json!(18_446_744_073_709_551_616.0)).is_lt()
        );
        assert!(compare_json_values(&json!(-1), &json!(-1.5)).is_gt());
        assert!(compare_json_values(&json!(0), &json!(0.5)).is_lt());
        assert!(compare_json_values(&json!(-1), &json!(0_u64)).is_lt());
    }

    #[test]
    fn structured_order_is_lexical_and_predicates_do_not_rank_types() {
        assert!(compare_json_values(&json!([1, 2]), &json!([1, 3])).is_lt());
        assert!(!json_values_equal(&json!({"a": 1}), &json!({"b": 1})));
        assert_eq!(compare_predicate_values(&json!(null), &json!(1)), None);
        assert_eq!(compare_predicate_values(&json!("a"), &json!(1)), None);
    }
}
