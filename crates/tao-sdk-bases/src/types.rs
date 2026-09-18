use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Typed field kind used by strict coercion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaseFieldType {
    String,
    Number,
    Bool,
    Date,
    Json,
}

/// Coercion mode for base value materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BaseCoercionMode {
    /// Return errors for invalid coercions.
    Strict,
    /// Keep original values when coercion fails.
    #[default]
    Permissive,
}

/// Stable coercion error payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseCoercionError {
    pub code: String,
    pub field_type: BaseFieldType,
    pub value: Box<JsonValue>,
    pub message: String,
}

impl std::fmt::Display for BaseCoercionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}

impl std::error::Error for BaseCoercionError {}

/// Coerce one JSON value into the requested base field type.
pub fn coerce_json_value(
    value: &JsonValue,
    field_type: BaseFieldType,
    mode: BaseCoercionMode,
) -> Result<JsonValue, BaseCoercionError> {
    if value.is_null() || matches!(mode, BaseCoercionMode::Permissive) {
        return Ok(value.clone());
    }

    let coerced = match field_type {
        BaseFieldType::Json => Ok(value.clone()),
        BaseFieldType::String => coerce_string(value).map(JsonValue::String),
        BaseFieldType::Number => coerce_number(value),
        BaseFieldType::Bool => coerce_bool(value).map(JsonValue::Bool),
        BaseFieldType::Date => coerce_date(value).map(JsonValue::String),
    };

    match coerced {
        Ok(value) => Ok(value),
        Err(message) => Err(BaseCoercionError {
            code: "bases.coercion.invalid_value".to_string(),
            field_type,
            value: Box::new(value.clone()),
            message,
        }),
    }
}

fn coerce_string(value: &JsonValue) -> Result<String, String> {
    match value {
        JsonValue::String(value) => Ok(value.clone()),
        JsonValue::Number(value) => Ok(value.to_string()),
        JsonValue::Bool(value) => Ok(value.to_string()),
        JsonValue::Null => Ok(String::new()),
        _ => Err("expected scalar value coercible to string".to_string()),
    }
}

fn coerce_number(value: &JsonValue) -> Result<JsonValue, String> {
    match value {
        JsonValue::Number(number) => Ok(JsonValue::Number(number.clone())),
        JsonValue::String(value) => {
            let normalized = value.trim();
            if normalized.is_empty() {
                return Err("empty string is not coercible to number".to_string());
            }
            if let Ok(integer) = normalized.parse::<i64>() {
                return Ok(JsonValue::from(integer));
            }
            if let Ok(integer) = normalized.parse::<u64>() {
                return Ok(JsonValue::from(integer));
            }
            let parsed = normalized
                .parse::<f64>()
                .map_err(|_| "string is not coercible to number".to_string())?;
            serde_json::Number::from_f64(parsed)
                .map(JsonValue::Number)
                .ok_or_else(|| "number is not finite".to_string())
        }
        JsonValue::Bool(value) => Ok(JsonValue::Number(serde_json::Number::from(*value as i64))),
        _ => Err("value is not coercible to number".to_string()),
    }
}

fn coerce_bool(value: &JsonValue) -> Result<bool, String> {
    match value {
        JsonValue::Bool(value) => Ok(*value),
        JsonValue::Number(number) => {
            if number.as_i64() == Some(0) {
                Ok(false)
            } else if number.as_i64() == Some(1) {
                Ok(true)
            } else {
                Err("number is not coercible to bool (expected 0 or 1)".to_string())
            }
        }
        JsonValue::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" => Ok(true),
            "false" | "no" | "0" => Ok(false),
            _ => Err("string is not coercible to bool".to_string()),
        },
        _ => Err("value is not coercible to bool".to_string()),
    }
}

fn coerce_date(value: &JsonValue) -> Result<String, String> {
    let JsonValue::String(value) = value else {
        return Err("value is not coercible to date string".to_string());
    };
    let normalized = value.trim();
    if is_iso_date(normalized) {
        return Ok(normalized.to_string());
    }
    Err("string is not coercible to ISO date (YYYY-MM-DD)".to_string())
}

fn is_iso_date(value: &str) -> bool {
    if value.len() != 10 {
        return false;
    }
    let bytes = value.as_bytes();
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes[..4]
            .iter()
            .chain(&bytes[5..7])
            .chain(&bytes[8..10])
            .all(u8::is_ascii_digit)
    {
        return false;
    }
    let year = value[..4].parse::<u32>().unwrap_or_default();
    let month = value[5..7].parse::<u32>().unwrap_or_default();
    let day = value[8..10].parse::<u32>().unwrap_or_default();
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        _ => 0,
    };
    day > 0 && day <= days
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{BaseCoercionMode, BaseFieldType, coerce_json_value};

    #[test]
    fn strict_coercion_reports_invalid_values() {
        let error = coerce_json_value(
            &json!("NaN"),
            BaseFieldType::Number,
            BaseCoercionMode::Strict,
        )
        .expect_err("strict number coercion should fail");
        assert_eq!(error.code, "bases.coercion.invalid_value");
    }

    #[test]
    fn permissive_coercion_keeps_original_value() {
        let value = coerce_json_value(
            &json!([1, 2]),
            BaseFieldType::Number,
            BaseCoercionMode::Permissive,
        )
        .expect("permissive coercion should not fail");
        assert_eq!(value, json!([1, 2]));
    }

    #[test]
    fn coercion_supports_number_bool_date_edges() {
        assert_eq!(
            coerce_json_value(
                &json!("42"),
                BaseFieldType::Number,
                BaseCoercionMode::Strict
            )
            .expect("number"),
            json!(42)
        );
        assert_eq!(
            coerce_json_value(
                &json!("true"),
                BaseFieldType::Bool,
                BaseCoercionMode::Strict
            )
            .expect("bool"),
            json!(true)
        );
        assert_eq!(
            coerce_json_value(
                &json!("2026-03-05"),
                BaseFieldType::Date,
                BaseCoercionMode::Strict
            )
            .expect("date"),
            json!("2026-03-05")
        );
    }
    #[test]
    fn strict_dates_and_large_integer_strings_are_validated_without_rounding() {
        for date in ["2026-99-99", "2026-02-29", "1900-02-29", "2026-04-31"] {
            assert!(
                coerce_json_value(&json!(date), BaseFieldType::Date, BaseCoercionMode::Strict)
                    .is_err()
            );
        }
        assert!(
            coerce_json_value(
                &json!("2000-02-29"),
                BaseFieldType::Date,
                BaseCoercionMode::Strict
            )
            .is_ok()
        );
        assert_eq!(
            coerce_json_value(
                &json!("9007199254740993"),
                BaseFieldType::Number,
                BaseCoercionMode::Strict
            )
            .unwrap(),
            json!(9_007_199_254_740_993_u64)
        );
        assert_eq!(
            coerce_json_value(
                &json!(null),
                BaseFieldType::String,
                BaseCoercionMode::Strict
            )
            .unwrap(),
            json!(null)
        );
    }
}
