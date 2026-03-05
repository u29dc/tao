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
    pub value: JsonValue,
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
    if matches!(mode, BaseCoercionMode::Permissive) {
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
            value: value.clone(),
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
    bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[..4].iter().all(|byte| byte.is_ascii_digit())
        && bytes[5..7].iter().all(|byte| byte.is_ascii_digit())
        && bytes[8..10].iter().all(|byte| byte.is_ascii_digit())
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
            json!(42.0)
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
}
