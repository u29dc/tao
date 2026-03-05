use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
}

impl LiteralValue {
    #[must_use]
    pub fn to_json_value(&self) -> JsonValue {
        match self {
            Self::Null => JsonValue::Null,
            Self::Bool(value) => JsonValue::Bool(*value),
            Self::Number(value) => {
                serde_json::Number::from_f64(*value).map_or(JsonValue::Null, JsonValue::Number)
            }
            Self::String(value) => JsonValue::String(value.clone()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    StartsWith,
    EndsWith,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhereExpr {
    Compare {
        field: String,
        op: CompareOp,
        value: LiteralValue,
    },
    Not(Box<WhereExpr>),
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrder {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub field: String,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} at byte {}", self.message, self.position)
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    Identifier,
    String,
    Number,
    Bool,
    Null,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    Not,
    Contains,
    StartsWith,
    EndsWith,
    LParen,
    RParen,
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
struct Token {
    kind: TokenKind,
    raw: String,
    position: usize,
}

pub fn parse_where_expression(input: &str) -> Result<WhereExpr, ParseError> {
    let mut parser = WhereParser::new(input)?;
    let expression = parser.parse_expression()?;
    parser.expect(TokenKind::Eof)?;
    Ok(expression)
}

pub fn parse_where_expression_opt(input: Option<&str>) -> Result<Option<WhereExpr>, ParseError> {
    let Some(raw) = input else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    parse_where_expression(trimmed).map(Some)
}

pub fn parse_sort_keys(input: Option<&str>) -> Result<Vec<SortKey>, ParseError> {
    let Some(raw) = input else {
        return Ok(Vec::new());
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut keys = Vec::new();
    for (segment_index, segment) in trimmed.split(',').enumerate() {
        let segment = segment.trim();
        if segment.is_empty() {
            return Err(ParseError {
                message: "sort clause contains empty segment".to_string(),
                position: segment_index,
            });
        }

        let mut field = None::<String>;
        let mut direction = SortDirection::Asc;
        let mut null_order = NullOrder::First;

        for part in segment
            .split(':')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            match part.to_ascii_lowercase().as_str() {
                "asc" => direction = SortDirection::Asc,
                "desc" => direction = SortDirection::Desc,
                "nulls_first" | "nullsfirst" => null_order = NullOrder::First,
                "nulls_last" | "nullslast" => null_order = NullOrder::Last,
                _ => {
                    if field.is_some() {
                        return Err(ParseError {
                            message: format!(
                                "invalid sort token '{}'; expected direction/null ordering",
                                part
                            ),
                            position: raw.find(segment).unwrap_or(0),
                        });
                    }
                    field = Some(part.to_string());
                }
            }
        }

        let Some(field) = field else {
            return Err(ParseError {
                message: "sort clause is missing field name".to_string(),
                position: raw.find(segment).unwrap_or(0),
            });
        };

        keys.push(SortKey {
            field,
            direction,
            null_order,
        });
    }

    Ok(keys)
}

pub fn build_fts_query(query: &str) -> String {
    let tokens = query
        .split_whitespace()
        .filter_map(|token| {
            let sanitized = token
                .chars()
                .filter(|ch| ch.is_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/'))
                .collect::<String>()
                .to_lowercase();
            if sanitized.is_empty() {
                None
            } else {
                Some(sanitized)
            }
        })
        .collect::<Vec<_>>();

    if tokens.is_empty() {
        return String::from("\"\"");
    }

    tokens
        .into_iter()
        .map(|token| format!("\"{token}\"*"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

struct WhereParser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl WhereParser {
    fn new(input: &str) -> Result<Self, ParseError> {
        Ok(Self {
            tokens: tokenize_where(input)?,
            cursor: 0,
        })
    }

    fn parse_expression(&mut self) -> Result<WhereExpr, ParseError> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> Result<WhereExpr, ParseError> {
        let mut left = self.parse_and_expression()?;
        while self.matches(TokenKind::Or) {
            let right = self.parse_and_expression()?;
            left = WhereExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and_expression(&mut self) -> Result<WhereExpr, ParseError> {
        let mut left = self.parse_unary_expression()?;
        while self.matches(TokenKind::And) {
            let right = self.parse_unary_expression()?;
            left = WhereExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_unary_expression(&mut self) -> Result<WhereExpr, ParseError> {
        if self.matches(TokenKind::Not) {
            return Ok(WhereExpr::Not(Box::new(self.parse_unary_expression()?)));
        }
        self.parse_primary_expression()
    }

    fn parse_primary_expression(&mut self) -> Result<WhereExpr, ParseError> {
        if self.matches(TokenKind::LParen) {
            let expression = self.parse_expression()?;
            self.expect(TokenKind::RParen)?;
            return Ok(expression);
        }

        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<WhereExpr, ParseError> {
        let field_token = self.expect(TokenKind::Identifier)?;
        let op_token = self.advance();
        let op = match op_token.kind {
            TokenKind::Eq => CompareOp::Eq,
            TokenKind::Neq => CompareOp::Neq,
            TokenKind::Gt => CompareOp::Gt,
            TokenKind::Gte => CompareOp::Gte,
            TokenKind::Lt => CompareOp::Lt,
            TokenKind::Lte => CompareOp::Lte,
            TokenKind::Contains => CompareOp::Contains,
            TokenKind::StartsWith => CompareOp::StartsWith,
            TokenKind::EndsWith => CompareOp::EndsWith,
            _ => {
                return Err(ParseError {
                    message: "expected comparison operator".to_string(),
                    position: op_token.position,
                });
            }
        };

        let value_token = self.advance();
        let value = parse_literal(&value_token)?;

        Ok(WhereExpr::Compare {
            field: field_token.raw,
            op,
            value,
        })
    }

    fn matches(&mut self, kind: TokenKind) -> bool {
        if self.current().kind == kind {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, kind: TokenKind) -> Result<Token, ParseError> {
        let token = self.advance();
        if token.kind == kind {
            Ok(token)
        } else {
            Err(ParseError {
                message: format!("expected {:?}", kind),
                position: token.position,
            })
        }
    }

    fn current(&self) -> &Token {
        self.tokens
            .get(self.cursor)
            .unwrap_or_else(|| self.tokens.last().expect("token stream not empty"))
    }

    fn advance(&mut self) -> Token {
        let token = self.current().clone();
        if self.cursor < self.tokens.len() {
            self.cursor += 1;
        }
        token
    }
}

fn parse_literal(token: &Token) -> Result<LiteralValue, ParseError> {
    match token.kind {
        TokenKind::String => Ok(LiteralValue::String(token.raw.clone())),
        TokenKind::Number => token
            .raw
            .parse::<f64>()
            .map(LiteralValue::Number)
            .map_err(|_| ParseError {
                message: format!("invalid numeric literal '{}'", token.raw),
                position: token.position,
            }),
        TokenKind::Bool => Ok(LiteralValue::Bool(token.raw.eq_ignore_ascii_case("true"))),
        TokenKind::Null => Ok(LiteralValue::Null),
        _ => Err(ParseError {
            message: "expected literal value".to_string(),
            position: token.position,
        }),
    }
}

fn tokenize_where(input: &str) -> Result<Vec<Token>, ParseError> {
    let mut tokens = Vec::new();
    let chars = input.chars().collect::<Vec<_>>();
    let mut index = 0usize;

    while index < chars.len() {
        let ch = chars[index];
        if ch.is_whitespace() {
            index += 1;
            continue;
        }

        if ch == '(' {
            tokens.push(Token {
                kind: TokenKind::LParen,
                raw: "(".to_string(),
                position: index,
            });
            index += 1;
            continue;
        }

        if ch == ')' {
            tokens.push(Token {
                kind: TokenKind::RParen,
                raw: ")".to_string(),
                position: index,
            });
            index += 1;
            continue;
        }

        if ch == '=' && chars.get(index + 1) == Some(&'=') {
            tokens.push(Token {
                kind: TokenKind::Eq,
                raw: "==".to_string(),
                position: index,
            });
            index += 2;
            continue;
        }

        if ch == '!' && chars.get(index + 1) == Some(&'=') {
            tokens.push(Token {
                kind: TokenKind::Neq,
                raw: "!=".to_string(),
                position: index,
            });
            index += 2;
            continue;
        }

        if ch == '>' {
            if chars.get(index + 1) == Some(&'=') {
                tokens.push(Token {
                    kind: TokenKind::Gte,
                    raw: ">=".to_string(),
                    position: index,
                });
                index += 2;
            } else {
                tokens.push(Token {
                    kind: TokenKind::Gt,
                    raw: ">".to_string(),
                    position: index,
                });
                index += 1;
            }
            continue;
        }

        if ch == '<' {
            if chars.get(index + 1) == Some(&'=') {
                tokens.push(Token {
                    kind: TokenKind::Lte,
                    raw: "<=".to_string(),
                    position: index,
                });
                index += 2;
            } else {
                tokens.push(Token {
                    kind: TokenKind::Lt,
                    raw: "<".to_string(),
                    position: index,
                });
                index += 1;
            }
            continue;
        }

        if ch == '"' || ch == '\'' {
            let quote = ch;
            let start = index;
            index += 1;
            let mut value = String::new();
            while index < chars.len() && chars[index] != quote {
                value.push(chars[index]);
                index += 1;
            }
            if index >= chars.len() {
                return Err(ParseError {
                    message: "unterminated string literal".to_string(),
                    position: start,
                });
            }
            index += 1;
            tokens.push(Token {
                kind: TokenKind::String,
                raw: value,
                position: start,
            });
            continue;
        }

        if ch.is_ascii_digit()
            || (ch == '-'
                && chars
                    .get(index + 1)
                    .is_some_and(|next| next.is_ascii_digit()))
        {
            let start = index;
            let mut raw = String::new();
            raw.push(ch);
            index += 1;
            while index < chars.len() && (chars[index].is_ascii_digit() || chars[index] == '.') {
                raw.push(chars[index]);
                index += 1;
            }
            tokens.push(Token {
                kind: TokenKind::Number,
                raw,
                position: start,
            });
            continue;
        }

        if ch.is_ascii_alphabetic() || matches!(ch, '_' | '.') {
            let start = index;
            let mut raw = String::new();
            raw.push(ch);
            index += 1;
            while index < chars.len()
                && (chars[index].is_ascii_alphanumeric()
                    || matches!(chars[index], '_' | '-' | '.' | ':'))
            {
                raw.push(chars[index]);
                index += 1;
            }
            let lowered = raw.to_ascii_lowercase();
            let kind = match lowered.as_str() {
                "and" => TokenKind::And,
                "or" => TokenKind::Or,
                "not" => TokenKind::Not,
                "contains" => TokenKind::Contains,
                "starts_with" | "startswith" => TokenKind::StartsWith,
                "ends_with" | "endswith" => TokenKind::EndsWith,
                "true" | "false" => TokenKind::Bool,
                "null" => TokenKind::Null,
                _ => TokenKind::Identifier,
            };
            tokens.push(Token {
                kind,
                raw,
                position: start,
            });
            continue;
        }

        return Err(ParseError {
            message: format!("unexpected character '{}'", ch),
            position: index,
        });
    }

    tokens.push(Token {
        kind: TokenKind::Eof,
        raw: String::new(),
        position: input.len(),
    });
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::{
        CompareOp, NullOrder, SortDirection, WhereExpr, parse_sort_keys, parse_where_expression,
        parse_where_expression_opt,
    };

    #[test]
    fn where_parser_respects_boolean_precedence() {
        let parsed = parse_where_expression("status == 'open' or priority >= 2 and score < 10")
            .expect("parse where");
        match parsed {
            WhereExpr::Or(_, right) => {
                assert!(matches!(*right, WhereExpr::And(_, _)));
            }
            _ => panic!("expected OR root"),
        }
    }

    #[test]
    fn where_parser_supports_parentheses_and_unary_not() {
        let parsed =
            parse_where_expression("not (done == true or score < 2)").expect("parse where");
        assert!(matches!(parsed, WhereExpr::Not(_)));
    }

    #[test]
    fn where_parser_reports_position_for_errors() {
        let error = parse_where_expression("status = 'open'").expect_err("should fail");
        assert!(error.position > 0);
        assert!(
            error.message.contains("unexpected character")
                || error.message.contains("comparison operator")
        );
    }

    #[test]
    fn where_parser_parses_operator_keywords() {
        let parsed = parse_where_expression("title ends_with 'log'").expect("parse where");
        assert!(matches!(
            parsed,
            WhereExpr::Compare {
                op: CompareOp::EndsWith,
                ..
            }
        ));
    }

    #[test]
    fn where_optional_parser_handles_empty_input() {
        let parsed = parse_where_expression_opt(Some("   ")).expect("parse optional");
        assert!(parsed.is_none());
    }

    #[test]
    fn sort_parser_supports_multi_key_and_null_policies() {
        let keys =
            parse_sort_keys(Some("priority:desc:nulls_last,title:asc")).expect("parse sort keys");
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].field, "priority");
        assert_eq!(keys[0].direction, SortDirection::Desc);
        assert_eq!(keys[0].null_order, NullOrder::Last);
        assert_eq!(keys[1].field, "title");
        assert_eq!(keys[1].direction, SortDirection::Asc);
    }
}
