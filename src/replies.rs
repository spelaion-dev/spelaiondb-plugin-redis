//! Command replies as rows and columns (SpelaionDB's ADR-056, point 3).
//!
//! - A single value (a string, a number, `OK`, nil) is one row with a `value` column.
//! - A list or a set is one row per element, in a `value` column.
//! - A map (a hash read with `HGETALL`, `CONFIG GET`, …) is `field`, `value` rows.
//! - A list whose every element is a pair is two columns: `member`, `score` for a sorted set
//!   read `WITHSCORES`, `id`, `fields` for a stream's entries, `field`, `value` otherwise.
//! - The `SCAN` family's reply is its elements, as above, and its next cursor as a notice.
//!
//! A column's type comes from all its cells: integers stay integers, numbers with a fraction
//! are floats, a value that is not UTF-8 text makes the column bytes, and a nested list or map
//! is JSON. Text is what `redis-cli` prints.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder,
};
use redis::Value;
use serde_json::{Map, Value as Json};
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

/// One cell, before its column's type is known.
#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Nil,
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
    Json(Json),
}

impl Cell {
    fn of(value: &Value) -> Cell {
        match value {
            Value::Nil => Cell::Nil,
            Value::Int(n) => Cell::Int(*n),
            Value::Double(n) => Cell::Float(*n),
            Value::Boolean(b) => Cell::Bool(*b),
            Value::Okay => Cell::Text("OK".into()),
            Value::SimpleString(text) => Cell::Text(text.clone()),
            Value::VerbatimString { text, .. } => Cell::Text(text.clone()),
            Value::BulkString(bytes) => match String::from_utf8(bytes.clone()) {
                Ok(text) => Cell::Text(text),
                Err(_) => Cell::Bytes(bytes.clone()),
            },
            Value::ServerError(error) => Cell::Text(format!("(error) {error}")),
            other => Cell::Json(json(other)),
        }
    }

    fn native(&self) -> &'static str {
        match self {
            Cell::Nil => "nil",
            Cell::Int(_) => "integer",
            Cell::Float(_) => "double",
            Cell::Bool(_) => "boolean",
            Cell::Text(_) => "string",
            Cell::Bytes(_) => "string",
            Cell::Json(_) => "array",
        }
    }

    /// The text `redis-cli` would print for it.
    fn text(&self) -> Option<String> {
        match self {
            Cell::Nil => None,
            Cell::Int(n) => Some(n.to_string()),
            Cell::Float(n) => Some(n.to_string()),
            Cell::Bool(b) => Some(if *b { "true" } else { "false" }.into()),
            Cell::Text(text) => Some(text.clone()),
            Cell::Bytes(bytes) => Some(escaped(bytes)),
            Cell::Json(value) => Some(value.to_string()),
        }
    }

    fn json(&self) -> Option<Json> {
        match self {
            Cell::Nil => None,
            Cell::Int(n) => Some(Json::from(*n)),
            Cell::Float(n) => Some(Json::from(*n)),
            Cell::Bool(b) => Some(Json::Bool(*b)),
            Cell::Text(text) => Some(Json::String(text.clone())),
            Cell::Bytes(bytes) => Some(Json::String(escaped(bytes))),
            Cell::Json(value) => Some(value.clone()),
        }
    }
}

/// Bytes as `redis-cli` shows them: printable ASCII as it is, the rest as `\xHH`.
pub fn escaped(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("\\x{b:02x}")),
        }
    }
    out
}

/// A nested value as JSON: lists as arrays, maps with text keys as objects (others as arrays
/// of pairs), bytes that are not UTF-8 as `redis-cli`'s escaped text.
pub fn json(value: &Value) -> Json {
    match value {
        Value::Array(items) | Value::Set(items) => Json::Array(items.iter().map(json).collect()),
        Value::Push { data, .. } => Json::Array(data.iter().map(json).collect()),
        Value::Map(pairs) => {
            let keys: Option<Vec<String>> = pairs
                .iter()
                .map(|(k, _)| match Cell::of(k) {
                    Cell::Text(text) => Some(text),
                    _ => None,
                })
                .collect();
            match keys {
                Some(keys) => {
                    let mut object = Map::new();
                    for (key, (_, v)) in keys.into_iter().zip(pairs) {
                        object.insert(key, json(v));
                    }
                    Json::Object(object)
                }
                None => Json::Array(
                    pairs
                        .iter()
                        .map(|(k, v)| Json::Array(vec![json(k), json(v)]))
                        .collect(),
                ),
            }
        }
        Value::Attribute { data, .. } => json(data),
        Value::BigNumber(n) => Json::String(format!("{n:?}")),
        other => Cell::of(other).json().unwrap_or(Json::Null),
    }
}

/// A reply laid out: its columns' names, its rows, and a notice for the user, if any.
#[derive(Debug)]
pub struct Table {
    names: Vec<&'static str>,
    rows: Vec<Vec<Cell>>,
    pub notice: Option<String>,
}

const SCANS: &[&str] = &["SCAN", "SSCAN", "HSCAN", "ZSCAN"];

impl Table {
    /// How `command`'s `reply` reads as rows.
    pub fn of(command: &str, reply: Value) -> Table {
        let command = command.to_ascii_uppercase();
        // The `SCAN` family: `[cursor, [elements]]`.
        if SCANS.contains(&command.as_str())
            && let Value::Array(parts) = &reply
            && let [cursor, Value::Array(elements)] = parts.as_slice()
        {
            let cursor = Cell::of(cursor).text().unwrap_or_default();
            let mut table = match command.as_str() {
                "HSCAN" => Table::pairs(("field", "value"), flat_pairs(elements)),
                "ZSCAN" => Table::pairs(("member", "score"), flat_pairs(elements)),
                _ => Table::list(elements),
            };
            if cursor != "0" {
                table.notice = Some(format!(
                    "More remain: run it again with the cursor {cursor} to read the next ones."
                ));
            }
            return table;
        }
        match reply {
            Value::Map(pairs) => Table::pairs(
                ("field", "value"),
                pairs.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            ),
            Value::Array(items) | Value::Set(items) => {
                let all_pairs = !items.is_empty()
                    && items
                        .iter()
                        .all(|i| matches!(i, Value::Array(p) if p.len() == 2));
                if all_pairs {
                    let names = if command.starts_with('Z') {
                        ("member", "score")
                    } else if command.starts_with('X') {
                        ("id", "fields")
                    } else {
                        ("field", "value")
                    };
                    let pairs = items
                        .into_iter()
                        .map(|i| match i {
                            Value::Array(mut p) => {
                                let second = p.pop().unwrap_or(Value::Nil);
                                let first = p.pop().unwrap_or(Value::Nil);
                                (first, second)
                            }
                            other => (other, Value::Nil),
                        })
                        .collect();
                    Table::pairs(names, pairs)
                } else {
                    Table::list(&items)
                }
            }
            Value::Attribute { data, .. } => Table::of(&command, *data),
            scalar => Table {
                names: vec!["value"],
                rows: vec![vec![Cell::of(&scalar)]],
                notice: None,
            },
        }
    }

    fn list(items: &[Value]) -> Table {
        Table {
            names: vec!["value"],
            rows: items.iter().map(|i| vec![Cell::of(i)]).collect(),
            notice: None,
        }
    }

    fn pairs(names: (&'static str, &'static str), pairs: Vec<(Value, Value)>) -> Table {
        Table {
            names: vec![names.0, names.1],
            rows: pairs
                .iter()
                .map(|(a, b)| vec![Cell::of(a), Cell::of(b)])
                .collect(),
            notice: None,
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The columns, typed from all their cells.
    pub fn schema(&self) -> Arc<ResultSchema> {
        let metas = self
            .names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let kind = self.kind(i);
                let native = self
                    .rows
                    .iter()
                    .map(|r| &r[i])
                    .find(|c| **c != Cell::Nil)
                    .map_or("nil", Cell::native);
                ColumnMeta {
                    name: (*name).into(),
                    logical: kind.logical(),
                    native_type: native.into(),
                    native_id: None,
                    origin: None,
                }
            })
            .collect();
        Arc::new(ResultSchema::new(metas))
    }

    fn kind(&self, column: usize) -> Kind {
        let mut kind: Option<Kind> = None;
        for row in &self.rows {
            let cell = match &row[column] {
                Cell::Nil => continue,
                Cell::Int(_) => Kind::Int,
                Cell::Float(_) => Kind::Float,
                Cell::Bool(_) => Kind::Bool,
                Cell::Text(_) => Kind::Text,
                Cell::Bytes(_) => Kind::Bytes,
                Cell::Json(_) => Kind::Json,
            };
            kind = Some(match (kind, cell) {
                (None, k) => k,
                (Some(a), b) if a == b => a,
                (Some(Kind::Int), Kind::Float) | (Some(Kind::Float), Kind::Int) => Kind::Float,
                // Bytes keep every value whole; JSON holds any other mix.
                (Some(Kind::Bytes), _) | (_, Kind::Bytes) => Kind::Bytes,
                (Some(Kind::Json), _) | (_, Kind::Json) => Kind::Json,
                _ => Kind::Text,
            });
        }
        kind.unwrap_or(Kind::Text)
    }

    /// Rows `from..to` as one batch of [`Table::schema`].
    pub fn batch(&self, schema: &ResultSchema, from: usize, to: usize) -> Result<RecordBatch> {
        let rows = &self.rows[from..to];
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.names.len());
        for column in 0..self.names.len() {
            let cells = rows.iter().map(|r| &r[column]);
            let array: ArrayRef = match self.kind(column) {
                Kind::Int => {
                    let mut b = Int64Builder::with_capacity(rows.len());
                    for cell in cells {
                        match cell {
                            Cell::Int(n) => b.append_value(*n),
                            _ => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
                Kind::Float => {
                    let mut b = Float64Builder::with_capacity(rows.len());
                    for cell in cells {
                        match cell {
                            Cell::Int(n) => b.append_value(*n as f64),
                            Cell::Float(n) => b.append_value(*n),
                            _ => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
                Kind::Bool => {
                    let mut b = BooleanBuilder::with_capacity(rows.len());
                    for cell in cells {
                        match cell {
                            Cell::Bool(v) => b.append_value(*v),
                            _ => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
                Kind::Bytes => {
                    let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                    for cell in cells {
                        match cell {
                            Cell::Nil => b.append_null(),
                            Cell::Bytes(bytes) => b.append_value(bytes),
                            other => b.append_value(other.text().unwrap_or_default()),
                        }
                    }
                    Arc::new(b.finish())
                }
                Kind::Json => {
                    let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                    for cell in cells {
                        match cell.json() {
                            None => b.append_null(),
                            Some(value) => b.append_value(value.to_string()),
                        }
                    }
                    Arc::new(b.finish())
                }
                Kind::Text => {
                    let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                    for cell in cells {
                        match cell.text() {
                            None => b.append_null(),
                            Some(text) => b.append_value(text),
                        }
                    }
                    Arc::new(b.finish())
                }
            };
            arrays.push(array);
        }
        RecordBatch::try_new(schema.arrow.clone(), arrays)
            .map_err(|e| DriverError::Other(format!("building a batch of a reply: {e}")))
    }
}

/// `[a, b, c, d]` as `[(a, b), (c, d)]`: how RESP2 writes pairs.
fn flat_pairs(items: &[Value]) -> Vec<(Value, Value)> {
    items
        .chunks(2)
        .map(|c| (c[0].clone(), c.get(1).cloned().unwrap_or(Value::Nil)))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Int,
    Float,
    Bool,
    Text,
    Bytes,
    Json,
}

impl Kind {
    fn logical(self) -> LogicalType {
        match self {
            Kind::Int => LogicalType::Int,
            Kind::Float => LogicalType::Float,
            Kind::Bool => LogicalType::Bool,
            Kind::Text => LogicalType::Text,
            Kind::Bytes => LogicalType::Bytes,
            Kind::Json => LogicalType::Json,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray, Int64Array, StringArray};

    fn bulk(text: &str) -> Value {
        Value::BulkString(text.as_bytes().to_vec())
    }

    fn texts(table: &Table, column: usize) -> Vec<Option<String>> {
        let schema = table.schema();
        let batch = table.batch(&schema, 0, table.len()).unwrap();
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..array.len())
            .map(|i| array.is_valid(i).then(|| array.value(i).to_string()))
            .collect()
    }

    #[test]
    fn a_single_value_is_one_row() {
        let table = Table::of("GET", bulk("ñandú"));
        assert_eq!(texts(&table, 0), [Some("ñandú".into())]);
        assert_eq!(table.schema().columns[0].name, "value");
        let table = Table::of("GET", Value::Nil);
        assert_eq!(texts(&table, 0), [None]);
        let table = Table::of("SET", Value::Okay);
        assert_eq!(texts(&table, 0), [Some("OK".into())]);
        let table = Table::of("INCR", Value::Int(42));
        let schema = table.schema();
        assert_eq!(schema.columns[0].logical, LogicalType::Int);
        let batch = table.batch(&schema, 0, 1).unwrap();
        let ints = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.value(0), 42);
    }

    #[test]
    fn a_list_is_one_row_per_element_and_a_map_is_fields_and_values() {
        let table = Table::of("LRANGE", Value::Array(vec![bulk("a"), bulk("b")]));
        assert_eq!(texts(&table, 0), [Some("a".into()), Some("b".into())]);
        let table = Table::of(
            "HGETALL",
            Value::Map(vec![(bulk("name"), bulk("Ana")), (bulk("age"), bulk("7"))]),
        );
        assert_eq!(
            table
                .schema()
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["field", "value"]
        );
        assert_eq!(texts(&table, 0), [Some("name".into()), Some("age".into())]);
        assert_eq!(texts(&table, 1), [Some("Ana".into()), Some("7".into())]);
    }

    #[test]
    fn pairs_get_their_names() {
        let z = Table::of(
            "ZRANGE",
            Value::Array(vec![
                Value::Array(vec![bulk("a"), Value::Double(1.5)]),
                Value::Array(vec![bulk("b"), Value::Double(2.0)]),
            ]),
        );
        let schema = z.schema();
        assert_eq!(schema.columns[0].name, "member");
        assert_eq!(schema.columns[1].name, "score");
        assert_eq!(schema.columns[1].logical, LogicalType::Float);
        let x = Table::of(
            "XRANGE",
            Value::Array(vec![Value::Array(vec![
                bulk("1-0"),
                Value::Array(vec![bulk("f"), bulk("v")]),
            ])]),
        );
        assert_eq!(x.schema().columns[0].name, "id");
        assert_eq!(x.schema().columns[1].logical, LogicalType::Json);
        assert_eq!(texts(&x, 1), [Some(r#"["f","v"]"#.into())]);
    }

    #[test]
    fn a_scan_is_its_elements_and_its_cursor_a_notice() {
        let reply = Value::Array(vec![
            bulk("17"),
            Value::Array(vec![bulk("f1"), bulk("v1"), bulk("f2"), bulk("v2")]),
        ]);
        let table = Table::of("hscan", reply);
        assert_eq!(texts(&table, 0), [Some("f1".into()), Some("f2".into())]);
        assert_eq!(texts(&table, 1), [Some("v1".into()), Some("v2".into())]);
        assert!(table.notice.unwrap().contains("cursor 17"));
        let done = Table::of(
            "SCAN",
            Value::Array(vec![bulk("0"), Value::Array(vec![bulk("k")])]),
        );
        assert!(done.notice.is_none());
        assert_eq!(texts(&done, 0), [Some("k".into())]);
    }

    #[test]
    fn bytes_that_are_not_text_make_a_bytes_column() {
        let table = Table::of(
            "LRANGE",
            Value::Array(vec![Value::BulkString(vec![0xff, 0x00]), bulk("ok")]),
        );
        let schema = table.schema();
        assert_eq!(schema.columns[0].logical, LogicalType::Bytes);
        let batch = table.batch(&schema, 0, 2).unwrap();
        let bytes = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(bytes.value(0), [0xff, 0x00]);
        assert_eq!(bytes.value(1), b"ok");
        assert_eq!(escaped(&[0xff, b'a', b'\n']), r"\xffa\n");
    }

    #[test]
    fn a_mixed_column_is_text_and_numbers_stay_whole() {
        let table = Table::of(
            "MGET",
            Value::Array(vec![Value::Int(1), bulk("x"), Value::Nil]),
        );
        assert_eq!(table.schema().columns[0].logical, LogicalType::Text);
        assert_eq!(texts(&table, 0), [Some("1".into()), Some("x".into()), None]);
        let table = Table::of("X", Value::Array(vec![Value::Int(i64::MAX)]));
        let schema = table.schema();
        let batch = table.batch(&schema, 0, 1).unwrap();
        let ints = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.value(0), i64::MAX);
    }
}
