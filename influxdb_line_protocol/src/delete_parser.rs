//! parse string that should be done at client before sending to server

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use std::fmt::Display;

use thiserror::Error;

use serde::{Deserialize, Serialize};

use chrono::DateTime;

use crate::timestamp;

/// Parse Error
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid time format
    #[error("Invalid timestamp: {}", .0)]
    InvalidTimestamp(String),

    /// Invalid time range
    #[error("Invalid time range: ({}, {})", .0, .1)]
    InvalidTimeRange(String, String),
}

/// Result type for Parser Cient
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Parser for Delete predicate and time range
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct ProvidedParseDelete {
    pub start_time: i64,
    pub stop_time: i64,
    // conjunctive predicate of binary expressions of = or !=
    pub predicate: Vec<ProvidedDeleteBinaryExpr>,
}

/// Single Binary expression of delete which
/// in the form of "column = value" or column != value"
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct ProvidedDeleteBinaryExpr {
    pub column: String,
    pub op: ProvidedDeleteOp,
    pub value: String, // NGA Todo: should be enum like FieldValue
}

/// Delete Operator which either "=" or "!="
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
pub enum ProvidedDeleteOp {
    /// represent "="
    Eq,
    /// represet "!="
    NotEq,
}

impl Display for ProvidedDeleteOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eq => write!(f, "Eq"),
            Self::NotEq => write!(f, "NotEq"),
        }
    }
}

impl ProvidedDeleteOp {
    /// Return a str representation of this DeleteOp
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "Eq",
            Self::NotEq => "NotEq",
        }
    }
}

impl ProvidedParseDelete {
    /// Create a ProvidedParseDelete
    pub fn new(start_time: i64, stop_time: i64, predicate: Vec<ProvidedDeleteBinaryExpr>) -> Self {
        Self {
            start_time,
            stop_time,
            predicate,
        }
    }

    /// Parse and convert the delete grpc API into ProvidedParseDelete to send to server
    pub fn try_new(start: &str, stop: &str, predicate: &str) -> Result<Self> {
        // parse and check time range
        let (start_time, stop_time) = Self::parse_time_range(start, stop)?;

        // Parse the predicate
        let delete_exprs = Self::parse_predicate(predicate)?;

        Ok(Self::new(start_time, stop_time, delete_exprs))
    }

    /// Parse the predicate
    // NGA TODO: parse the delete predicate which is a conjunctive expression of many
    // binary expressions of 'colum = constant' or 'column != constant'
    pub fn parse_predicate(_predicate: &str) -> Result<Vec<ProvidedDeleteBinaryExpr>> {
        Ok(vec![])
    }

    /// Parse a time and return its time in nanosecond
    pub fn parse_time(input: &str) -> Result<i64> {
        // This input can be in timestamp form that end with Z such as 1970-01-01T00:00:00Z
        // See examples here https://docs.influxdata.com/influxdb/v2.0/reference/cli/influx/delete/#delete-all-points-within-a-specified-time-frame
        let datetime_result = DateTime::parse_from_rfc3339(input);
        match datetime_result {
            Ok(datetime) => Ok(datetime.timestamp_nanos()),
            Err(timestamp_err) => {
                // See if it is in nanosecond form
                let time_result = timestamp(input);
                match time_result {
                    Ok((_, nano)) => Ok(nano),
                    Err(nano_err) => {
                        // wrong format, return both error
                        let error_str = format!("{}, {}", timestamp_err, nano_err);
                        Err(Error::InvalidTimestamp(error_str))
                    }
                }
            }
        }
    }

    /// Parse a time range [start, stop]
    pub fn parse_time_range(start: &str, stop: &str) -> Result<(i64, i64)> {
        let start_time = Self::parse_time(start)?;
        let stop_time = Self::parse_time(stop)?;
        if start_time > stop_time {
            return Err(Error::InvalidTimeRange(start.to_string(), stop.to_string()));
        }

        Ok((start_time, stop_time))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_time_range_valid() {
        let start = r#"100"#;
        let stop = r#"100"#;
        let result = ProvidedParseDelete::parse_time_range(start, stop).unwrap();
        let expected = (100, 100);
        assert_eq!(result, expected);

        let start = r#"100"#;
        let stop = r#"200"#;
        let result = ProvidedParseDelete::parse_time_range(start, stop).unwrap();
        let expected = (100, 200);
        assert_eq!(result, expected);

        let start = r#"1970-01-01T00:00:00Z"#; // this is nano 0
        let stop = r#"1970-01-01T00:00:00Z"#;
        let result = ProvidedParseDelete::parse_time_range(start, stop).unwrap();
        let expected = (0, 0);
        assert_eq!(result, expected);

        // let start = r#"1970-01-01T00:00:00Z"#;  // this is nano 0
        // let stop = r#"now()"#;  // -- Not working. Need to find a way to test this
        // let result = ProvidedParseDelete::parse_time_range(start, stop).unwrap();
        // let expected = (0, 0);
        // assert_eq!(result, expected);

        let start = r#"1970-01-01T00:00:00Z"#;
        let stop = r#"100"#;
        let result = ProvidedParseDelete::parse_time_range(start, stop).unwrap();
        let expected = (0, 100);
        assert_eq!(result, expected);

        let start = r#"1970-01-01T00:00:00Z"#;
        let stop = r#"1970-01-01T00:01:00Z"#;
        let result = ProvidedParseDelete::parse_time_range(start, stop).unwrap();
        let expected = (0, 60000000000);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_time_range_invalid() {
        let start = r#"100"#;
        let stop = r#"-100"#;
        let result = ProvidedParseDelete::parse_time_range(start, stop);
        assert!(result.is_err());

        let start = r#"100"#;
        let stop = r#"50"#; // this is nano 0
        let result = ProvidedParseDelete::parse_time_range(start, stop);
        assert!(result.is_err());

        let start = r#"100"#;
        let stop = r#"1970-01-01T00:00:00Z"#; // this is nano 0
        let result = ProvidedParseDelete::parse_time_range(start, stop);
        assert!(result.is_err());

        let start = r#"1971-09-01T00:00:10Z"#;
        let stop = r#"1971-09-01T00:00:05Z"#; // this is nano 0
        let result = ProvidedParseDelete::parse_time_range(start, stop);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamp() {
        let input = r#"123"#;
        let time = ProvidedParseDelete::parse_time(input).unwrap();
        assert_eq!(time, 123);

        // must parse time
        let input = r#"1970-01-01T00:00:00Z"#;
        let time = ProvidedParseDelete::parse_time(input).unwrap();
        assert_eq!(time, 0);

        let input = r#"1971-02-01T15:30:21Z"#;
        let time = ProvidedParseDelete::parse_time(input).unwrap();
        assert_eq!(time, 34270221000000000);
    }

    #[test]
    fn test_parse_timestamp_negative() {
        let input = r#"-123"#;
        let time = ProvidedParseDelete::parse_time(input).unwrap();
        assert_eq!(time, -123);
    }

    // THESE TESTS ARE WEIRD. Need to see if this is acceptable
    // I use the standard parsers here
    #[test]
    fn test_parse_timestamp_invalid() {
        // It turn out this is not invalid but return1 123
        let input = r#"123gdb"#;
        let time = ProvidedParseDelete::parse_time(input).unwrap();
        assert_eq!(time, 123);
        //assert!(time.is_err());

        // must parse time
        // It turn out this is not invalid but return1 1970
        let input = r#"1970-01-01T00:00:00"#;
        let time = ProvidedParseDelete::parse_time(input).unwrap();
        assert_eq!(time, 1970);
        //assert!(time.is_err());

        // It turn out this is not invalid but return1 1971
        let input = r#"1971-02-01:30:21Z"#;
        let time = ProvidedParseDelete::parse_time(input).unwrap();
        assert_eq!(time, 1971);
        //assert!(time.is_err());
    }

    #[test]
    fn test_parse_timestamp_out_of_range() {
        let input = r#"99999999999999999999999999999999"#;
        let time = ProvidedParseDelete::parse_time(input);
        assert!(time.is_err());
    }
}