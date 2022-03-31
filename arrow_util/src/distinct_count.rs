use arrow::array::{Array, ArrayRef, DictionaryArray, StringArray};
use arrow::{
    datatypes::{DataType, Int32Type},
    record_batch::RecordBatch,
};
use schema::sort::SortKey;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
};

/// Takes a `RecordBatch` and returns a map of column names to the set of the distinct string
/// values, for string tags only. Used to compute cardinality across multiple `RecordBatch`es.
fn distinct_values(batch: &RecordBatch) -> HashMap<String, HashSet<String>> {
    let schema = batch.schema();
    batch
        .columns()
        .iter()
        .zip(schema.fields())
        .flat_map(|(col, field)| {
            dbg!(field.name(), field.data_type());
            match field.data_type() {
                DataType::Dictionary(key, value) => {
                    let col = col
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                        .expect("unexpected datatype");

                    let values = col.values();
                    let values = values
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("unexpected datatype");

                    Some((
                        field.name().into(),
                        values.iter().flatten().map(ToString::to_string).collect(),
                    ))
                }
                other => None,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::selection::Selection;

    fn lp_to_record_batch(line_protocol: &str) -> RecordBatch {
        let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(line_protocol);
        mb.to_arrow(Selection::All).unwrap()
    }

    #[test]
    fn test_distinct_values() {
        let lp = r#"
            cpu,host=a val=23 1
            cpu,host=b,env=prod val=2 1
            cpu,host=c,env=stage val=11 1
            cpu,host=a,env=prod val=14 2
        "#;
        let rb = lp_to_record_batch(lp);

        let distinct_values = distinct_values(&rb);

        // Return unique values
        assert_eq!(
            *distinct_values.get("host").unwrap(),
            HashSet::from(["a".into(), "b".into(), "c".into()]),
        );
        // TODO: do nulls count as a value?
        assert_eq!(
            *distinct_values.get("env").unwrap(),
            HashSet::from(["prod".into(), "stage".into()]),
        );

        // Requesting a column not present returns None
        assert_eq!(distinct_values.get("foo"), None);

        // Distinct count isn't computed for the time column or fields
        assert_eq!(distinct_values.get("time"), None);
        assert_eq!(distinct_values.get("val"), None);
    }
}
