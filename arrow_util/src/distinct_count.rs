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

/// Given a list of `RecordBatch`es, compute the cardinality of all data in all columns across all
/// of the batches and return a sort key that lists the columns in order from lowest cardinality
/// to highest cardinality, with the time column always last.
fn compute_sort_key(batches: &[RecordBatch]) -> SortKey {
    unimplemented!()
}

/// Takes a list of `RecordBatch`es and computes the number of distinct values for each string tag
/// column across all batches, also known as "cardinality". Used to determine sort order.
fn distinct_counts(batches: &[RecordBatch]) -> HashMap<String, NonZeroU64> {
    let mut distinct_values_across_batches = HashMap::new();

    for batch in batches {
        for (column, distinct_values) in distinct_values(batch) {
            let set = distinct_values_across_batches
                .entry(column)
                .or_insert_with(HashSet::new);
            set.extend(distinct_values.into_iter());
        }
    }

    distinct_values_across_batches
        .into_iter()
        .filter_map(|(column, distinct_values)| {
            distinct_values
                .len()
                .try_into()
                .ok()
                .and_then(NonZeroU64::new)
                .map(|count| (column, count))
        })
        .collect()
}

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

    #[test]
    fn test_distinct_count() {}

    #[test]
    fn test_sort_key() {
        // Across these three record batches:
        // - `host` has 2 distinct values: "a", "b"
        // - 'env' has 3 distinct values: "prod", "stage", "dev"
        // host's 2 values appear in each record batch, so the distinct counts could be incorrectly
        // aggregated together as 2 + 2 + 2 = 6. env's 3 values each occur in their own record
        // batch, so they should always be aggregated as 3.
        // host has the lower cardinality, so it should appear first in the sort key.
        let lp1 = r#"
            cpu,host=a,env=prod val=23 1
            cpu,host=b,env=prod val=2 2
        "#;
        let rb1 = lp_to_record_batch(lp1);

        let lp2 = r#"
            cpu,host=a,env=stage val=23 3
            cpu,host=b,env=stage val=2 4
        "#;
        let rb2 = lp_to_record_batch(lp2);

        let lp3 = r#"
            cpu,host=a,env=dev val=23 5
            cpu,host=b,env=dev val=2 6
        "#;
        let rb3 = lp_to_record_batch(lp3);

        let sort_key = compute_sort_key(&[rb1, rb2, rb3]);
        assert_eq!(sort_key, SortKey::from_columns(["host", "env", "time"]));
    }
}
