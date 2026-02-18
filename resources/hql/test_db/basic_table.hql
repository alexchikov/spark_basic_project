create table test_db.basic_table(
    id integer,
    some_value string,
    some_decimal decimal(38),
    some_date date
) partitioned by (some_partition_key date)