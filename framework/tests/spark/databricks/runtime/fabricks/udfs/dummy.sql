create or replace function udf_dummy(value string) returns string return concat('dummy_', value)
