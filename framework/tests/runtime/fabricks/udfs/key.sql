-- fmt: off
create or replace function udf_key(keys array<string>) returns string return array_join(keys, "*", "-1")
