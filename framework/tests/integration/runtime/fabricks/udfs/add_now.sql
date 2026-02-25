-- fmt: off
create or replace function udf_add_now(input string) returns string return array_join(array(input, current_timestamp()::string), "*", "-1")
