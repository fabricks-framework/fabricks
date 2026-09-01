create or replace view expected.gold_scd2_job5 as
select `id` as id,
    `name` as monarch,
    `doubleField` as value,
    __is_deleted,
    __is_current,
    __valid_from,
    __valid_to
from expected.silver_scd2_job5
