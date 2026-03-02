create or replace view expected.gold_scd0_job1 as
select
    id,
    monarch,
    value
from
  expected.gold_scd2_job1
where
  `__is_current`
