create or replace view expected.gold_scd0_job8 as
select
    id,
    monarch,
    value
from
  expected.gold_scd0_job7
union all
select
  id,
  monarch,
  value
from
  expected.gold_scd2_job8 s1
  left anti join expected.gold_scd0_job7 s0 on s1.id = s0.id
where
  `__is_current`
