create or replace view expected.gold_scd0_iter1 as
select
    id,
    monarch,
    value
from
  expected.gold_scd2_iter1
where
  `__is_current`
