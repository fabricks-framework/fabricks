create or replace view expected.gold_scd1_iter1 as
select
    id,
    monarch,
    value,
  `__is_current`,
  `__is_deleted`
from
  expected.gold_scd2_iter1
where
  `__is_current`
