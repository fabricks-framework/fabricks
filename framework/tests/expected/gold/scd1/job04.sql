create or replace view expected.gold_scd1_job4 as
select
    id,
    monarch,
    value,
  `__is_current`,
  `__is_deleted`
from
  expected.gold_scd2_job4
where
  `__is_current`
union all
select
    id,
    monarch,
    value,
  false as `__is_current`,
  true as `__is_deleted`
from
  expected.gold_scd1_job3 s1
  left anti join expected.gold_scd2_job4 s2 on s1.id = s2.id and s2.`__is_current`
