create or replace view expected.gold_scd0_job5 as
select
    id,
    monarch,
    value
from
  expected.gold_scd2_job5 s0
where
  true
  
  and exists (
    select 1
    from expected.gold_scd1_job5 s1
    where s1.id = s0.id
      and s1.`__is_current`
  )

qualify row_number() over (partition by id order by `__valid_from` asc) = 1
