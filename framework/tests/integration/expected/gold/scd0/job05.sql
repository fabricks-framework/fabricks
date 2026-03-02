create or replace view expected.gold_scd0_job5 as
select
    id,
    monarch,
    value
from
  expected.gold_scd2_job5
where
  qualify row_number() over (partition by id order by `__valid_from` asc) = 1
