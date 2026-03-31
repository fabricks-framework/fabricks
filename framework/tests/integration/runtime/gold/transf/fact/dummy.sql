select 1 as `value`
from gold.fact_overwrite
limit 1
union all
select 2 as `value`
from gold.fact_memory
limit 1
union all
select 3 as `value`
from gold.fact_option
limit 1
