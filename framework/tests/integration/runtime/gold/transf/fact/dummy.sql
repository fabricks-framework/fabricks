with merger as (
select 1 as `value`
from gold.fact_overwrite
union all
select 2 as `value`
from gold.fact_memory
union all
select 3 as `value`
from gold.fact_option
)
select *
from merger 
qualify row_number() over (partition by `value`) = 1