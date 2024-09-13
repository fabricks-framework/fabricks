with
    f as (select '2022-01-01' as `date`),
    d as (select explode(sequence(0, 365, 1)) as i),
    merger as (select cast(dateadd(day, i, `date`) as date) as `date` from f cross join d)
select cast(date_format(`date`, 'yyyyMmdd') as bigint) as __identity, `date`
from merger
