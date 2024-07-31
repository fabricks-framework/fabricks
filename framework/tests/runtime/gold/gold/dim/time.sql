with
    h as (select explode(sequence(0, 23, 1)) as h),
    m as (select explode(sequence(0, 59, 1)) as m),
    merger as (select format_number(h, '00') as hour, format_number(m, '00') as minute from h cross join m)
select
    cast(concat(hour, minute) as int) as __identity,
    concat(hour, minute) as __key,
    concat(hour, minute) as id,
    hour,
    minute,
    concat_ws(':', hour, minute) as time
from merger
