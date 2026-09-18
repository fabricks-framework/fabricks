create or replace view expected.scd1_iter9 as
select *
except (__valid_from, __valid_to)
from expected.scd2_iter9 qualify row_number() over (
        partition by `id`
        order by __valid_to desc
    ) = 1
