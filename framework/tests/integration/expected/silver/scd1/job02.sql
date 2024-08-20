create or replace view expected.silver_scd1_job2 as
select *
except (__valid_from, __valid_to)
from expected.silver_scd2_job2 qualify row_number() over (
        partition by `id`
        order by __valid_to desc
    ) = 1
