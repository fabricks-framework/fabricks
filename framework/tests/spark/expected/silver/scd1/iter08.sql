create or replace view expected.silver_scd1_iter8 as
select *
except (__valid_from, __valid_to)
from expected.silver_scd2_iter8 qualify row_number() over (
        partition by `id`
        order by __valid_to desc
    ) = 1
