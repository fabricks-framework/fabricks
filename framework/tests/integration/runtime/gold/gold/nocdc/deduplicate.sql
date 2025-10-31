with
    base as (
        select
            concat_ws('*', `id`, __valid_from) as __key,
            if(__valid_to::date == '9999-12-31', __valid_from, __valid_to) as __timestamp,
            __valid_from as valid_from,
            __valid_to as valid_to,
            `id` as `id`,
            name as monarch,
            `doubleField` as `value`
        from silver.monarch_scd2 s
        where __is_current
    )
select *
from base
union all
select *
from base
