with
    dates as (
        select `id` as `id`, __valid_from as __timestamp, 'upsert' as __operation
        from silver.monarch_scd2
        where __valid_from > '1900-01-02'
        union
        select `id` as `id`, __valid_to as __timestamp, 'delete' as __operation
        from silver.monarch_scd2
        where __is_deleted
    )
select
    d.`id` as __key,
    s.`id` as id,
    s.name as monarch,
    s.`doubleField` as value,
    d.__operation,
    if(d.__operation == 'delete', d.__timestamp + interval 1 second, d.__timestamp) as __timestamp
from dates d
left join silver.monarch_scd2 s on d.`id` = s.`id` and d.__timestamp between s.__valid_from and s.__valid_to
