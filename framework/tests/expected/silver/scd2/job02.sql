create or replace view expected.silver_scd2_job2 as
select cast(col1 as timestamp) as __valid_from,
    cast(col2 as timestamp) as __valid_to,
    cast(col3 as int) as `id`,
    cast(col4 as string) as `name`,
    cast(col5 as double) as `doubleField`,
    cast(col6 as string) as `newField`,
    cast(col7 as boolean) as __is_current,
    cast(col8 as boolean) as __is_deleted,
    cast(col9 as string) as __source
from  -- closed
values (
        '1900-01-01 00:00:00',
        '2022-02-05 00:00:59',
        '1',
        'Leopold I',
        '0.19000101',
        'null',
        'false',
        'true',
        'king'
    ),
    (
        '2022-01-02 00:01:00',
        '2022-02-05 00:00:59',
        '2',
        'Leopold II',
        '0.20220102',
        'null',
        'false',
        'true',
        'king'
    ),
-- open
    (
        '2022-02-05 00:01:00',
        '9999-12-31 00:00:00',
        '3',
        'Albert I',
        '0.20220205',
        'true',
        'true',
        'false',
        'king'
    ),
    (
        '2022-02-05 00:01:00',
        '9999-12-31 00:00:00',
        '301',
        'Elisabeth',
        '0.20220205',
        'true',
        'true',
        'false',
        'queen'
    )
union all
select __valid_from,
    __valid_to,
    `id`,
    `name`,
    `doubleField`,
    null as `newField`,
    __is_current,
    __is_deleted,
    __source
from expected.silver_scd2_job1
where not `__is_current`
order by id,
    `__valid_from`
