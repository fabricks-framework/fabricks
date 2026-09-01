create or replace view expected.silver_scd2_job1 as
select cast(col1 as timestamp) as __valid_from,
    cast(col2 as timestamp) as __valid_to,
    cast(col3 as int) as `id`,
    cast(col4 as string) as `name`,
    cast(col5 as double) as `doubleField`,
    cast(col6 as boolean) as __is_current,
    cast(col7 as boolean) as __is_deleted,
    cast(col8 as string) as __source
from  -- king
values (
        '1900-01-01 00:00:00',
        '9999-12-31 00:00:00',
        '1',
        'Leopold I',
        '0.19000101',
        'true',
        'false',
        'king'
    ),
    (
        '1900-01-01 00:00:00',
        '2022-01-02 00:00:59',
        '2',
        'Leopold II',
        '0.19000101',
        'false',
        'false',
        'king'
    ),
    (
        '2022-01-02 00:01:00',
        '9999-12-31 00:00:00',
        '2',
        'Leopold II',
        '0.20220102',
        'true',
        'false',
        'king'
    ),
-- queen
    (
        '1900-01-01 00:00:00',
        '2022-01-04 00:00:59',
        '101',
        'Louise',
        '0.19000101',
        'false',
        'true',
        'queen'
    ),
    (
        '2022-01-02 00:01:00',
        '2022-01-03 00:00:59',
        '201',
        'Marie-Henriette',
        '0.20220102',
        'false',
        'true',
        'queen'
    )
order by id,
    `__valid_from`
