create or replace view expected.silver_scd2_job6 as
select cast(col1 as timestamp) as __valid_from,
    cast(col2 as timestamp) as __valid_to,
    cast(col3 as int) as `id`,
    cast(col4 as string) as `name`,
    cast(col5 as double) as `doubleField`,
    cast(col6 as string) as `newField`,
    cast(col7 as boolean) as __is_current,
    cast(col8 as boolean) as __is_deleted,
    cast(col9 as string) as __source
from
values
-- open
    (
        '2022-05-01 00:01:00',
        '9999-12-31 00:00:00',
        '6',
        'Albert II',
        '0.20220501',
        'false',
        'true',
        'false',
        'king'
    ),
    (
      '2022-05-01 00:01:00',
      '9999-12-31 00:00:00',
      '601',
      'Paola',
      '0.20220501',
      'false',
      'true',
      'false',
      'queen'
  )
union all
select
  __valid_from,
  __valid_to,
  `id`,
  `name`,
  `doubleField`,
  `newField`,
  __is_current,
  __is_deleted,
  __source
from
  expected.silver_scd2_job5
where
  not `__is_current`
order by id, `__valid_from`
