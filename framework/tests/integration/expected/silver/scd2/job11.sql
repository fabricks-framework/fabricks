create or replace view expected.silver_scd2_job11 as
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
values -- open
  (
    '2022-10-03 00:01:00',
    '2022-11-01 00:00:59',
    '9',
    'Philippe',
    '0.20221003',
    'true',
    'False',
    'true',
    'king'
  ),
  (
    '2022-11-01 00:01:00',
    '9999-12-31 00:00:00',
    '8',
    'Philippe',
    '0.20221101',
    'false',
    'true',
    'false',
    'king'
  )
union all
select __valid_from,
  __valid_to,
  `id`,
  `name`,
  `doubleField`,
  `newField`,
  __is_current,
  __is_deleted,
  __source
from expected.silver_scd2_job10
where not `__is_current`
order by id,
  `__valid_from`
