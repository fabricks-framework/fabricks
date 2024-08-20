create or replace view expected.silver_scd2_job9 as
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
    '2022-08-01 00:01:00',
    '2022-09-01 00:00:59',
    '7',
    'Philippe',
    '0.20220801',
    'false',
    'false',
    'false',
    'king'
  ),
  (
    '2022-09-01 00:01:00',
    '2022-09-02 00:00:59',
    '7',
    'Philippe',
    '0.20220901',
    'false',
    'false',
    'true',
    'king'
  ),
  (
    '2022-09-03 00:01:00',
    '2022-09-04 00:00:59',
    '7',
    'Philippe',
    '0.20220903',
    'false',
    'false',
    'true',
    'king'
  ),
  (
    '2022-09-08 00:01:00',
    '9999-12-31 00:00:00',
    '8',
    'Philippe',
    '0.20220902',
    'true',
    'true',
    'false',
    'king'
  ),
  (
    '2022-08-01 00:01:00',
    '2022-09-01 00:00:59',
    '701',
    'Mathilde',
    '0.20220801',
    'false',
    'false',
    'true',
    'queen'
  ),
  (
    '2022-09-08 00:01:00',
    '9999-12-31 00:00:00',
    '801',
    'Mathilde',
    '0.20220904',
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
  `newField`,
  __is_current,
  __is_deleted,
  __source
from expected.silver_scd2_job8
where not `__is_current`
order by id,
  `__valid_from`
