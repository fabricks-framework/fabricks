create or replace view expected.silver_scd2_job4 as
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
values  -- closed
  (
    '2022-03-05 00:12:34',
    '2022-04-01 00:00:59',
    '301',
    'Elisabeth',
    '0.20220305',
    'true',
    'false',
    'true',
    'queen'
  ),
  (
    '2022-03-05 00:12:34',
    '2022-04-01 00:00:59',
    '401',
    'Astrid',
    '0.20220305',
    'true',
    'false',
    'true',
    'queen'
  ),
-- open
  (
    '2022-04-01 00:01:00',
    '9999-12-31 00:00:00',
    '501',
    'Fabiola',
    '0.20220401',
    'true',
    'true',
    'false',
    'queen'
  ),
  (
    '2022-04-01 00:01:00',
    '9999-12-31 00:00:00',
    '5',
    'Baudouin',
    '0.20220401',
    'true',
    'true',
    'false',
    'king'
  ),
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
from expected.silver_scd2_job3
where not `__is_current`
order by id,
  `__valid_from`
