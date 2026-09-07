declare or replace _max date;
declare or replace _min date;
set variable _max = (select max(`__valid_from`) from silver.monarch_scd2)
;
set variable _min = (select min(`__valid_from`) from silver.monarch_scd2)
;
select `id` as __key, `id` as id, name as monarch, `doubleField` as `value`, _max as `max`, _min as `min`
from silver.monarch_scd1__current s
