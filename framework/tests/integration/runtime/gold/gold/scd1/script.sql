declare variable _max DATE  = (select max(`__valid_from`) from silver.monarch_scd2);
declare variable _min DATE = (select min(`__valid_from`) from silver.monarch_scd2);
select `id` as __key, `id` as id, name as monarch, `doubleField` as `value`, _max as `max`, _min as `min`
from silver.monarch_scd1__current s
