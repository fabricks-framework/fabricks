select
    udf.key(array(f.id, d.time)) as __key,
    f.id as id,
    f.monarch as monarch,
    s.__source as role,
    f.value as value,
    d.time as time
from gold.dim_time d
cross join transf.fact_memory f
left join silver.king_and_queen_scd1__current s on f.id = s.id
where d.hour = 10
