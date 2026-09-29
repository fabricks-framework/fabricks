select d.time as time, s.id as id
from gold.dim_time d
cross join silver.king_scd1__current s
cross join silver.queen_scd1__current q
cross join transf.fact_memory f
where d.hour = '10'
limit 10
