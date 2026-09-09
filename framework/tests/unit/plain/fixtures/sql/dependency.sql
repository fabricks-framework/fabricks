select
  d.time,
  f.memory,
  s.id
from gold.dim_time as d
cross join transf.fact_memory as f
cross join silver.king_and_queen_scd1__current as s
