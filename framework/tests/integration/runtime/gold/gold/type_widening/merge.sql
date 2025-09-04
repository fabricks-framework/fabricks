select
  __key,
	integerField::int as field
from
	silver.princess_type_widening
group by all