select
	concat_ws('*', `id`, __valid_from) as __key,
	__valid_from,
	__valid_to,
	`id` as `id`,
	name as monarch,
	`doubleField` as `value`
from
	silver.monarch_scd2 s
where
	__is_current