select `id` as __key, udf_identity(`id`, `id`) as __identity, `id` as id, name as monarch, `doubleField` as value
from silver.monarch_scd1__current s
