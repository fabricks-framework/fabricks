select
    udf_identity(s.id, id) as __identity,
    udf_key(array(s.id)) as __key,
    s.id as id,
    s.name as monarch,
    udf_additition(1, 2) as addition
-- udf_parse_phone_number('0478/478.478', 'BE') as phone_number
from silver.monarch_scd1__current s
