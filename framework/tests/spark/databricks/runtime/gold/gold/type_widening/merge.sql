select __key, cast(field as int) as field
from values ('one', 1), ('two', 2) as source(__key, field)
