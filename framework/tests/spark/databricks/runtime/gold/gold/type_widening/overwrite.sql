select cast(field as int) as field
from values (1), (2) as source(field)
