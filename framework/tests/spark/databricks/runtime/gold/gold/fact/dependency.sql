-- gold.dim_time here is auto-detected by Gold.get_dependencies() via SQL
-- parsing (sqlglot) -- no wait_for needed for this pair. Proves gold-depends-
-- on-gold ordering; the silver join proves gold-depends-on-silver ordering
-- alongside it.
select d.time as time, s.id as id from gold.dim_time d cross join silver.king_scd1__current s where d.hour = '10'
