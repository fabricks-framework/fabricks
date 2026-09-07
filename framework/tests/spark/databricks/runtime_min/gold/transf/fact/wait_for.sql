-- wait_for: [transf.fact_memory, silver.king_scd1] is a manual dependency
-- override (Gold.get_dependencies(), options.parents branch) -- unlike
-- gold.fact_dependency's auto-detected pair, this SQL doesn't even need to
-- reference transf.fact_memory for the ordering to be enforced, but does
-- here anyway so the real wall-clock check has real data to compare.
select id as id, monarch as monarch, value as value from transf.fact_memory
