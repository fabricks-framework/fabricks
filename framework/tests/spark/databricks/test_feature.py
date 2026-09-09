"""Plugin-loading mechanisms proven end-to-end against a real workspace.

Each job is invoked directly via get_job(...).run() -- not through the
schedule, same pattern as test_notebook.py -- since neither job is tagged
(see runtime/gold/gold/feature/_config.feature.yml).
"""

from fabricks.context import SPARK
from fabricks.core import get_job


def test_bronze_feature_parser():
    # bronze.feature_parser: real file parsing via the "dummy" custom parser
    # plugin (fabricks/parsers/dummy.py) -- get_parser() loads it from
    # PATH_PARSERS by name. The tagged king/regent/queen jobs are all
    # register mode now (Bronze.parser asserts mode != "register"), so this
    # untagged job is the only place that proves plugin loading works.
    j = get_job(step="bronze", topic="feature", item="parser")
    j.run()
    df = SPARK.sql("select distinct __parsed_by from bronze.feature_parser")
    assert [r["__parsed_by"] for r in df.collect()] == ["dummy"]


def test_gold_feature_extender():
    # gold.feature_extender: job-level extender_options applies the "dummy"
    # extender (fabricks/extenders/dummy.py) via Invoker.extend_job().
    j = get_job(step="gold", topic="feature", item="extender")
    j.run()
    df = SPARK.sql("select distinct extended_by from gold.feature_extender")
    assert [r["extended_by"] for r in df.collect()] == ["dummy"]


def test_gold_feature_udf():
    # gold.feature_udf: udf_dummy (fabricks/udfs/dummy.sql) registered via
    # register_all_udfs() and called directly in the job's SQL.
    j = get_job(step="gold", topic="feature", item="udf")
    j.run()
    rows = SPARK.sql("select dummy from gold.feature_udf order by dummy").collect()
    assert [r["dummy"] for r in rows] == ["dummy_1", "dummy_2"]


def test_gold_feature_mask():
    # gold.feature_mask: table_options.masks.dummy applies mask_dummy
    # (fabricks/masks/dummy.sql) -- unconditionally (no caller-identity
    # check), so the real masked value is visible to any query, proving the
    # mask function actually runs, not just that the DDL was issued.
    j = get_job(step="gold", topic="feature", item="mask")
    j.run()
    rows = SPARK.sql("select dummy from gold.feature_mask order by dummy").collect()
    assert [r["dummy"] for r in rows] == ["***", "2"]


def test_gold_feature_cluster_by():
    # gold.feature_cluster_by: table_options.cluster_by enables real liquid
    # clustering -- Table.liquid_clustering_enabled reads the Delta table
    # feature back to confirm it actually took effect, not just that the
    # option was set (that DDL-shape half is already covered by
    # unit/config/test_create_table_defaults.py).
    j = get_job(step="gold", topic="feature", item="cluster_by")
    j.run()
    assert j.table.liquid_clustering_enabled
