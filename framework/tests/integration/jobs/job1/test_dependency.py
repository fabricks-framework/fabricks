from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(151)
def test_gold_fact_dependency_sql():
    def check():
        dep_df = SPARK.sql(
            """
            select
              j.job_id,
              j.job,
              p.job as parent
            from
              fabricks.gold_dependencies d
              left join fabricks.jobs p on d.parent_id = p.job_id
              left join fabricks.jobs j on d.job_id = j.job_id
            where
              true
              and j.job = 'gold.fact_dependency_sql'
            group by all
            """
        )
        assert dep_df.count() == 3, f"dependency {dep_df.count()} <> 3"

        expected_parents = set(
            sorted(
                [
                    "gold.dim_time",
                    "transf.fact_memory",
                    "silver.king_and_queen_scd1",
                ]
            )
        )
        parents = set(sorted([row.parent for row in dep_df.select("parent").collect()]))
        assert parents == expected_parents, f"{', '.join(parents)} <> {', '.join(expected_parents)}"

    check()

    j = get_job(step="gold", topic="fact", item="dependency_sql")
    j.update_dependencies()
    check()


@pytest.mark.order(152)
def test_gold_fact_dependency_notebook():
    def check():
        dep_df = SPARK.sql(
            """
            select
              d.dependency_id,
              j.job_id,
              j.job,
              d.parent_id,
              p.job as parent
            from
              fabricks.gold_dependencies d
                left join fabricks.jobs p
                  on d.parent_id = p.job_id
                left join fabricks.jobs j
                  on d.job_id = j.job_id
            where
              true
              and j.job = 'gold.fact_dependency_notebook'
            group by
              all
            """
        )
        assert dep_df.count() == 4, f"dependency {dep_df.count()} <> 4"

        expected_parents = set(
            sorted(
                [
                    "gold.dim_time",
                    "transf.fact_memory",
                    "silver.king_and_queen_scd1",
                    "silver.monarch_scd1",
                ]
            )
        )
        parents = set(sorted([row.parent for row in dep_df.select("parent").collect()]))
        assert parents == expected_parents, f"{', '.join(parents)} <> {', '.join(expected_parents)}"

        # check specific ids to ensure hashing is correct
        row = dep_df.filter("parent = 'gold.dim_time'").collect()[0]
        assert row.job_id == "c576b41222d104dbbc0e0b84cda29ff5", (
            f"job_id {row.job_id} <> c576b41222d104dbbc0e0b84cda29ff5"
        )
        assert row.parent_id == "4422820e99d36169d633ef4b0f4f4889", (
            f"parent_id {row.parent_id} <> 4422820e99d36169d633ef4b0f4f4889"
        )
        assert row.dependency_id == "dd6782bc6d6b6504e66c437d9ac79b55", (
            f"dependency_id {row.dependency_id} <> dd6782bc6d6b6504e66c437d9ac79b55"
        )
