from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import Logger
from fabricks.core import get_job

Logger.setLevel(ERROR)


@pytest.mark.order(151)
def test_gold_fact_dependency():
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
              and j.step = 'gold'
              and j.topic = 'fact'
              and j.item = 'dependency'
            group by all
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

    check()

    j = get_job(step="gold", topic="fact", item="dependency")
    j.update_dependencies()
    check()
