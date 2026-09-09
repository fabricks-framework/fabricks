"""Gold.get_cdc_context() (framework/fabricks/core/jobs/gold.py:243-342) and
Silver.get_cdc_context() (framework/fabricks/core/jobs/silver.py:263-332):
the pure decision layer that turns job options + the incoming dataframe's
columns into the kwargs dict passed to cdc.get_query()/.complete()/.update().

One parametrized case per branch, not a full option x cdc-type x mode cross
product (see the plan this file implements). Gold cases favor `nocdc` +
mode="complete" as the "neutral" scenario when isolating an option that
also has slowly-changing-dimension-only side effects, so only one branch
moves at a time.
"""

import pytest

from fabricks.core import get_job
from fabricks.core.jobs.silver import Silver
from fabricks.models import StepPathOptions, StepSilverConf, StepSilverOptions
from tests.unit.config._helpers import _FakeDF


def _gold_job(*, mode="complete", change_data_capture="nocdc", **option_overrides):
    job = get_job(step="gold", topic="fact", item="step_option")
    options = job.conf.options.model_copy(
        update={"mode": mode, "change_data_capture": change_data_capture, **option_overrides}
    )
    job.conf = job.conf.model_copy(update={"options": options})
    return job


# -- deduplicate / rectify_as_upserts: explicit True/False/unset ------------


@pytest.mark.parametrize("deduplicate", [True, False, None])
def test_gold_deduplicate_option_maps_directly_to_context(deduplicate):
    job = _gold_job(deduplicate=deduplicate)

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    if deduplicate is None:
        assert context["deduplicate"] is False
        assert context["deduplicate_key"] is None
    else:
        assert context["deduplicate"] is deduplicate
        assert context["deduplicate_key"] is deduplicate
        assert context["deduplicate_hash"] is deduplicate


@pytest.mark.parametrize("rectify", [True, False, None])
def test_gold_rectify_as_upserts_option_maps_directly_to_context(rectify):
    job = _gold_job(rectify_as_upserts=rectify)

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation"]))

    assert context["rectify"] is (rectify if rectify is not None else False)


# -- hard_delete -> soft_delete, scd1/scd2 vs nocdc --------------------------


def test_gold_soft_delete_unset_for_nocdc_when_hard_delete_unset():
    job = _gold_job(change_data_capture="nocdc")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["soft_delete"] is None


def test_gold_soft_delete_defaults_true_for_scd_when_hard_delete_unset():
    job = _gold_job(change_data_capture="scd1")

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation"]))

    assert context["soft_delete"] is True


def test_gold_hard_delete_true_overrides_scd_default_to_soft_delete_false():
    job = _gold_job(change_data_capture="scd1", hard_delete=True)

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation"]))

    assert context["soft_delete"] is False


def test_gold_hard_delete_false_forces_soft_delete_true():
    job = _gold_job(change_data_capture="nocdc", hard_delete=False)

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["soft_delete"] is True


# -- metadata: job-level vs step-level fallback ------------------------------


def test_gold_metadata_job_level_true_wins():
    job = _gold_job(metadata=True)

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_metadata"] is True


def test_gold_metadata_falls_back_to_step_level_when_job_unset():
    job = _gold_job(metadata=None)
    step_conf = job.step_conf
    job.base_step_conf = step_conf.model_copy(
        update={"options": step_conf.options.model_copy(update={"metadata": True})}
    )

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_metadata"] is True


def test_gold_metadata_defaults_false_when_neither_level_sets_it():
    job = _gold_job(metadata=None)
    step_conf = job.step_conf
    job.base_step_conf = step_conf.model_copy(
        update={"options": step_conf.options.model_copy(update={"metadata": None})}
    )

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_metadata"] is False


# -- __key/__hash/__operation presence -> add_key/add_hash/add_operation ----


def test_gold_scd_adds_key_hash_operation_when_absent():
    job = _gold_job(change_data_capture="scd1")  # mode="complete" -> add_operation="upsert"

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_key"] is True
    assert context["add_hash"] is True
    assert context["add_operation"] == "upsert"
    # dedup unset + __operation missing -> deduplicate_hash downgraded from
    # the scd default (True) to None.
    assert context["deduplicate_hash"] is None


def test_gold_scd_skips_add_key_hash_operation_when_already_present():
    job = _gold_job(change_data_capture="scd1")

    context = job.get_cdc_context(_FakeDF(columns=["id", "__key", "__hash", "__operation"]))

    assert "add_key" not in context
    assert "add_hash" not in context
    assert "add_operation" not in context
    # __operation present -> the deduplicate_hash-downgrade block never runs,
    # so it keeps the scd default (True).
    assert context["deduplicate_hash"] is True


def test_gold_scd_update_mode_forces_rectify_when_operation_missing():
    job = _gold_job(change_data_capture="scd2", mode="update")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_operation"] == "reload"
    assert context["rectify"] is True  # forced True despite rectify_as_upserts unset


def test_gold_nocdc_update_mode_adds_key_hash_when_absent():
    job = _gold_job(change_data_capture="nocdc", mode="update")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_key"] is True
    assert context["add_hash"] is True


# -- mode -> slice / context["mode"] overrides -------------------------------


def test_gold_scd2_update_mode_slices_update():
    job = _gold_job(change_data_capture="scd2", mode="update")

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation"]))

    assert context["slice"] == "update"


def test_gold_nocdc_update_mode_slices_update_when_timestamp_present():
    job = _gold_job(change_data_capture="nocdc", mode="update")

    context = job.get_cdc_context(_FakeDF(columns=["id", "__timestamp"]))

    assert context["slice"] == "update"


def test_gold_nocdc_update_mode_no_slice_when_timestamp_absent():
    job = _gold_job(change_data_capture="nocdc", mode="update")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert "slice" not in context


def test_gold_append_mode_slices_update_when_timestamp_present():
    job = _gold_job(change_data_capture="nocdc", mode="append")

    context = job.get_cdc_context(_FakeDF(columns=["id", "__timestamp"]))

    assert context["slice"] == "update"


def test_gold_memory_mode_sets_context_mode_complete():
    job = _gold_job(change_data_capture="nocdc", mode="memory")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["mode"] == "complete"


def test_gold_reload_true_suppresses_slice_override():
    job = _gold_job(change_data_capture="scd2", mode="update")

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation"]), reload=True)

    assert "slice" not in context


# -- change_data_capture == scd2 -> correct_valid_from -----------------------


def test_gold_scd2_correct_valid_from_defaults_true_when_unset():
    job = _gold_job(change_data_capture="scd2")

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation"]))

    assert context["correct_valid_from"] is True


def test_gold_scd2_correct_valid_from_explicit_false_respected():
    job = _gold_job(change_data_capture="scd2", correct_valid_from=False)

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation"]))

    assert context["correct_valid_from"] is False


def test_gold_scd1_has_no_correct_valid_from_key():
    job = _gold_job(change_data_capture="scd1")

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation"]))

    assert "correct_valid_from" not in context


# -- persist_last_timestamp / persist_last_updated_timestamp / last_updated -


def test_gold_persist_last_timestamp_scd1_adds_timestamp_when_absent():
    job = _gold_job(change_data_capture="scd1", persist_last_timestamp=True)

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation"]))

    assert context["add_timestamp"] is True


def test_gold_persist_last_timestamp_scd1_skipped_when_timestamp_present():
    job = _gold_job(change_data_capture="scd1", persist_last_timestamp=True)

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation", "__timestamp"]))

    assert "add_timestamp" not in context


def test_gold_persist_last_timestamp_scd2_gated_on_valid_from():
    job = _gold_job(change_data_capture="scd2", persist_last_timestamp=True)

    context = job.get_cdc_context(_FakeDF(columns=["id", "__operation"]))

    assert context["add_timestamp"] is True


def test_gold_persist_last_updated_timestamp_adds_last_updated_when_absent():
    job = _gold_job(change_data_capture="nocdc", persist_last_updated_timestamp=True)

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_last_updated"] is True


def test_gold_last_updated_option_also_adds_last_updated():
    job = _gold_job(change_data_capture="nocdc", last_updated=True)

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_last_updated"] is True


def test_gold_add_last_updated_skipped_when_already_present():
    job = _gold_job(change_data_capture="nocdc", persist_last_updated_timestamp=True)

    context = job.get_cdc_context(_FakeDF(columns=["id", "__last_updated"]))

    assert "add_last_updated" not in context


# -- __order_duplicate_by_asc / _desc column presence ------------------------


def test_gold_order_duplicate_by_asc_from_column_presence():
    job = _gold_job()

    context = job.get_cdc_context(_FakeDF(columns=["id", "__order_duplicate_by_asc"]))

    assert context["order_duplicate_by"] == {"__order_duplicate_by_asc": "asc"}


def test_gold_order_duplicate_by_desc_from_column_presence():
    job = _gold_job()

    context = job.get_cdc_context(_FakeDF(columns=["id", "__order_duplicate_by_desc"]))

    assert context["order_duplicate_by"] == {"__order_duplicate_by_desc": "desc"}


def test_gold_order_duplicate_by_absent_when_neither_column_present():
    job = _gold_job()

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert "order_duplicate_by" not in context


# =============================== Silver =====================================


def _silver_job(*, mode="update", change_data_capture="nocdc", stream=True, **option_overrides):
    conf = {
        "step": "silver_test",
        "topic": "fact",
        "item": "dummy",
        "options": {"mode": mode, "change_data_capture": change_data_capture, "stream": stream, **option_overrides},
    }
    job = Silver(step="silver_test", topic="fact", item="dummy", conf=conf)
    # job.spark (Configurator.spark) unconditionally reads
    # self.step_spark_options -> self.step_conf.spark_options, which would
    # KeyError on STEPS["silver_test"] (not a registered step) the moment
    # anything here touches job.spark (the reload-eligibility probe does,
    # for any non-append/non-nocdc case) - bypass STEPS the same way `conf=`
    # bypasses YAML for job.conf, by overriding the base_step_conf
    # cached_property directly with a minimal, self-contained step config.
    job.base_step_conf = StepSilverConf(
        name="silver_test",
        path_options=StepPathOptions(runtime="silver_test", storage="silver_test"),
        options=StepSilverOptions(order=1, parent="bronze"),
    )
    return job


def test_silver_deduplicate_defaults_to_not_append():
    job = _silver_job(mode="update", change_data_capture="nocdc")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["deduplicate"] is True  # mode != "append"


def test_silver_deduplicate_false_for_append_mode():
    job = _silver_job(mode="append", change_data_capture="nocdc")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["deduplicate"] is False


def test_silver_deduplicate_explicit_option_wins_over_mode_default():
    job = _silver_job(mode="append", change_data_capture="nocdc", deduplicate=True)

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["deduplicate"] is True


def test_silver_rectify_true_when_reload_probe_finds_rows():
    # not_append (mode="update") and not nocdc (scd1) -> the reload-check
    # branch runs; stream stays at its default (True) so it skips the
    # self.table.exists() sub-branch (real DeltaTable/JVM call) and always
    # renders "-- no extra check", but the check_df.isEmpty() probe itself
    # still executes for real against the faked spark.
    job = _silver_job(mode="update", change_data_capture="scd1", stream=True)
    job.spark.sql.return_value.isEmpty.return_value = False

    context = job.get_cdc_context(_FakeDF(columns=["id", "__key"]))

    assert context["rectify"] is True


def test_silver_rectify_false_when_reload_probe_finds_no_rows():
    job = _silver_job(mode="update", change_data_capture="scd1", stream=True)
    job.spark.sql.return_value.isEmpty.return_value = True

    context = job.get_cdc_context(_FakeDF(columns=["id", "__key"]))

    assert context["rectify"] is False


def test_silver_rectify_stays_false_for_nocdc_without_probing():
    # nocdc -> "not nocdc" is False, so the reload probe never runs (and
    # spark.sql is never called for it).
    job = _silver_job(mode="update", change_data_capture="nocdc")
    job.spark.sql.reset_mock()

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["rectify"] is False


def test_silver_scd_adds_key_when_absent():
    job = _silver_job(mode="update", change_data_capture="scd1")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_key"] is True


def test_silver_scd_skips_add_key_when_present():
    job = _silver_job(mode="update", change_data_capture="scd1")

    context = job.get_cdc_context(_FakeDF(columns=["id", "__key"]))

    assert "add_key" not in context


def test_silver_memory_mode_sets_context_mode_complete():
    job = _silver_job(mode="memory", change_data_capture="nocdc")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["mode"] == "complete"


def test_silver_nocdc_memory_mode_adds_operation_when_absent():
    job = _silver_job(mode="memory", change_data_capture="nocdc")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_operation"] == "upsert"


def test_silver_latest_mode_slices_latest():
    job = _silver_job(mode="latest", change_data_capture="nocdc")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["slice"] == "latest"


def test_silver_non_stream_update_mode_slices_update():
    job = _silver_job(mode="update", change_data_capture="nocdc", stream=False)

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["slice"] == "update"


def test_silver_scd2_always_corrects_valid_from():
    job = _silver_job(mode="update", change_data_capture="scd2")
    job.spark.sql.return_value.isEmpty.return_value = True

    context = job.get_cdc_context(_FakeDF(columns=["id", "__key"]))

    assert context["correct_valid_from"] is True


def test_silver_excludes_operation_when_present_in_columns():
    job = _silver_job(mode="update", change_data_capture="scd1")
    job.spark.sql.return_value.isEmpty.return_value = True

    context = job.get_cdc_context(_FakeDF(columns=["id", "__key", "__operation"]))

    assert context["exclude"] == ["__operation"]


def test_silver_nocdc_always_excludes_operation():
    job = _silver_job(mode="update", change_data_capture="nocdc")

    context = job.get_cdc_context(_FakeDF(columns=["id"]))

    assert context["exclude"] == ["__operation"]
