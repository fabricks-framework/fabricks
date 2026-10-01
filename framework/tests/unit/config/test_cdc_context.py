"""Gold.build_cdc_context() (framework/fabricks/core/jobs/gold.py:243-342) and
Silver.build_cdc_context() (framework/fabricks/core/jobs/silver.py:263-332):
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

    context = job.build_cdc_context(_FakeDF(columns=["id"]))

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

    context = job.build_cdc_context(_FakeDF(columns=["id", "__operation"]))

    assert context["rectify"] is (rectify if rectify is not None else False)


# -- metadata: job-level vs step-level fallback ------------------------------


def test_gold_metadata_job_level_true_wins():
    job = _gold_job(metadata=True)

    context = job.build_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_metadata"] is True


@pytest.mark.parametrize(("step_metadata", "expected"), [(True, True), (None, False)])
def test_gold_metadata_step_level_fallback(step_metadata, expected):
    job = _gold_job(metadata=None)
    step_conf = job.step_conf
    job.base_step_conf = step_conf.model_copy(
        update={"options": step_conf.options.model_copy(update={"metadata": step_metadata})}
    )

    context = job.build_cdc_context(_FakeDF(columns=["id"]))

    assert context["add_metadata"] is expected


def test_gold_reload_true_suppresses_slice_override():
    job = _gold_job(change_data_capture="scd2", mode="update")

    context = job.build_cdc_context(_FakeDF(columns=["id", "__operation"]), reload=True)

    assert "slice" not in context


# -- one case per branch: (job options, incoming columns, expected context items, keys that must be absent) ----------
# nocdc + mode="complete" is the neutral scenario, so only the option under test moves a branch.

_ID = ["id"]
_ID_OP = ["id", "__operation"]

_GOLD_CASES = {
    # hard_delete -> soft_delete, scd1/scd2 vs nocdc
    "soft_delete_unset_for_nocdc_when_hard_delete_unset": (
        {"change_data_capture": "nocdc"},
        _ID,
        {"soft_delete": None},
        (),
    ),
    "soft_delete_defaults_true_for_scd_when_hard_delete_unset": (
        {"change_data_capture": "scd1"},
        _ID_OP,
        {"soft_delete": True},
        (),
    ),
    "hard_delete_true_overrides_scd_default_to_soft_delete_false": (
        {"change_data_capture": "scd1", "hard_delete": True},
        _ID_OP,
        {"soft_delete": False},
        (),
    ),
    "hard_delete_false_forces_soft_delete_true": (
        {"change_data_capture": "nocdc", "hard_delete": False},
        _ID,
        {"soft_delete": True},
        (),
    ),
    # __key/__hash/__operation presence -> add_key/add_hash/add_operation
    # (dedup unset + __operation missing downgrades deduplicate_hash from the scd default True to None;
    # __operation present skips that block, so the default stays True)
    "scd_adds_key_hash_operation_when_absent": (
        {"change_data_capture": "scd1"},
        _ID,
        {"add_key": True, "add_hash": True, "add_operation": "upsert", "deduplicate_hash": None},
        (),
    ),
    "scd_skips_add_key_hash_operation_when_already_present": (
        {"change_data_capture": "scd1"},
        ["id", "__key", "__hash", "__operation"],
        {"deduplicate_hash": True},
        ("add_key", "add_hash", "add_operation"),
    ),
    "scd_update_mode_forces_rectify_when_operation_missing": (
        {"change_data_capture": "scd2", "mode": "update"},
        _ID,
        {"add_operation": "reload", "rectify": True},
        (),
    ),
    "nocdc_update_mode_adds_key_hash_when_absent": (
        {"change_data_capture": "nocdc", "mode": "update"},
        _ID,
        {"add_key": True, "add_hash": True},
        (),
    ),
    # mode -> slice / context["mode"] overrides
    "scd2_update_mode_slices_update": (
        {"change_data_capture": "scd2", "mode": "update"},
        _ID_OP,
        {"slice": "update"},
        (),
    ),
    "nocdc_update_mode_slices_update_when_timestamp_present": (
        {"change_data_capture": "nocdc", "mode": "update"},
        ["id", "__timestamp"],
        {"slice": "update"},
        (),
    ),
    "nocdc_update_mode_no_slice_when_timestamp_absent": (
        {"change_data_capture": "nocdc", "mode": "update"},
        _ID,
        {},
        ("slice",),
    ),
    "append_mode_slices_update_when_timestamp_present": (
        {"change_data_capture": "nocdc", "mode": "append"},
        ["id", "__timestamp"],
        {"slice": "update"},
        (),
    ),
    "memory_mode_sets_context_mode_complete": (
        {"change_data_capture": "nocdc", "mode": "memory"},
        _ID,
        {"mode": "complete"},
        (),
    ),
    # change_data_capture == scd2 -> correct_valid_from
    "scd2_correct_valid_from_defaults_true_when_unset": (
        {"change_data_capture": "scd2"},
        _ID_OP,
        {"correct_valid_from": True},
        (),
    ),
    "scd2_correct_valid_from_explicit_false_respected": (
        {"change_data_capture": "scd2", "correct_valid_from": False},
        _ID_OP,
        {"correct_valid_from": False},
        (),
    ),
    "scd1_has_no_correct_valid_from_key": ({"change_data_capture": "scd1"}, _ID_OP, {}, ("correct_valid_from",)),
    # persist_last_timestamp / persist_last_updated_timestamp / last_updated
    "persist_last_timestamp_scd1_adds_timestamp_when_absent": (
        {"change_data_capture": "scd1", "persist_last_timestamp": True},
        _ID_OP,
        {"add_timestamp": True},
        (),
    ),
    "persist_last_timestamp_scd1_skipped_when_timestamp_present": (
        {"change_data_capture": "scd1", "persist_last_timestamp": True},
        ["id", "__operation", "__timestamp"],
        {},
        ("add_timestamp",),
    ),
    "persist_last_timestamp_scd2_gated_on_valid_from": (
        {"change_data_capture": "scd2", "persist_last_timestamp": True},
        _ID_OP,
        {"add_timestamp": True},
        (),
    ),
    "persist_last_updated_timestamp_adds_last_updated_when_absent": (
        {"change_data_capture": "nocdc", "persist_last_updated_timestamp": True},
        _ID,
        {"add_last_updated": True},
        (),
    ),
    "last_updated_option_also_adds_last_updated": (
        {"change_data_capture": "nocdc", "last_updated": True},
        _ID,
        {"add_last_updated": True},
        (),
    ),
    "add_last_updated_skipped_when_already_present": (
        {"change_data_capture": "nocdc", "persist_last_updated_timestamp": True},
        ["id", "__last_updated"],
        {},
        ("add_last_updated",),
    ),
    # __order_duplicate_by_asc / _desc column presence
    "order_duplicate_by_asc_from_column_presence": (
        {},
        ["id", "__order_duplicate_by_asc"],
        {"order_duplicate_by": {"__order_duplicate_by_asc": "asc"}},
        (),
    ),
    "order_duplicate_by_desc_from_column_presence": (
        {},
        ["id", "__order_duplicate_by_desc"],
        {"order_duplicate_by": {"__order_duplicate_by_desc": "desc"}},
        (),
    ),
    "order_duplicate_by_absent_when_neither_column_present": ({}, _ID, {}, ("order_duplicate_by",)),
}


def _assert_context(context, expected, absent):
    for key, value in expected.items():
        assert context[key] == value, key
        if value is None or isinstance(value, bool):
            assert context[key] is value, key
    for key in absent:
        assert key not in context, key


@pytest.mark.parametrize(
    ("job_options", "columns", "expected", "absent"), _GOLD_CASES.values(), ids=_GOLD_CASES.keys()
)
def test_gold_build_cdc_context(job_options, columns, expected, absent):
    job = _gold_job(**job_options)

    context = job.build_cdc_context(_FakeDF(columns=columns))

    _assert_context(context, expected, absent)


# =============================== Silver =====================================


def _silver_job(*, mode="update", change_data_capture="nocdc", stream=True, **option_overrides):
    conf = {
        "step": "silver",
        "topic": "fact",
        "item": "dummy",
        "options": {"mode": mode, "change_data_capture": change_data_capture, "stream": stream, **option_overrides},
    }
    job = Silver(step="silver", topic="fact", item="dummy", conf=conf)
    # job.spark (Configurator.spark) unconditionally reads
    # Keep the step config minimal so these tests only exercise CDC context.
    job.base_step_conf = StepSilverConf(
        name="silver",
        path_options=StepPathOptions(runtime="silver", storage="silver"),
        options=StepSilverOptions(order=1, parent="bronze"),
    )
    return job


# (job options, incoming columns, expected context items, keys that must be absent, reload-probe is empty).
# The reload probe only runs for a non-nocdc, non-append job; None leaves the faked spark untouched.
_SILVER_CASES = {
    "deduplicate_defaults_to_not_append": (
        {"mode": "update", "change_data_capture": "nocdc"},
        _ID,
        {"deduplicate": True},
        (),
        None,
    ),
    "deduplicate_false_for_append_mode": (
        {"mode": "append", "change_data_capture": "nocdc"},
        _ID,
        {"deduplicate": False},
        (),
        None,
    ),
    "deduplicate_explicit_option_wins_over_mode_default": (
        {"mode": "append", "change_data_capture": "nocdc", "deduplicate": True},
        _ID,
        {"deduplicate": True},
        (),
        None,
    ),
    "rectify_true_when_reload_probe_finds_rows": (
        {"mode": "update", "change_data_capture": "scd1", "stream": True},
        ["id", "__key"],
        {"rectify": True},
        (),
        False,
    ),
    "rectify_false_when_reload_probe_finds_no_rows": (
        {"mode": "update", "change_data_capture": "scd1", "stream": True},
        ["id", "__key"],
        {"rectify": False},
        (),
        True,
    ),
    "scd_adds_key_when_absent": ({"mode": "update", "change_data_capture": "scd1"}, _ID, {"add_key": True}, (), None),
    "scd_skips_add_key_when_present": (
        {"mode": "update", "change_data_capture": "scd1"},
        ["id", "__key"],
        {},
        ("add_key",),
        None,
    ),
    "memory_mode_sets_context_mode_complete": (
        {"mode": "memory", "change_data_capture": "nocdc"},
        _ID,
        {"mode": "complete"},
        (),
        None,
    ),
    "nocdc_memory_mode_adds_operation_when_absent": (
        {"mode": "memory", "change_data_capture": "nocdc"},
        _ID,
        {"add_operation": "upsert"},
        (),
        None,
    ),
    "latest_mode_slices_latest": (
        {"mode": "latest", "change_data_capture": "nocdc"},
        _ID,
        {"slice": "latest"},
        (),
        None,
    ),
    "non_stream_update_mode_slices_update": (
        {"mode": "update", "change_data_capture": "nocdc", "stream": False},
        _ID,
        {"slice": "update"},
        (),
        None,
    ),
    "scd2_always_corrects_valid_from": (
        {"mode": "update", "change_data_capture": "scd2"},
        ["id", "__key"],
        {"correct_valid_from": True},
        (),
        True,
    ),
    "excludes_operation_when_present_in_columns": (
        {"mode": "update", "change_data_capture": "scd1"},
        ["id", "__key", "__operation"],
        {"exclude": ["__operation"]},
        (),
        True,
    ),
    "nocdc_always_excludes_operation": (
        {"mode": "update", "change_data_capture": "nocdc"},
        _ID,
        {"exclude": ["__operation"]},
        (),
        None,
    ),
}


@pytest.mark.parametrize(
    ("job_options", "columns", "expected", "absent", "probe_empty"), _SILVER_CASES.values(), ids=_SILVER_CASES.keys()
)
def test_silver_build_cdc_context(job_options, columns, expected, absent, probe_empty):
    job = _silver_job(**job_options)
    if probe_empty is not None:
        job.spark.sql.return_value.isEmpty.return_value = probe_empty

    context = job.build_cdc_context(_FakeDF(columns=columns))

    _assert_context(context, expected, absent)


def test_silver_reload_probe_also_matches_truncate():
    # https://github.com/fabricks-framework/fabricks/issues/66: a 'truncate'
    # sentinel row is rewritten to 'reload' inside the CDC query template
    # (fabricks/cdc/templates/ctes/base.sql.jinja), but this probe runs
    # against the raw incoming batch *before* that template renders -- it
    # must recognize 'truncate' directly, or rectify never turns on and the
    # truncate row is never reconciled.
    job = _silver_job(mode="update", change_data_capture="scd1", stream=True)
    job.spark.sql.return_value.isEmpty.return_value = False

    job.build_cdc_context(_FakeDF(columns=["id", "__key"]))

    rendered_sql = job.spark.sql.call_args[0][0]
    assert "truncate" in rendered_sql


def test_silver_rectify_stays_false_for_nocdc_without_probing():
    # nocdc -> "not nocdc" is False, so the reload probe never runs (and
    # spark.sql is never called for it).
    job = _silver_job(mode="update", change_data_capture="nocdc")
    job.spark.sql.reset_mock()

    context = job.build_cdc_context(_FakeDF(columns=["id"]))

    assert context["rectify"] is False
