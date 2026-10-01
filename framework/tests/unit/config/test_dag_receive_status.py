from tests.unit.config._dag import edge, incoming_edges, job_status, receive


def test_receive_records_stale_status_on_a_real_exception_and_propagates_to_dependency_edges(semblance):
    receive(semblance, incoming=["ok"], run_raises=Exception("boom"))

    assert job_status(semblance) == "stale"
    assert edge(semblance, "child-1")["Status"] == "stale"


def test_receive_records_stale_status_and_updates_dependency_edges_not_delete(semblance):
    receive(semblance, incoming=["ok"], run_result="stale")

    assert job_status(semblance) == "stale"
    # the outgoing edge this job just wrote must be updated in place, not deleted
    assert edge(semblance, "child-1")["Status"] == "stale"


def test_receive_records_ok_status_on_a_successful_run_and_propagates_to_dependency_edges(semblance):
    # Control test, paired with the "stale" ones above: without it, a bug that always writes "stale"
    # regardless of the real outcome would pass every other test in this file.
    receive(semblance, incoming=["ok"], run_result="ok")

    assert job_status(semblance) == "ok"
    assert edge(semblance, "child-1")["Status"] == "ok"


def test_receive_only_rewrites_edges_that_name_this_job_as_parent(semblance):
    receive(semblance, incoming=["ok"], run_result="ok")

    assert edge(semblance, "child-2")["Status"] == "pending"


def test_receive_deletes_its_own_incoming_edges(semblance):
    receive(semblance, incoming=["ok"], run_result="ok")

    assert incoming_edges(semblance) == []
