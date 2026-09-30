import inspect

import pytest

from tests.semblance.dbutils_fake import DbutilsState, FakeDbutils, NotebookCall, NotebookExit
from tests.semblance.stub import stub_signature


@pytest.fixture
def state(tmp_path):
    return DbutilsState(fs_root=tmp_path)


@pytest.fixture
def dbutils(state):
    return FakeDbutils(state)


def _public_callables(obj, prefix=""):
    for name in dir(obj):
        if name.startswith("_"):
            continue
        value = getattr(obj, name)
        path = f"{prefix}{name}"
        if callable(value):
            yield path, value
        else:
            yield from _public_callables(value, f"{path}.")


def test_every_fake_method_conforms_to_the_sdk_stub(dbutils):
    checked = dict(_public_callables(dbutils))
    assert checked, "expected the fake to expose methods"
    for path, fn in checked.items():
        assert getattr(fn, "stub_path", None) == path, f"{path} is not declared with @conforms_to"
        fake_params = list(inspect.signature(fn.__wrapped__).parameters)[1:]  # drop self
        assert fake_params == list(stub_signature(path).parameters), path


def test_a_wrong_argument_name_fails_at_the_fake(dbutils):
    with pytest.raises(TypeError):
        dbutils.widgets.get(nme="x")


def test_unknown_attribute_raises(dbutils):
    with pytest.raises(AttributeError):
        dbutils.credentials  # noqa: B018


def test_widgets_get_and_text(dbutils, state):
    with pytest.raises(ValueError, match="schedule"):
        dbutils.widgets.get("schedule")
    dbutils.widgets.text("schedule", "daily")
    assert dbutils.widgets.get("schedule") == "daily"
    state.widgets["schedule"] = "hourly"
    dbutils.widgets.text("schedule", "daily")  # does not overwrite an existing value
    assert dbutils.widgets.get("schedule") == "hourly"


def test_secrets(dbutils, state):
    state.secrets[("scope-a", "k")] = "v"
    assert dbutils.secrets.get("scope-a", "k") == "v"
    assert [s.name for s in dbutils.secrets.listScopes()] == ["scope-a"]
    with pytest.raises(ValueError, match="missing"):
        dbutils.secrets.get("scope-a", "missing")


def test_task_values_raise_type_error_when_unset_like_real_dbutils(dbutils):
    with pytest.raises(TypeError):
        dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
    dbutils.jobs.taskValues.set(key="schedule_id", value="s1")
    assert dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id") == "s1"
    assert dbutils.jobs.taskValues.get(taskKey="initialize", key="nope", debugValue="d") == "d"


def test_fs_ls_rm_are_sandboxed_to_fs_root(dbutils, tmp_path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "sub").mkdir()
    listed = {i.name: i for i in dbutils.fs.ls(str(tmp_path))}
    assert set(listed) == {"a.txt", "sub/"}
    assert listed["sub/"].isDir()
    assert not listed["a.txt"].isDir()

    assert dbutils.fs.rm(str(tmp_path / "sub"), recurse=True) is True
    assert not (tmp_path / "sub").exists()
    with pytest.raises(PermissionError):
        dbutils.fs.rm("/", recurse=True)
    with pytest.raises(FileNotFoundError):
        dbutils.fs.ls(str(tmp_path / "missing"))


def test_notebook_run_is_scripted_and_recorded(dbutils, state):
    with pytest.raises(AssertionError, match=r"unexpected dbutils\.notebook\.run"):
        dbutils.notebook.run("/n", 60, {})

    state.notebook_results.append((None, "success"))
    assert dbutils.notebook.run(path="/n", timeout_seconds=60, arguments={"a": "1"}) == "success"
    assert state.notebook_calls[-1] == NotebookCall("/n", 60, {"a": "1"})


def test_notebook_exit_raises_carrying_the_value(dbutils):
    with pytest.raises(NotebookExit) as exc:
        dbutils.notebook.exit("done")
    assert exc.value.value == "done"
