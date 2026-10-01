"""hash.sql.jinja renders the field list in the order given, so reordering fields changes __key and __hash."""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

import fabricks

# not PackageLoader("fabricks.cdc", ...): importing fabricks.cdc needs the real fabricks.context (replaced here)
_TEMPLATES = Path(fabricks.__file__).parent / "cdc" / "templates"


def _macros():
    return Environment(loader=FileSystemLoader(_TEMPLATES)).get_template("macros/hash.sql.jinja").make_module()


def test_add_key_field_order_is_significant():
    # If this became order-independent, every already-materialized __key in production would change with it.
    macros = _macros()

    assert macros.add_key(["id", "name"]) != macros.add_key(["name", "id"])


def test_add_key_renders_each_field_once_in_the_given_order():
    rendered = " ".join(_macros().add_key(["id", "name"]).split())

    assert rendered.index("`id`") < rendered.index("`name`")
    assert rendered.count("`id`") == 1
    assert rendered.count("`name`") == 1
