import ast
from pathlib import Path


PROCESSOR_FILE = (
    Path(__file__).resolve().parents[4] / "fabricks" / "core" / "dags" / "processor.py"
)


def _get_process_function() -> ast.FunctionDef:
    tree = ast.parse(PROCESSOR_FILE.read_text())
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "DagProcessor":
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == "process":
                    return item
    raise AssertionError("DagProcessor.process() not found")


def test_process_uses_process_method_reference_for_target():
    process_fn = _get_process_function()

    for node in ast.walk(process_fn):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Process":
            target = next((kw.value for kw in node.keywords if kw.arg == "target"), None)
            assert isinstance(target, ast.Attribute), "Process target should be method reference, not function call"
            assert isinstance(target.value, ast.Name) and target.value.id == "self"
            assert target.attr == "_process"
            return

    raise AssertionError("Process(...) call not found in DagProcessor.process()")


def test_process_handles_queue_resource_not_found_on_delete():
    process_fn = _get_process_function()

    for node in ast.walk(process_fn):
        if isinstance(node, ast.Try):
            has_queue_delete = False
            for stmt in node.body:
                if isinstance(stmt, ast.With):
                    for with_stmt in stmt.body:
                        if (
                            isinstance(with_stmt, ast.Expr)
                            and isinstance(with_stmt.value, ast.Call)
                            and isinstance(with_stmt.value.func, ast.Attribute)
                            and with_stmt.value.func.attr == "delete"
                        ):
                            has_queue_delete = True
                            break

            catches_not_found = any(
                isinstance(handler.type, ast.Name) and handler.type.id == "ResourceNotFoundError"
                for handler in node.handlers
            )
            if has_queue_delete and catches_not_found:
                return

    raise AssertionError("Queue delete should be wrapped with ResourceNotFoundError handling")
