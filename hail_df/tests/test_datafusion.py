"""`hail_df.datafusion` is the one module the package imports DataFusion through.

The import check reads the package's source files, which is unavoidable: what a module imports is
in its source, and importing it shows only what it binds.
"""

import ast
from pathlib import Path

import hail_df
import hail_df.datafusion

PACKAGE = Path(hail_df.__file__).parent


def test_no_other_module_of_the_package_imports_datafusion():
    importers = []
    for path in sorted(PACKAGE.rglob("*.py")):
        if path.name == "datafusion.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                modules = [node.module or ""]
            else:
                continue
            if any(module.split(".")[0] == "datafusion" for module in modules):
                importers.append(path.name)
    assert importers == []


def test_it_exports_the_dataframe_and_session_types_of_the_datafusion_wheel():
    import datafusion

    assert hail_df.datafusion.DataFrame is datafusion.DataFrame
    assert hail_df.datafusion.SessionContext is datafusion.SessionContext
