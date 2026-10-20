# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The data ChamPBH keeps in ``config/datastore.py`` (prompt 05 of dk-and-rpk-migration), and how
the drivers give it to datastorekit's pool (prompt 08).

No Ray cluster and no store is needed; importing ``config.datastore`` builds the factories. The
expected values below were captured from the datastore layer's own module-level objects at
prompt 04's tree, before ``config/datastore.py`` existed, not copied from it; prompt 08 removed
``LambdaCDM`` from the batch sizes (campaign README, P8).

One test runs a subprocess from the repository root: ``import config.datastore`` alone. It takes a
few seconds.
"""

import ast
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

import ray
import sqlalchemy as sqla

import config.datastore as config_datastore
from config.argument_parser import allowed_drop_actions
from config.sharding import replicated_tables, sharded_tables

REPO_ROOT = Path(__file__).resolve().parents[2]

EXPECTED_FACTORIES = [
    ("version", "sqla_version_factory", None),
    ("store_tag", "sqla_store_tag_factory", None),
    ("redshift", "sqla_redshift_factory", None),
    ("tolerance", "sqla_tolerance_factory", None),
    ("beta_value", "sqla_dimensionless_quantity_factory", "beta_value"),
    ("M_value", "sqla_dimensionful_quantity_factory", "M_value"),
    ("Lambda_value", "sqla_dimensionful_quantity_factory", "Lambda_value"),
    ("temperature", "sqla_dimensionful_quantity_factory", "temperature"),
    ("phi_value", "sqla_dimensionful_quantity_factory", "phi_value"),
    ("pi_value", "sqla_dimensionful_quantity_factory", "pi_value"),
    ("ExponentialPotential", "sqla_ExponentialPotential_factory", None),
    ("ExponentialCoupling", "sqla_ExponentialCoupling_factory", None),
    ("QCD_Cosmology", "sqla_QCDCosmology_factory", None),
    ("IntegrationSolver", "sqla_IntegrationSolver_factory", None),
    ("ScalarModel", "sqla_ScalarModelFactory", None),
    ("ScalarModel_tags", "sqla_ScalarModelTagAssociation_factory", None),
    ("ScalarModelValue", "sqla_ScalarModelValue_factory", None),
    ("AdiabaticHistory", "sqla_AdiabaticHistoryFactory", None),
    ("AdiabaticHistory_tags", "sqla_AdiabaticHistoryTagAssociation_factory", None),
    ("AdiabaticHistoryValue", "sqla_AdiabaticHistoryValue_factory", None),
    ("BBNData", "sqla_BBNDataFactory", None),
    ("BBNData_tags", "sqla_BBNDataTagAssociation_factory", None),
    ("BBNDataValue", "sqla_BBNDataValue_factory", None),
]

EXPECTED_DROP_GROUPS = {
    "scalar-model": ["ScalarModel_tags", "ScalarModelValue", "ScalarModel"],
    "adiabatic-history": [
        "AdiabaticHistory_tags",
        "AdiabaticHistoryValue",
        "AdiabaticHistory",
    ],
    "bbn-data": ["BBNData_tags", "BBNDataValue", "BBNData"],
}

EXPECTED_SERIAL_BATCH_SIZES = {
    "store_tag": 5,
    "redshift": 200,
    "tolerance": 5,
    "beta_value": 200,
    "M_value": 200,
    "Lambda_value": 200,
    "temperature": 200,
    "phi_value": 200,
    "pi_value": 200,
    "ExponentialCoupling": 50,
    "ExponentialPotential": 50,
    "QCD_Cosmology": 5,
    "IntegrationSolver": 6,
    "ScalarModel": 20,
    "ScalarModelValue": 250,
    "AdiabaticHistory": 20,
    "AdiabaticHistoryValue": 250,
    "BBNData": 20,
    "BBNDataValue": 250,
}


class TestConfigDatastoreData(unittest.TestCase):
    """``config.datastore`` holds exactly the data the layer held at prompt 04's tree, less
    ``LambdaCDM`` (P8), as the module docstring says."""

    def test_factories_registry(self):
        actual = []
        for name, factory in config_datastore.factories.items():
            object_type = getattr(factory, "ObjectType", None)
            actual.append(
                (
                    name,
                    type(factory).__name__,
                    None if object_type is None else object_type.__name__,
                )
            )
        self.assertEqual(actual, EXPECTED_FACTORIES)

    def test_drop_groups_literal_and_ordered(self):
        self.assertEqual(config_datastore.drop_groups, EXPECTED_DROP_GROUPS)
        self.assertEqual(list(config_datastore.drop_groups), list(EXPECTED_DROP_GROUPS))
        for group, tables in EXPECTED_DROP_GROUPS.items():
            self.assertEqual(config_datastore.drop_groups[group], tables)

    def test_serial_batch_sizes_literal_and_ordered(self):
        self.assertEqual(
            config_datastore.serial_batch_sizes, EXPECTED_SERIAL_BATCH_SIZES
        )
        self.assertEqual(
            list(config_datastore.serial_batch_sizes), list(EXPECTED_SERIAL_BATCH_SIZES)
        )


def _vectorized_shard_key_arguments(path: Path):
    """Return ``[(lineno, shard-key argument node or None)]`` for each object_get_vectorized call."""
    tree = ast.parse(path.read_text(), str(path))
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = (
            func.attr
            if isinstance(func, ast.Attribute)
            else func.id if isinstance(func, ast.Name) else None
        )
        if name != "object_get_vectorized":
            continue
        key = None
        if len(node.args) >= 2:
            key = node.args[1]
        else:
            for kw in node.keywords:
                if kw.arg == "shard_key":
                    key = kw.value
        found.append((node.lineno, key))
    return found


class TestDriversPassShardKeyMapping(unittest.TestCase):
    """Every vectorized get in the drivers passes ``{"shard_key": ...}``, never the bare key."""

    # plot_inputs.py holds the plot scripts' setup since read-only-plots prompt 02 (README P6)
    EXPECTED_COUNTS = {
        "main.py": 7,
        "plot_by_beta.py": 0,
        "plot_ScalarModel.py": 0,
        "plot_inputs.py": 0,
    }

    def test_vectorized_gets_pass_a_mapping(self):
        for driver, expected_count in self.EXPECTED_COUNTS.items():
            with self.subTest(driver=driver):
                calls = _vectorized_shard_key_arguments(REPO_ROOT / driver)
                self.assertEqual(len(calls), expected_count)
                for lineno, key in calls:
                    self.assertIsInstance(
                        key,
                        ast.Dict,
                        f"{driver}:{lineno}: shard key is not a dict literal",
                    )
                    self.assertEqual(
                        [
                            k.value if isinstance(k, ast.Constant) else None
                            for k in key.keys
                        ],
                        ["shard_key"],
                        f"{driver}:{lineno}: dict must have the single key 'shard_key'",
                    )


# ------------------------------------------------------------------------------------------------
# prompt 08: the drivers give config.datastore to datastorekit's pool
# ------------------------------------------------------------------------------------------------

DRIVERS = ("main.py", "plot_by_beta.py", "plot_ScalarModel.py")


class _NoRay(unittest.TestCase):
    def tearDown(self):
        self.assertFalse(ray.is_initialized())


class TestTablesAreDeclared(_NoRay):
    """Every table the pool is given is a class of the registry (the package's reader refuses a store
    that records a class the registry does not declare), and ``LambdaCDM``, which no factory
    registers, is in none of the lists (P8)."""

    def test_every_listed_table_is_registered_and_LambdaCDM_is_in_none(self):
        for name in list(replicated_tables) + list(sharded_tables):
            with self.subTest(table=name):
                self.assertIn(name, config_datastore.factories)
        self.assertNotIn("LambdaCDM", replicated_tables)
        self.assertNotIn("LambdaCDM", sharded_tables)
        self.assertNotIn("LambdaCDM", config_datastore.serial_batch_sizes)
        self.assertNotIn("LambdaCDM", config_datastore.factories)


def _names_bound(tree: ast.Module) -> dict:
    """Each name bound at any level of ``tree``, mapped to how: ("import", module) for each
    ``from module import name``, and "assign" for each assignment or other import."""
    bound = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                bound.setdefault(a.asname or a.name, []).append(("import", node.module))
        elif isinstance(node, ast.Import):
            for a in node.names:
                bound.setdefault(a.asname or a.name, []).append("assign")
        elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for t in targets:
                for n in ast.walk(t):
                    if isinstance(n, ast.Name):
                        bound.setdefault(n.id, []).append("assign")
    return bound


def _sharded_pool_calls(tree: ast.Module) -> list:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "ShardedPool"
    ]


class TestPoolsAreGivenTheConfig(_NoRay):
    """Each driver's one ``ShardedPool(...)`` passes ``factories=`` and ``serial_batch_sizes=``, each
    bound to the name it imports from ``config.datastore`` and binds nowhere else; ``main.py``'s
    passes ``drop_tables=``; none passes the old layer's ``drop_actions=`` or ``inventory_config=``.
    Read by ``ast``: the drivers start Ray when run."""

    def test_each_pool_is_given_config_datastore(self):
        found = 0
        for driver in DRIVERS:
            with self.subTest(driver=driver):
                tree = ast.parse((REPO_ROOT / driver).read_text(), driver)
                calls = _sharded_pool_calls(tree)
                self.assertEqual(len(calls), 1, f"{driver}: ShardedPool( calls")
                found += len(calls)
                keywords = {kw.arg: kw.value for kw in calls[0].keywords}
                bound = _names_bound(tree)

                for arg in ("factories", "serial_batch_sizes"):
                    value = keywords.get(arg)
                    self.assertIsInstance(
                        value, ast.Name, f"{driver}: {arg}= is not a bare name"
                    )
                    self.assertEqual(
                        bound.get(value.id),
                        [("import", "config.datastore")],
                        f"{driver}: {arg}={value.id} is not config.datastore's {arg}",
                    )
                    # imported under its own name, so it is that object and no other
                    self.assertEqual(value.id, arg)

                for arg in ("drop_actions", "inventory_config"):
                    self.assertNotIn(arg, keywords, f"{driver} passes {arg}=")

                if driver == "main.py":
                    self.assertIn("drop_tables", keywords)
                    self.assertIsInstance(keywords["drop_tables"], ast.Name)
                else:
                    self.assertNotIn("drop_tables", keywords)
        self.assertEqual(found, len(DRIVERS))


def _top_level_index(tree: ast.Module, predicate) -> list:
    """The indices of the module's top-level statements that contain a node satisfying
    ``predicate``."""
    return [
        i
        for i, stmt in enumerate(tree.body)
        if any(predicate(node) for node in ast.walk(stmt))
    ]


def _is_call_to(node, dotted: str) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if "." in dotted:
        owner, attr = dotted.split(".")
        return (
            isinstance(func, ast.Attribute)
            and func.attr == attr
            and isinstance(func.value, ast.Name)
            and func.value.id == owner
        )
    return isinstance(func, ast.Name) and func.id == dotted


def _main_tree() -> ast.Module:
    return ast.parse((REPO_ROOT / "main.py").read_text(), "main.py")


SCALAR_MODEL_ALONE = (
    "cannot drop scalar-model alone: AdiabaticHistory and BBNData name its rows; "
    "add `--drop adiabatic-history bbn-data`"
)


class TestDropPolicy(_NoRay):
    """U4: ``tables_to_drop`` refuses a ``--drop`` request that leaves rows naming dropped rows,
    naming the groups to add, and accepts every closed request; ``main.py`` calls it before Ray
    starts."""

    def test_scalar_model_alone_is_refused_naming_the_groups_to_add(self):
        with self.assertRaises(ValueError) as ctx:
            config_datastore.tables_to_drop(["scalar-model"])
        self.assertEqual(str(ctx.exception), SCALAR_MODEL_ALONE)

    def test_scalar_model_with_one_dependent_group_is_refused(self):
        for groups, message in (
            (
                ["scalar-model", "adiabatic-history"],
                "cannot drop scalar-model and adiabatic-history alone: BBNData names their "
                "rows; add `--drop bbn-data`",
            ),
            (
                ["scalar-model", "bbn-data"],
                "cannot drop scalar-model and bbn-data alone: AdiabaticHistory names their "
                "rows; add `--drop adiabatic-history`",
            ),
        ):
            with self.subTest(groups=groups):
                with self.assertRaises(ValueError) as ctx:
                    config_datastore.tables_to_drop(groups)
                self.assertEqual(str(ctx.exception), message)

    def test_each_closed_request_is_accepted(self):
        groups = config_datastore.drop_groups
        for request, expected in (
            ([], []),
            (["bbn-data"], groups["bbn-data"]),
            (["adiabatic-history"], groups["adiabatic-history"]),
            (
                ["adiabatic-history", "bbn-data"],
                groups["adiabatic-history"] + groups["bbn-data"],
            ),
            (
                ["bbn-data", "scalar-model", "adiabatic-history"],
                groups["scalar-model"]
                + groups["adiabatic-history"]
                + groups["bbn-data"],
            ),
        ):
            with self.subTest(request=request):
                self.assertEqual(config_datastore.tables_to_drop(request), expected)

    def test_an_unknown_group_is_refused(self):
        with self.assertRaises(ValueError) as ctx:
            config_datastore.tables_to_drop(["bbn-data", "no-such-group"])
        self.assertIn("no-such-group", str(ctx.exception))

    def test_a_dependent_in_no_group_is_named_as_a_table(self):
        """With a registry of the test's own, in which a table outside every group names
        ``ScalarModel``: the refusal names it as a table."""

        class _Orphan:
            def register(self):
                return {
                    "version": False,
                    "timestamp": False,
                    "columns": [
                        sqla.Column(
                            "model_serial",
                            sqla.Integer,
                            sqla.ForeignKey("ScalarModel.serial"),
                        )
                    ],
                }

        registry = dict(config_datastore.factories)
        registry["Orphan"] = _Orphan()
        with mock.patch.object(config_datastore, "factories", registry):
            with self.assertRaises(ValueError) as ctx:
                config_datastore.tables_to_drop(
                    ["scalar-model", "adiabatic-history", "bbn-data"]
                )
        self.assertEqual(
            str(ctx.exception),
            "cannot drop scalar-model, adiabatic-history and bbn-data alone: Orphan names "
            "their rows; add the table Orphan, which no drop group holds",
        )

    def test_the_command_line_choices_are_the_groups(self):
        self.assertEqual(allowed_drop_actions, list(config_datastore.drop_groups))

    def test_main_computes_the_drop_before_ray_starts(self):
        tree = _main_tree()
        drop = _top_level_index(tree, lambda n: _is_call_to(n, "tables_to_drop"))
        init = _top_level_index(tree, lambda n: _is_call_to(n, "ray.init"))
        self.assertEqual(len(drop), 1, "main.py: module-level tables_to_drop calls")
        self.assertEqual(len(init), 1, "main.py: module-level ray.init calls")
        self.assertLess(drop[0], init[0])


class TestConfigDatastoreImportsAlone(_NoRay):
    """The import cycle through the old layer is gone: ``config.datastore`` imports first, alone, in
    a fresh process (issue 05-config-datastore-cannot-be-imported-before-the-layer)."""

    def test_import_config_datastore_alone(self):
        result = subprocess.run(
            [sys.executable, "-c", "import config.datastore"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
