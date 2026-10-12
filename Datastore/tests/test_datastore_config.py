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

"""The data ChamPBH keeps in ``config/datastore.py`` (prompt 05 of dk-and-rpk-migration).

No Ray cluster and no store is needed; importing the layer builds the factories, as it always did.
The expected values below were captured from the datastore layer's own module-level objects at
prompt 04's tree, before ``config/datastore.py`` existed, not copied from it.
"""

import ast
import importlib
import unittest
from pathlib import Path

# The layer must be imported BEFORE config.datastore. Datastore/SQL/__init__.py imports the actor,
# and Datastore/SQL/Datastore.py imports config.datastore; a fresh ``import config.datastore``
# in a process that has not yet imported the layer fails with a circular-import ImportError
# (issue 05-config-datastore-cannot-be-imported-before-the-layer; prompt 08 removes the cycle).
import Datastore.SQL.ClientPool as client_pool_module  # noqa: E402
from Datastore.SQL.Datastore import Datastore as _actor  # noqa: E402,F401

import config.datastore as config_datastore  # noqa: E402

# ``import Datastore.SQL.Datastore`` binds the actor, not the module, because Datastore/SQL/__init__.py
# re-exports the actor; the module itself is found by name.
layer_module = importlib.import_module("Datastore.SQL.Datastore")

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
    ("InversePowerPotential", "sqla_InversePowerPotential_factory", None),
    ("StarobinskyPotential", "sqla_StarobinskyPotential_factory", None),
    ("ExponentialPotential", "sqla_ExponentialPotential_factory", None),
    ("ReclinerPotential", "sqla_ReclinerPotential_factory", None),
    ("ReflectingPotential", "sqla_ReflectingPotential_factory", None),
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
    "InversePowerPotential": 50,
    "ExponentialPotential": 50,
    "ReflectingPotential": 50,
    "StarobinskyPotential": 50,
    "ReclinerPotential": 50,
    "LambdaCDM": 5,
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
    """``config.datastore`` holds exactly the data the layer held at prompt 04's tree."""

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


class TestLayerReadsConfig(unittest.TestCase):
    """The old layer's names are the very objects of ``config.datastore``, not copies of them."""

    def test_layer_registry_is_config_registry(self):
        self.assertIs(layer_module._factories, config_datastore.factories)

    def test_layer_drop_actions_is_config_drop_groups(self):
        self.assertIs(layer_module._drop_actions, config_datastore.drop_groups)

    def test_client_pool_batch_sizes_is_config_serial_batch_sizes(self):
        self.assertIs(
            client_pool_module._default_serial_batch_size,
            config_datastore.serial_batch_sizes,
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

    EXPECTED_COUNTS = {"main.py": 7, "plot_by_beta.py": 0, "plot_ScalarModel.py": 0}

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


if __name__ == "__main__":
    unittest.main()
