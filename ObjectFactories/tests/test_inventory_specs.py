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

"""
The inventory's declarations: each factory's ``inventory_spec()`` (dk-and-rpk-migration prompt 09).

No Ray cluster and no store is needed: importing ``config.datastore`` builds the factories, and the
tables are the ones ``datastorekit.SQL.schema.build_schema`` builds from them, in memory.

- the package's ``inventory_classes`` accepts ChamPBH's registry and orders it (test 1);
- every column a spec names is a column of the table the package builds, and every class a spec
  references is registered and declares a spec (test 2);
- **coverage** (campaign README §6.5, the user's ruling): every registered class declares a spec,
  or is a tag or value table read through an owner's spec, named here with its owner and slot, or
  is excluded here by name with a reason. The fingerprint of RunRegistryKit, which ChamPBH adopted
  in `rrk-adoption` (`config/fingerprint.py`), reads only what the specs declare and passes over
  the rest silently, so a class in none of the three would go unfingerprinted (test 6).
"""

import unittest
from typing import Dict, List, Mapping, Tuple

import ray
import sqlalchemy as sqla
from datastorekit.contract import TAG_LABEL, TAG_SERIAL, TAG_TABLE
from datastorekit.SQL.schema import build_schema
from datastorekit.store_inventory import InventorySpec, inventory_classes

import CosmologyConcepts.ConformalCouplings.model_ids as coupling_ids
import CosmologyConcepts.Potentials.model_ids as potential_ids
import CosmologyModels.model_ids as cosmology_ids
from config.datastore import factories
from config.sharding import replicated_tables, sharded_tables

# inventory_classes' order, as derived from the specs: the roots in registry order, then each
# class once every class it references has been placed
EXPECTED_ORDER = [
    "version",
    "store_tag",
    "redshift",
    "tolerance",
    "beta_value",
    "M_value",
    "Lambda_value",
    "temperature",
    "phi_value",
    "pi_value",
    "QCD_Cosmology",
    "IntegrationSolver",
    "ExponentialPotential",
    "ExponentialCoupling",
    "ScalarModel",
    "AdiabaticHistory",
    "BBNData",
]

# I5: the registered classes that declare no spec of their own, each read through an owner's spec,
# by (owner, slot). The test checks that the owner's spec names the table in that slot
READ_THROUGH: Dict[str, Tuple[str, str]] = {
    "ScalarModel_tags": ("ScalarModel", "tags"),
    "ScalarModelValue": ("ScalarModel", "values"),
    "AdiabaticHistory_tags": ("AdiabaticHistory", "tags"),
    "AdiabaticHistoryValue": ("AdiabaticHistory", "values"),
    "BBNData_tags": ("BBNData", "tags"),
    "BBNDataValue": ("BBNData", "values"),
}

# I5: the registered classes that are in no spec, each with a one-line reason. Empty: every class
# that holds state is covered (README §6.5, measured at 12c82e8 and again at prompt 09)
EXCLUDED: Dict[str, str] = {}

SLOTS = ("tags", "values", "parent_sets")


def _spec(factory):
    hook = getattr(factory, "inventory_spec", None)
    return hook() if hook is not None else None


def _slot_tables(spec: InventorySpec, slot: str) -> List[str]:
    if slot == "parent_sets":
        return [p.table for p in spec.parent_sets.values()]
    value = getattr(spec, slot)
    return [] if value is None else [value[0]]


def coverage_failures(
    registry: Mapping,
    read_through: Mapping[str, Tuple[str, str]],
    excluded: Mapping[str, str],
) -> List[str]:
    """
    Every way ``registry`` fails I5: a class in none of the three places, or in more than one; a
    read-through entry whose owner does not name the table in that slot, or that names an
    unregistered class or owner; an exclusion naming a class no longer registered, or with no
    reason. Empty when the registry is covered.
    """
    failures = []
    for name in list(read_through) + list(excluded):
        if name not in registry:
            failures.append(f"{name}: named in the test but no longer registered")
    for name, reason in excluded.items():
        if not isinstance(reason, str) or reason.strip() == "":
            failures.append(f"{name}: excluded with no reason")

    for name, factory in registry.items():
        spec = _spec(factory)
        places = []
        if spec is not None:
            if not isinstance(spec, InventorySpec):
                failures.append(f"{name}: inventory_spec() returned {spec!r}")
            places.append("declares a spec")
        if name in read_through:
            places.append("read through an owner's spec")
            owner, slot = read_through[name]
            owner_spec = _spec(registry[owner]) if owner in registry else None
            if slot not in SLOTS:
                failures.append(f"{name}: read through an unknown slot {slot!r}")
            elif owner_spec is None:
                failures.append(f"{name}: its owner {owner} declares no spec")
            elif name not in _slot_tables(owner_spec, slot):
                failures.append(f"{name}: {owner}'s spec does not name it in {slot}")
        if name in excluded:
            places.append("excluded")
        if len(places) == 0:
            failures.append(
                f"{name}: declares no inventory_spec, is read through no owner's spec, and is "
                f"not excluded"
            )
        elif len(places) > 1:
            failures.append(f"{name}: in more than one place ({', '.join(places)})")
    return failures


class _NoRay(unittest.TestCase):
    def tearDown(self):
        self.assertFalse(ray.is_initialized())


class TestInventoryClasses(_NoRay):
    """Test 1: the package's ``inventory_classes`` accepts the registry, and orders it."""

    def test_the_registry_is_accepted_and_ordered(self):
        self.assertEqual(inventory_classes(factories), EXPECTED_ORDER)

    def test_thirteen_definitions_serve_seventeen_classes(self):
        owners = [name for name, f in factories.items() if _spec(f) is not None]
        self.assertEqual(len(owners), 17)
        self.assertEqual(len({type(factories[n]) for n in owners}), 13)
        # the six quantity entries share two factory classes, each reading its own value column
        quantities = ["beta_value", "M_value", "Lambda_value", "temperature"]
        quantities += ["phi_value", "pi_value"]
        self.assertEqual(
            {n: _spec(factories[n]).leaves for n in quantities},
            {
                "beta_value": ("value",),
                "M_value": ("value_eV",),
                "Lambda_value": ("value_eV",),
                "temperature": ("value_GeV",),
                "phi_value": ("value_PlanckMass",),
                "pi_value": ("value_PlanckMass",),
            },
        )


class TestSpecsResolve(_NoRay):
    """Test 2: every column a spec names is in the table ``build_schema`` builds, and every class
    a spec references is registered and declares a spec."""

    def setUp(self):
        self.tables = build_schema(sqla.MetaData(), factories).tables
        self.specs = {n: _spec(f) for n, f in factories.items() if _spec(f) is not None}

    def test_every_named_column_is_a_column_of_its_table(self):
        for name, spec in self.specs.items():
            table = self.tables[name]
            with self.subTest(cls=name):
                self.assertIn("serial", table.c)
                for leaf in spec.leaves:
                    self.assertIn(leaf, table.c, f"{name}: leaf {leaf}")
                for field, parent in spec.parents.items():
                    self.assertIn(parent.column, table.c, f"{name}.{field}")
                    if parent.type_column is not None:
                        self.assertIn(parent.type_column, table.c, f"{name}.{field}")
                        self.assertIn(
                            parent.type_column, spec.leaves, f"{name}.{field}"
                        )
                if spec.validated is not None:
                    self.assertIn(spec.validated, table.c, f"{name}: validated")
                if spec.tags is not None:
                    tag_table, column = spec.tags
                    self.assertIn(column, self.tables[tag_table].c, f"{name}: tags")
                    self.assertIn(TAG_SERIAL, self.tables[tag_table].c, f"{name}: tags")
                if spec.values is not None:
                    value_table, column = spec.values
                    self.assertIn(column, self.tables[value_table].c, f"{name}: values")
                self.assertEqual(dict(spec.parent_sets), {}, f"{name}: parent sets")
        # a tag's label is read from store_tag's key (contract §5)
        self.assertIn(TAG_LABEL, self.specs[TAG_TABLE].leaves)

    def test_every_referenced_class_is_registered_and_declares_a_spec(self):
        for name, spec in self.specs.items():
            for referenced in spec.dependencies():
                with self.subTest(cls=name, references=referenced):
                    self.assertIn(referenced, factories)
                    self.assertIn(referenced, self.specs)

    def test_the_polymorphic_maps_are_the_model_ids(self):
        parents = self.specs["ScalarModel"].parents
        self.assertEqual(
            dict(parents["cosmology"].types),
            {cosmology_ids.XAV_IMPROVED_EOS_IDENTIFIER: "QCD_Cosmology"},
        )
        self.assertEqual(
            dict(parents["potential"].types),
            {
                potential_ids.EXPONENTIAL_POTENTIAL: "ExponentialPotential",
            },
        )
        self.assertEqual(
            dict(parents["coupling"].types),
            {coupling_ids.EXPONENTIAL_COUPLING: "ExponentialCoupling"},
        )

    def test_no_parent_crosses_a_shard(self):
        # a sharded parent is named only by a sharded child, which takes its shard key from the same
        # beta (README §6.5; the log's spec table); every other parent is replicated
        for name, spec in self.specs.items():
            for field, parent in spec.parents.items():
                with self.subTest(cls=name, field=field):
                    self.assertFalse(parent.cross_shard)
                    for of in parent.classes():
                        if of in sharded_tables:
                            self.assertIn(name, sharded_tables)
                        else:
                            self.assertIn(of, replicated_tables)


class TestCoverage(_NoRay):
    """Test 6, I5: every registered class declares a spec, is read through an owner's spec, or is
    excluded with a reason (campaign README §6.5)."""

    def test_every_registered_class_is_covered(self):
        registry = dict(factories)
        self.assertEqual(coverage_failures(registry, READ_THROUGH, EXCLUDED), [])
        self.assertEqual(EXCLUDED, {})

    def test_a_class_in_none_of_the_three_is_named(self):
        registry = dict(factories)
        registry["Scratch"] = factories["ScalarModelValue"]
        self.assertEqual(
            coverage_failures(registry, READ_THROUGH, EXCLUDED),
            [
                "Scratch: declares no inventory_spec, is read through no owner's spec, and "
                "is not excluded"
            ],
        )

    def test_a_stale_exclusion_is_named(self):
        failures = coverage_failures(
            dict(factories), READ_THROUGH, {"LambdaCDM": "no factory stores it"}
        )
        self.assertEqual(
            failures, ["LambdaCDM: named in the test but no longer registered"]
        )

    def test_a_wrong_slot_is_named(self):
        read_through = dict(READ_THROUGH)
        read_through["ScalarModelValue"] = ("ScalarModel", "tags")
        self.assertEqual(
            coverage_failures(dict(factories), read_through, EXCLUDED),
            ["ScalarModelValue: ScalarModel's spec does not name it in tags"],
        )


if __name__ == "__main__":
    unittest.main()
