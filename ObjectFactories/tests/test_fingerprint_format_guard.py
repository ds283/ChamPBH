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

"""Guard the fingerprint format against spec changes (rrk-adoption prompt 02; campaign README §0.3
and §6.2, P8 and P9; RunRegistryKit's docs/adoption/README.md §6, "The Q5 guard").

`config.fingerprint.FINGERPRINT_FORMAT` is a number the author bumps when what the inventory
records changes. Nothing makes the bump happen, and the golden fingerprint says only that the data
changed, not that the format was forgotten. This module pins, for each format that has a pin
(`config.fingerprint.SPEC_DIGESTS`), the SHA-256 of the canonical JSON of every
`inventory_spec()` in the registry, in registry order, as `[class, digest]` pairs: 21 of them, one
per registered class with a spec (17 `inventory_spec` definitions serve 21 classes, as
`test_inventory_specs.test_seventeen_definitions_serve_twenty_one_classes` says). A change to any
spec then fails `TestFormatGuard` naming the class, and says to bump the format.

The functions are the recipe of RunRegistryKit's `docs/adoption/README.md` §6 (nothing of the guard
is shipped by the package; it is this repository's test). `plain` writes every mapping key as its
own JSON, so that the polymorphic `Parent.types` keys of `ScalarModel`, which are `int` (the ids of
the three `model_ids` modules), stay apart from the string `"0"`: `canonical_json` refuses a key
that is not a string, and a bare `str(k)` would make the two alike and miss a change of a key's
type. `test_a_key_keeps_its_type` pins that conversion, with stand-in `Parent`s.

Nothing is opened: no store, nothing under `var/`, no Ray.
"""

import dataclasses
import hashlib
import json
import unittest
from collections.abc import Mapping

from datastorekit.store_inventory import Parent, canonical_json, inventory_specs

from config.datastore import factories
from config.fingerprint import FINGERPRINT_FORMAT, SPEC_DIGESTS


def plain(value):
    """A spec as JSON: dataclasses as objects, sequences as lists, and every mapping key as the
    JSON of the key, so that 0 (`0`) and "0" (`"0"`) stay apart."""
    if dataclasses.is_dataclass(value):
        return {
            f.name: plain(getattr(value, f.name)) for f in dataclasses.fields(value)
        }
    if isinstance(value, Mapping):
        return {json.dumps(k): plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    return value


def spec_digests(factories) -> list:
    """[class, SHA-256 of its spec's canonical JSON] for each spec, in registry order (the order
    of `inventory_specs`, which is the registry's)."""
    return [
        [name, hashlib.sha256(canonical_json(plain(spec)).encode()).hexdigest()]
        for name, spec in inventory_specs(factories).items()
    ]


class TestFormatGuard(unittest.TestCase):
    def test_every_spec_is_pinned_for_the_current_format(self):
        if FINGERPRINT_FORMAT not in SPEC_DIGESTS:
            self.fail(
                f"FINGERPRINT_FORMAT is {FINGERPRINT_FORMAT} but config.fingerprint.SPEC_DIGESTS "
                f"has no pin for it (it has {sorted(SPEC_DIGESTS)}): the format was bumped "
                "without a pin. Generate one with spec_digests(factories) from this module and "
                "add it beside the old ones, never over them, with a new golden file."
            )
        pinned = SPEC_DIGESTS[FINGERPRINT_FORMAT]
        current = spec_digests(factories)
        if current == pinned:
            return

        pinned_by_name = dict(pinned)
        current_by_name = dict(current)
        changed = [
            n
            for n in current_by_name
            if n in pinned_by_name and current_by_name[n] != pinned_by_name[n]
        ]
        added = [n for n in current_by_name if n not in pinned_by_name]
        removed = [n for n in pinned_by_name if n not in current_by_name]
        lines = [
            f"the inventory specs no longer match the pin for format {FINGERPRINT_FORMAT}:"
        ]
        lines += [f"  spec changed: {n}" for n in changed]
        lines += [f"  class added to the registry with a spec: {n}" for n in added]
        lines += [
            f"  class removed from the registry or its spec: {n}" for n in removed
        ]
        if not (changed or added or removed):
            lines.append(
                "  the classes and digests agree, but the registry order differs"
            )
        lines.append(
            "A spec changed, so bump FINGERPRINT_FORMAT in config/fingerprint.py and add a new "
            "pin to SPEC_DIGESTS and a new golden file beside the old ones, never over them."
        )
        self.fail("\n".join(lines))

    def test_the_pin_has_one_pair_per_class_with_a_spec(self):
        self.assertIn(FINGERPRINT_FORMAT, SPEC_DIGESTS)
        pinned = SPEC_DIGESTS[FINGERPRINT_FORMAT]
        self.assertEqual(len(pinned), 21)
        self.assertEqual([n for n, _ in pinned], list(inventory_specs(factories)))

    def test_a_key_keeps_its_type(self):
        self.assertNotEqual(plain({0: "a"}), plain({"0": "a"}))
        self.assertNotEqual(
            canonical_json(plain({0: "a"})), canonical_json(plain({"0": "a"}))
        )

    def test_a_parent_keyed_by_an_int_keeps_its_type(self):
        """The stand-in parents are those of `ScalarModel`'s coupling, keyed by the int 0 and by
        the string "0": their digests differ, and `canonical_json` refuses the int-keyed one
        unconverted, which is why `plain` exists."""
        by_int = Parent(
            "coupling_serial",
            type_column="coupling_type",
            types={0: "ExponentialCoupling"},
        )
        by_str = Parent(
            "coupling_serial",
            type_column="coupling_type",
            types={"0": "ExponentialCoupling"},
        )

        def digest(parent):
            return hashlib.sha256(canonical_json(plain(parent)).encode()).hexdigest()

        self.assertNotEqual(digest(by_int), digest(by_str))
        with self.assertRaises(TypeError):
            canonical_json(dataclasses.asdict(by_int))
        canonical_json(dataclasses.asdict(by_str))


if __name__ == "__main__":
    unittest.main()
