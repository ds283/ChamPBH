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
The datastore inventory report: ``main.py --inventory`` (dk-and-rpk-migration prompt 09).

``format_inventory_report(inventory, db_name, verbose=False)`` renders the ``StoreInventory`` that
``datastorekit.store_inventory.read_inventory`` returns. It reads nothing from a store and opens no
file: whatever it prints is in the inventory it is given, which was read read-only, with no Ray.
Which key leaves are floats it learns from the schema the registry builds (``build_schema`` over
``config.datastore.factories``, column types only), as SecondaryGWKit's report of the same name
does; that opens nothing either. It is modelled on SecondaryGWKit's ``tools/inventory_report.py``,
read in that repository, and written for ChamPBH's classes; nothing is imported from it.

**What it shows.** A title naming ``db_name`` as given; the shards; the number of records and
classes; the number of named problems ("none" when there are none). Then, category by category,
every class of the inventory (a class no category names is shown under "Other classes"):

- a header: its record count, whether it is replicated or sharded, its validated / unvalidated split
  and its value rows where it has them, and the class's timestamp range. The inventory carries one
  range per class, not one per validated group, so the report gives the class's;
- for a class whose key is one leaf (a redshift, a tolerance, a parameter value, a version or a
  tag label), its values, sorted: a float to five significant figures (``.5g``), with the unit its
  stored column is in (``value_<unit>``: the stored column already holds the value in the class's
  default unit, so no units object is needed);
- for any other class, its records, each by its physical labels: a parent by the parent's own key,
  never by a digest or a serial;
- for ``ScalarModel``, ``AdiabaticHistory`` and ``BBNData``, the validated and unvalidated groups,
  each with its count and version label(s), and its records by their physical labels: ``T_init``
  and ``T_stop`` (GeV), ``phiE_init`` and ``piE_init`` (Planck mass), the potential (its class, M,
  Lambda and n), the coupling (its class and beta), ``failure``, and for ``BBNData`` also its own
  ``failure`` and ``PRyM_version``; an ``AdiabaticHistory`` or ``BBNData`` record is labelled by its
  model's labels. Each record shows its tags and its value-row count. A key field that is the same
  for every model (the cosmology and the two tolerances, usually) is printed once, on the
  ``ScalarModel`` class's "common to every model" line, and otherwise on each record's line, so
  that two different records never render as one line except where their floats agree to five
  figures;
- every named problem, **whole**, under its class. Problems are never truncated.

A list of values or records of 20 or more shows its first and last ten, unless ``verbose``
(``main.py --show-all``), which shows every one. The stored ``label`` of a compute target (its job
name and time) is not in the inventory, which holds only what a class's ``inventory_spec`` declares,
and is not shown.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import sqlalchemy as sqla

from datastorekit.store_inventory import ClassInventory, Record, StoreInventory

# categories are reported in this order; a class of the inventory that no category names is
# reported under "Other classes", so that nothing the inventory holds can go unprinted
INVENTORY_CATEGORIES: List[Tuple[str, List[str]]] = [
    ("Datastore metadata", ["version", "store_tag"]),
    (
        "Redshifts, tolerances and parameter values",
        [
            "redshift",
            "tolerance",
            "beta_value",
            "temperature",
            "M_value",
            "Lambda_value",
            "phi_value",
            "pi_value",
        ],
    ),
    ("Cosmologies and solvers", ["QCD_Cosmology", "IntegrationSolver"]),
    (
        "Potentials and couplings",
        [
            "InversePowerPotential",
            "StarobinskyPotential",
            "ExponentialPotential",
            "ReclinerPotential",
            "ReflectingPotential",
            "ExponentialCoupling",
        ],
    ),
    ("Compute targets", ["ScalarModel", "AdiabaticHistory", "BBNData"]),
]

# the value table each class's value_count counts: the table its factory's inventory_spec names in
# ``values=``. Used only to name the table on the header
VALUE_TABLES: Dict[str, str] = {
    "ScalarModel": "ScalarModelValue",
    "AdiabaticHistory": "AdiabaticHistoryValue",
    "BBNData": "BBNDataValue",
}

# the three compute targets, whose records are shown by validated group and by their model's labels
COMPUTE_TARGETS = ("ScalarModel", "AdiabaticHistory", "BBNData")

# a list of this many values or records or more is shown by its first and last LIST_ENDS, unless
# verbose
LIST_LIMIT = 20
LIST_ENDS = 10

# a ScalarModel's key fields as the report labels them, in the order it prints them
MODEL_FIELDS: List[Tuple[str, str]] = [
    ("T_init", "T_init"),
    ("T_stop", "T_stop"),
    ("phi_init", "phiE_init"),
    ("pi_init", "piE_init"),
    ("potential", "potential"),
    ("coupling", "coupling"),
    ("failure", "failure"),
]

# the rest of a ScalarModel's key, printed once when every model shares it (the version is shown by
# validated group in ScalarModel's own section). The *_type leaves are not printed: each is the
# class its polymorphic parent is printed with
MODEL_EXTRA_FIELDS: List[Tuple[str, str]] = [
    ("cosmology", "cosmology"),
    ("atol", "log10_atol"),
    ("rtol", "log10_rtol"),
    ("version", "version"),
]
MODEL_TYPE_LEAVES = ("cosmology_type", "potential_type", "coupling_type")

_CLASS_INDENT = "      "
_DETAIL_INDENT = "         "
_RECORD_INDENT = "            "


def _format_timestamp(value: Optional[datetime]) -> str:
    if value is None:
        return "?"
    return value.strftime("%a %d %b %Y %H:%M:%S")


def _plural(n: int, word: str) -> str:
    if n == 1:
        return f"{n:,} {word}"
    return f"{n:,} {word}" + ("es" if word.endswith("s") else "s")


def _float_columns() -> Dict[str, frozenset]:
    """Each table of the registry, mapped to the names of its columns the schema types as floats."""
    from datastorekit.SQL.schema import build_schema
    from config.datastore import factories

    tables = build_schema(sqla.MetaData(), factories).tables
    return {
        name: frozenset(c.name for c in table.columns if isinstance(c.type, sqla.Float))
        for name, table in tables.items()
    }


def _truncated(items: Sequence[str], verbose: bool, what: str) -> List[str]:
    """``items`` whole when ``verbose`` or fewer than ``LIST_LIMIT``; otherwise the first and last
    ``LIST_ENDS``, with a line saying how many are not shown."""
    if verbose or len(items) < LIST_LIMIT:
        return list(items)
    hidden = len(items) - 2 * LIST_ENDS
    return (
        list(items[:LIST_ENDS])
        + [f"... {_plural(hidden, what)} not shown (--show-all shows every one) ..."]
        + list(items[-LIST_ENDS:])
    )


class _Renderer:
    """Renders the records of one ``StoreInventory`` by their physical labels."""

    def __init__(self, inventory: StoreInventory, verbose: bool):
        self.inventory = inventory
        self.verbose = verbose
        self.floats = _float_columns()
        # class -> reference digest -> the records it names, built once per class
        self._index: Dict[str, Dict[str, List[Record]]] = {}

    def cls(self, name: str) -> ClassInventory:
        return self.inventory[name]

    # --- parents -------------------------------------------------------------------------------

    def find(self, of: str, reference: str) -> List[Record]:
        if of not in self.inventory.classes:
            return []
        if of not in self._index:
            index: Dict[str, List[Record]] = {}
            for record in self.cls(of).records:
                index.setdefault(self.inventory.digest_of(of, record), []).append(
                    record
                )
            self._index[of] = index
        return self._index[of].get(reference, [])

    def parent(
        self, name: str, record: Record, field: str
    ) -> Tuple[Optional[str], Optional[Record], str]:
        """(the parent's class, its record, or None, and why not) for key field ``field``."""
        value = record.key[field]
        if value is None:
            return None, None, "none"
        of = self.cls(name).parent_class(field, record.key)
        if of is None:
            return None, None, "<a reference to no stored class>"
        found = self.find(of, value)
        if len(found) != 1:
            return of, None, f"<{of} reference resolving to {len(found)} records>"
        return of, found[0], ""

    # --- leaves --------------------------------------------------------------------------------

    def is_float(self, name: str, field: str) -> bool:
        return field in self.floats.get(name, frozenset())

    def number(self, name: str, field: str, value: Any) -> Optional[float]:
        """The leaf as a number where it is one, else None."""
        if value is None:
            return None
        if self.is_float(name, field):
            return float.fromhex(value)
        if isinstance(value, (bool, int)):
            return float(value)
        return None

    def leaf_text(self, name: str, field: str, value: Any) -> str:
        if value is None:
            return "none"
        if self.is_float(name, field):
            text = f"{float.fromhex(value):.5g}"
            if field.startswith("value_"):
                text += f" {field[len('value_'):]}"
            return text
        return str(value)

    def leaf_sort(self, name: str, field: str, value: Any) -> tuple:
        number = self.number(name, field, value)
        if value is None:
            return (0,)
        if number is not None:
            return (1, number)
        return (2, str(value))

    # --- any record ----------------------------------------------------------------------------

    def field_order(self, name: str, record: Record) -> List[str]:
        """A record's key fields as printed: its parents in declaration order, then its leaves."""
        parents = [f for f in record.key if f in self.cls(name).parents]
        leaves = [f for f in record.key if f not in self.cls(name).parents]
        return parents + leaves

    def ref_text(self, name: str, record: Record, field: str) -> str:
        """A parent by its own key: a one-field key bare (a parameter value, a tolerance), any other
        key, or a polymorphic parent's, as ``Class(field=..., ...)``."""
        of, parent, why = self.parent(name, record, field)
        if parent is None:
            return why
        if field in self.cls(name).parent_types or len(parent.key) > 1:
            return f"{of}({self.key_text(of, parent, named=True)})"
        return self.key_text(of, parent)

    def ref_sort(self, name: str, record: Record, field: str) -> tuple:
        of, parent, _ = self.parent(name, record, field)
        if parent is None:
            return (9,)
        return (3, of, self.key_sort(of, parent))

    def value_text(self, name: str, record: Record, field: str) -> str:
        if field in self.cls(name).parents:
            return self.ref_text(name, record, field)
        return self.leaf_text(name, field, record.key[field])

    def value_sort(self, name: str, record: Record, field: str) -> tuple:
        if field in self.cls(name).parents:
            return self.ref_sort(name, record, field)
        return self.leaf_sort(name, field, record.key[field])

    def key_text(self, name: str, record: Record, named: bool = False) -> str:
        """A record's key: one field bare unless ``named``, otherwise ``field=value, ...``."""
        fields = self.field_order(name, record)
        if len(fields) == 1 and not named:
            return self.value_text(name, record, fields[0])
        return ", ".join(f"{f}={self.value_text(name, record, f)}" for f in fields)

    def key_sort(self, name: str, record: Record) -> tuple:
        return tuple(
            self.value_sort(name, record, f) for f in self.field_order(name, record)
        )

    # --- a ScalarModel, by its physical labels -------------------------------------------------

    def model_extras(self) -> Tuple[List[str], List[str]]:
        """(the extra model fields every ScalarModel shares, those that vary)."""
        records = (
            self.cls("ScalarModel").records
            if "ScalarModel" in self.inventory.classes
            else ()
        )
        common, varying = [], []
        for field, _ in MODEL_EXTRA_FIELDS:
            values = {r.key.get(field) for r in records}
            (varying if len(values) > 1 else common).append(field)
        return common, varying

    def model_parts(
        self, record: Record, extras: Sequence[str], with_tags: bool = False
    ) -> List[str]:
        labels = dict(MODEL_EXTRA_FIELDS)
        parts = [
            f"{label}={self.value_text('ScalarModel', record, field)}"
            for field, label in MODEL_FIELDS
            if field in record.key
        ]
        parts += [
            f"{labels[field]}={self.value_text('ScalarModel', record, field)}"
            for field in extras
            if field in record.key
        ]
        # anything else in the key, so that no field of it goes unprinted
        known = (
            {f for f, _ in MODEL_FIELDS}
            | {f for f, _ in MODEL_EXTRA_FIELDS}
            | set(MODEL_TYPE_LEAVES)
        )
        parts += [
            f"{f}={self.value_text('ScalarModel', record, f)}"
            for f in record.key
            if f not in known
        ]
        if with_tags and len(record.tags) > 0:
            parts.append(f"tags=[{', '.join(record.tags)}]")
        return parts

    def model_sort(self, record: Record) -> tuple:
        order = ["coupling", "potential", "T_init", "T_stop", "phi_init", "pi_init"]
        return tuple(
            self.value_sort("ScalarModel", record, f) for f in order if f in record.key
        ) + (
            self.leaf_sort("ScalarModel", "failure", record.key.get("failure")),
            record.canonical_json(),
        )

    def model_text_of(self, name: str, record: Record) -> Tuple[str, tuple]:
        """An AdiabaticHistory's or BBNData's model, by the model's labels, and its sort key."""
        of, model, why = self.parent(name, record, "model")
        if model is None:
            return f"model: {why}", (9,)
        _, varying = self.model_extras()
        # the model's version is shown here only where the models' versions differ
        parts = self.model_parts(model, varying, with_tags=True)
        return "model: {" + ", ".join(parts) + "}", self.model_sort(model)


def _suffix(record: Record) -> str:
    text = ""
    if len(record.tags) > 0:
        text += f"  tags: {', '.join(record.tags)}"
    if record.value_count is not None:
        text += f"  | values: {record.value_count:,}"
    return text


def _header(renderer: _Renderer, name: str) -> str:
    cls = renderer.cls(name)
    where = "replicated" if cls.replicated else "sharded"
    header = f"{_CLASS_INDENT}@@ {name} ({where}): {_plural(cls.count, 'record')}"
    parts = []
    if name in COMPUTE_TARGETS or any(r.validated is not None for r in cls.records):
        validated = sum(1 for r in cls.records if r.validated is True)
        unvalidated = sum(1 for r in cls.records if r.validated is False)
        null = sum(1 for r in cls.records if r.validated is None)
        split = f"{validated:,} validated, {unvalidated:,} unvalidated"
        if null > 0:
            split += f", {null:,} validated NULL"
        parts.append(split)
    if name in VALUE_TABLES:
        total = sum(r.value_count or 0 for r in cls.records)
        parts.append(_plural(total, f"{VALUE_TABLES[name]} row"))
    if len(parts) > 0:
        header += ": " + "; ".join(parts)
    if cls.earliest_timestamp is not None and cls.latest_timestamp is not None:
        if cls.count == 1 or cls.earliest_timestamp == cls.latest_timestamp:
            header += f" | committed at {_format_timestamp(cls.earliest_timestamp)}"
        else:
            header += (
                f" | committed between {_format_timestamp(cls.earliest_timestamp)}"
                f" and {_format_timestamp(cls.latest_timestamp)}"
            )
    return header


def _value_lines(renderer: _Renderer, name: str) -> List[str]:
    """A class whose key is one leaf: its values, sorted."""
    cls = renderer.cls(name)
    field = next(iter(cls.records[0].key))
    ordered = sorted(
        cls.records,
        key=lambda r: (
            renderer.leaf_sort(name, field, r.key[field]),
            r.canonical_json(),
        ),
    )
    texts = [renderer.leaf_text(name, field, r.key[field]) for r in ordered]
    label = "values" if field in ("z", "value") or field.startswith("value_") else field
    return [
        f"{_DETAIL_INDENT}{label} = [ {', '.join(_truncated(texts, renderer.verbose, 'value'))} ]"
    ]


def _record_lines(renderer: _Renderer, name: str) -> List[str]:
    """Any other class: its records by their physical labels, sorted."""
    cls = renderer.cls(name)
    ordered = sorted(
        cls.records,
        key=lambda r: (renderer.key_sort(name, r), r.canonical_json()),
    )
    texts = [f"- {renderer.key_text(name, r, named=True)}{_suffix(r)}" for r in ordered]
    return [
        f"{_RECORD_INDENT}{t}" for t in _truncated(texts, renderer.verbose, "record")
    ]


def _version_labels(renderer: _Renderer, name: str, records: Sequence[Record]) -> str:
    labels = sorted(
        {renderer.value_text(name, r, "version") for r in records if "version" in r.key}
    )
    if len(labels) == 1:
        return f"version = {labels[0]}"
    return f"versions = [ {', '.join(labels)} ]"


def _compute_target_lines(renderer: _Renderer, name: str) -> List[str]:
    """ScalarModel, AdiabaticHistory or BBNData: validated groups, records by their model's labels."""
    cls = renderer.cls(name)
    lines: List[str] = []
    common, varying = renderer.model_extras()

    if name == "ScalarModel":
        shared = [f for f in common if f != "version"]
        if len(shared) > 0 and cls.count > 0:
            labels = dict(MODEL_EXTRA_FIELDS)
            first = cls.records[0]
            lines.append(
                f"{_DETAIL_INDENT}common to every model: "
                + ", ".join(
                    f"{labels[f]}={renderer.value_text(name, first, f)}"
                    for f in shared
                    if f in first.key
                )
            )

    def entry(record: Record) -> Tuple[tuple, str]:
        if name == "ScalarModel":
            extras = [f for f in varying if f != "version"]
            parts = renderer.model_parts(record, extras)
            return renderer.model_sort(record), ", ".join(parts)
        model_text, model_sort = renderer.model_text_of(name, record)
        own = [
            f"{f}={renderer.value_text(name, record, f)}"
            for f in record.key
            if f not in ("model", "version")
        ]
        own_sort = tuple(
            renderer.value_sort(name, record, f)
            for f in record.key
            if f not in ("model", "version")
        )
        return (model_sort, own_sort, record.canonical_json()), ", ".join(
            own + [model_text]
        )

    groups = [
        ("validated", [r for r in cls.records if r.validated is True]),
        ("unvalidated", [r for r in cls.records if r.validated is False]),
    ]
    null = [r for r in cls.records if r.validated is None]
    if len(null) > 0:
        groups.append(("validated NULL", null))

    for title, records in groups:
        if len(records) == 0:
            lines.append(f"{_DETAIL_INDENT}{title}: none")
            continue
        lines.append(
            f"{_DETAIL_INDENT}{title}: {_plural(len(records), 'record')}; "
            f"{_version_labels(renderer, name, records)}"
        )
        entries = sorted((entry(r) + (r,) for r in records), key=lambda e: e[0])
        texts = [f"- {text}{_suffix(r)}" for _, text, r in entries]
        lines.extend(
            f"{_RECORD_INDENT}{t}"
            for t in _truncated(texts, renderer.verbose, "record")
        )
    return lines


def _class_lines(renderer: _Renderer, name: str) -> List[str]:
    cls = renderer.cls(name)
    lines = [_header(renderer, name)]

    if name in COMPUTE_TARGETS:
        lines.extend(_compute_target_lines(renderer, name))
    elif cls.count > 0:
        one_leaf = all(
            len(r.key) == 1 and next(iter(r.key)) not in cls.parents
            for r in cls.records
        )
        if one_leaf and not cls.tagged:
            lines.extend(_value_lines(renderer, name))
        else:
            lines.extend(_record_lines(renderer, name))

    # every problem, whole
    if len(cls.problems) > 0:
        lines.append(f"{_DETAIL_INDENT}{_plural(len(cls.problems), 'problem')}:")
        for problem in cls.problems:
            lines.append(f"{_RECORD_INDENT}!! {problem}")

    return lines


def format_inventory_report(
    inventory: StoreInventory, db_name: Any, verbose: bool = False
) -> str:
    """
    Render ``inventory`` (what ``read_inventory`` returns) as the text of the inventory report.
    ``db_name`` is printed in the title as given. With ``verbose`` every value and record is
    listed; otherwise a list of ``LIST_LIMIT`` or more shows its first and last ``LIST_ENDS``.
    Every problem is printed whole either way.
    """
    renderer = _Renderer(inventory, verbose)
    problems = inventory.problems
    total = sum(c.count for c in inventory.classes.values())

    lines = [f"@@ DATASTORE INVENTORY: {db_name}"]
    shard_list = ", ".join(f"#{s}" for s in inventory.shards)
    lines.append(
        f"   read-only; {_plural(len(inventory.shards), 'shard')} ({shard_list}); "
        f"{_plural(total, 'record')} in {_plural(len(inventory.classes), 'class')}"
    )
    lines.append(
        "   problems: none"
        if len(problems) == 0
        else f"   problems: {len(problems):,}, each listed in full under its class"
    )

    named = {n for _, names in INVENTORY_CATEGORIES for n in names}
    categories = [
        (title, [n for n in names if n in inventory.classes])
        for title, names in INVENTORY_CATEGORIES
    ]
    others = [n for n in inventory.classes if n not in named]
    if len(others) > 0:
        categories.append(("Other classes", others))

    for title, names in categories:
        if len(names) == 0:
            continue
        lines.append("")
        lines.append(f"   -- {title}")
        for name in names:
            lines.extend(_class_lines(renderer, name))

    return "\n".join(lines)
