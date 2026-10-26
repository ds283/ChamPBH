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
ChamPBH's datastore registries (prompts/dk-and-rpk-migration, prompts 05 and 08).

The datastore layer is the ``datastorekit`` package, which owns none of these: each driver gives
them to its ``ShardedPool`` (``factories=``, ``serial_batch_sizes=`` and, in ``main.py``,
``drop_tables=``).

- ``factories``: the registry of storable classes, each table name mapped to its factory instance,
  in the order the schema is built. Six entries are built on a quantity class (by the dimensionless
  and the dimensionful quantity factories), which the factory keeps as ``ObjectType``.
- ``drop_groups``: the ``--drop`` groups, each mapped to the tables it drops. The pool drops tables,
  in the order their foreign keys require, so the order within a group is not the drop order.
- ``serial_batch_sizes``: how many serials an actor leases from the broker at a time, per table; a
  table not named here leases the package's default.
- ``tables_to_drop(groups)``: the tables of the requested ``--drop`` groups, refused with
  ``ValueError`` if a table outside them names the rows of one of them (the package's
  ``datastorekit.SQL.schema.dependent_tables``), with a message naming the groups to add.
  ``main.py`` calls it before Ray starts (campaign README, U4). The pool refuses the same drop at
  open, as a second line that the command line never reaches.

This module imports no driver, and can be imported first, alone.
"""

from typing import Iterable, List

import sqlalchemy as sqla
from datastorekit.SQL.schema import build_schema, dependent_tables
from datastorekit.store_inventory import inventory_specs

from CosmologyConcepts import (
    beta_value,
    M_value,
    temperature,
    phi_value,
    pi_value,
)
from ObjectFactories.AdiabaticHistory import (
    sqla_AdiabaticHistoryFactory,
    sqla_AdiabaticHistoryTagAssociation_factory,
    sqla_AdiabaticHistoryValue_factory,
)
from ObjectFactories.BBNData import (
    sqla_BBNDataFactory,
    sqla_BBNDataTagAssociation_factory,
    sqla_BBNDataValue_factory,
)
from ObjectFactories.DimensionfulQuantity import (
    sqla_dimensionful_quantity_factory,
)
from ObjectFactories.DimensionlessQuantity import (
    sqla_dimensionless_quantity_factory,
)
from ObjectFactories.ExponentialModel import sqla_ExponentialModel_factory
from ObjectFactories.QCD_Cosmology import sqla_QCDCosmology_factory
from ObjectFactories.ScalarHistory import (
    sqla_ScalarHistoryFactory,
    sqla_ScalarHistoryTagAssociation_factory,
    sqla_ScalarHistoryValue_factory,
)
from ObjectFactories.integration_metadata import (
    sqla_IntegrationSolver_factory,
)
from ObjectFactories.redshift import sqla_redshift_factory
from ObjectFactories.store_tag import sqla_store_tag_factory
from ObjectFactories.tolerance import sqla_tolerance_factory
from ObjectFactories.version import sqla_version_factory

factories = {
    "version": sqla_version_factory(),
    "store_tag": sqla_store_tag_factory(),
    "redshift": sqla_redshift_factory(),
    "tolerance": sqla_tolerance_factory(),
    "beta_value": sqla_dimensionless_quantity_factory(beta_value),
    "M_value": sqla_dimensionful_quantity_factory(M_value),
    "temperature": sqla_dimensionful_quantity_factory(temperature),
    "phi_value": sqla_dimensionful_quantity_factory(phi_value),
    "pi_value": sqla_dimensionful_quantity_factory(pi_value),
    "ExponentialModel": sqla_ExponentialModel_factory(),
    "QCD_Cosmology": sqla_QCDCosmology_factory(),
    "IntegrationSolver": sqla_IntegrationSolver_factory(),
    "ScalarHistory": sqla_ScalarHistoryFactory(),
    "ScalarHistory_tags": sqla_ScalarHistoryTagAssociation_factory(),
    "ScalarHistoryValue": sqla_ScalarHistoryValue_factory(),
    "AdiabaticHistory": sqla_AdiabaticHistoryFactory(),
    "AdiabaticHistory_tags": sqla_AdiabaticHistoryTagAssociation_factory(),
    "AdiabaticHistoryValue": sqla_AdiabaticHistoryValue_factory(),
    "BBNData": sqla_BBNDataFactory(),
    "BBNData_tags": sqla_BBNDataTagAssociation_factory(),
    "BBNDataValue": sqla_BBNDataValue_factory(),
}

drop_groups = {
    "scalar-history": ["ScalarHistory_tags", "ScalarHistoryValue", "ScalarHistory"],
    "adiabatic-history": [
        "AdiabaticHistory_tags",
        "AdiabaticHistoryValue",
        "AdiabaticHistory",
    ],
    "bbn-data": ["BBNData_tags", "BBNDataValue", "BBNData"],
}

serial_batch_sizes = {
    "store_tag": 5,
    "redshift": 200,
    "tolerance": 5,
    "beta_value": 200,
    "M_value": 200,
    "temperature": 200,
    "phi_value": 200,
    "pi_value": 200,
    "ExponentialModel": 50,
    "QCD_Cosmology": 5,
    "IntegrationSolver": 6,
    "ScalarHistory": 20,
    "ScalarHistoryValue": 250,
    "AdiabaticHistory": 20,
    "AdiabaticHistoryValue": 250,
    "BBNData": 20,
    "BBNDataValue": 250,
}


def _names_of(tables: List[str], among: List[str]) -> List[str]:
    """
    Which of ``among`` name a table of ``tables`` directly, by the rule ``dependent_tables`` follows
    transitively (a foreign key, or a parent a factory's ``inventory_spec`` declares), in registry
    order. Used only to word ``tables_to_drop``'s refusal; which tables must go is
    ``dependent_tables``' answer alone.
    """
    built = build_schema(sqla.MetaData(), factories).tables
    named = {
        name: {fk.column.table.name for fk in table.foreign_keys}
        for name, table in built.items()
    }
    for name, spec in inventory_specs(factories).items():
        named.setdefault(name, set()).update(spec.dependencies())
    return [
        name
        for name in built
        if name in among and not named.get(name, set()).isdisjoint(tables)
    ]


def _joined(items: List[str]) -> str:
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def tables_to_drop(groups: Iterable[str]) -> List[str]:
    """
    The tables of the ``--drop`` groups named by ``groups``, each once, in ``drop_groups``' order.

    A name that is not a drop group raises ``ValueError`` naming it and the groups there are. A
    request that would leave rows naming rows it drops raises ``ValueError``: the tables that must
    go with it are ``datastorekit.SQL.schema.dependent_tables``' answer, from the registry, and the
    message names the requested groups, the tables that name their rows, and the ``--drop`` groups
    to add (a table that must go but is in no group is named as a table). So
    ``tables_to_drop(["scalar-history"])`` raises, since ``AdiabaticHistory`` and ``BBNData`` name
    ``ScalarHistory`` rows; ``main.py`` calls this before Ray starts (dk-and-rpk-migration prompt 08,
    campaign README U4).
    """
    requested = list(dict.fromkeys(groups))
    unknown = [group for group in requested if group not in drop_groups]
    if len(unknown) > 0:
        raise ValueError(
            f"unknown drop group(s) {unknown}: the drop groups are {list(drop_groups)}"
        )

    tables = [
        table
        for group, group_tables in drop_groups.items()
        if group in requested
        for table in group_tables
    ]
    dependents = [
        table for table in dependent_tables(tables, factories) if table not in tables
    ]
    if len(dependents) == 0:
        return tables

    group_of = {
        table: group
        for group, group_tables in drop_groups.items()
        for table in group_tables
    }
    asked = [group for group in drop_groups if group in requested]
    to_add = [
        group
        for group in drop_groups
        if group not in requested and any(group_of.get(t) == group for t in dependents)
    ]
    loose = [table for table in dependents if table not in group_of]
    referrers = _names_of(tables, dependents)

    additions = []
    if len(to_add) > 0:
        additions.append(f"`--drop {' '.join(to_add)}`")
    if len(loose) > 0:
        additions.append(
            f"the table{'s' if len(loose) > 1 else ''} {_joined(loose)}, which no drop group holds"
        )
    raise ValueError(
        f"cannot drop {_joined(asked)} alone: {_joined(referrers)} "
        f"name{'s' if len(referrers) == 1 else ''} {'its' if len(asked) == 1 else 'their'} rows; "
        f"add {' and '.join(additions)}"
    )
