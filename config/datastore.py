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
ChamPBH's datastore registries (prompts/dk-and-rpk-migration, prompt 05).

The datastore layer does not own these; this module does.

- ``factories``: the registry of storable classes, each table name mapped to its factory instance,
  in the order the schema is built. Six entries are built on a quantity class (by the dimensionless
  and the dimensionful quantity factories), which the factory keeps as ``ObjectType``.
- ``drop_groups``: the ``--drop`` actions, each mapped to the tables it drops, in the order
  ``Datastore._drop_actions`` drops them.
- ``serial_batch_sizes``: how many serials an actor leases from the broker at a time, per table.
  ``LambdaCDM`` is listed although this project has no such table; it leaves when the layer does
  (campaign README, P8).

Until prompt 08 gives these to the package, the old layer reads them from here, under their old
names: ``Datastore.SQL.Datastore`` imports ``factories`` as ``_factories`` and ``drop_groups`` as
``_drop_actions``, and ``Datastore.SQL.ClientPool`` imports ``serial_batch_sizes`` as
``_default_serial_batch_size``.

Import-order hazard: ``Datastore/SQL/__init__.py`` imports the actor, so importing any factory
module runs ``Datastore/SQL/Datastore.py``, which imports this module. A fresh
``import config.datastore`` therefore fails with ``ImportError: cannot import name ... from
partially initialized module 'config.datastore'``. Import the layer (``Datastore.SQL.Datastore``)
first. Every driver and actor does, so nothing that runs is affected. Prompt 08 removes the cycle
(issue ``05-config-datastore-cannot-be-imported-before-the-layer``).
"""

from CosmologyConcepts import (
    beta_value,
    Lambda_value,
    M_value,
    temperature,
    phi_value,
    pi_value,
)
from Datastore.SQL.ObjectFactories.AdiabaticHistory import (
    sqla_AdiabaticHistoryFactory,
    sqla_AdiabaticHistoryTagAssociation_factory,
    sqla_AdiabaticHistoryValue_factory,
)
from Datastore.SQL.ObjectFactories.BBNData import (
    sqla_BBNDataFactory,
    sqla_BBNDataTagAssociation_factory,
    sqla_BBNDataValue_factory,
)
from Datastore.SQL.ObjectFactories.DimensionfulQuantity import (
    sqla_dimensionful_quantity_factory,
)
from Datastore.SQL.ObjectFactories.DimensionlessQuantity import (
    sqla_dimensionless_quantity_factory,
)
from Datastore.SQL.ObjectFactories.ExponentialCoupling import (
    sqla_ExponentialCoupling_factory,
)
from Datastore.SQL.ObjectFactories.ExponentialPotential import (
    sqla_ExponentialPotential_factory,
)
from Datastore.SQL.ObjectFactories.InversePowerPotential import (
    sqla_InversePowerPotential_factory,
)
from Datastore.SQL.ObjectFactories.QCD_Cosmology import sqla_QCDCosmology_factory
from Datastore.SQL.ObjectFactories.ReclinerPotential import (
    sqla_ReclinerPotential_factory,
)
from Datastore.SQL.ObjectFactories.ReflectingPotential import (
    sqla_ReflectingPotential_factory,
)
from Datastore.SQL.ObjectFactories.ScalarModel import (
    sqla_ScalarModelFactory,
    sqla_ScalarModelTagAssociation_factory,
    sqla_ScalarModelValue_factory,
)
from Datastore.SQL.ObjectFactories.Starobinsky import (
    sqla_StarobinskyPotential_factory,
)
from Datastore.SQL.ObjectFactories.integration_metadata import (
    sqla_IntegrationSolver_factory,
)
from Datastore.SQL.ObjectFactories.redshift import sqla_redshift_factory
from Datastore.SQL.ObjectFactories.store_tag import sqla_store_tag_factory
from Datastore.SQL.ObjectFactories.tolerance import sqla_tolerance_factory
from Datastore.SQL.ObjectFactories.version import sqla_version_factory

factories = {
    "version": sqla_version_factory(),
    "store_tag": sqla_store_tag_factory(),
    "redshift": sqla_redshift_factory(),
    "tolerance": sqla_tolerance_factory(),
    "beta_value": sqla_dimensionless_quantity_factory(beta_value),
    "M_value": sqla_dimensionful_quantity_factory(M_value),
    "Lambda_value": sqla_dimensionful_quantity_factory(Lambda_value),
    "temperature": sqla_dimensionful_quantity_factory(temperature),
    "phi_value": sqla_dimensionful_quantity_factory(phi_value),
    "pi_value": sqla_dimensionful_quantity_factory(pi_value),
    "InversePowerPotential": sqla_InversePowerPotential_factory(),
    "StarobinskyPotential": sqla_StarobinskyPotential_factory(),
    "ExponentialPotential": sqla_ExponentialPotential_factory(),
    "ReclinerPotential": sqla_ReclinerPotential_factory(),
    "ReflectingPotential": sqla_ReflectingPotential_factory(),
    "ExponentialCoupling": sqla_ExponentialCoupling_factory(),
    "QCD_Cosmology": sqla_QCDCosmology_factory(),
    "IntegrationSolver": sqla_IntegrationSolver_factory(),
    "ScalarModel": sqla_ScalarModelFactory(),
    "ScalarModel_tags": sqla_ScalarModelTagAssociation_factory(),
    "ScalarModelValue": sqla_ScalarModelValue_factory(),
    "AdiabaticHistory": sqla_AdiabaticHistoryFactory(),
    "AdiabaticHistory_tags": sqla_AdiabaticHistoryTagAssociation_factory(),
    "AdiabaticHistoryValue": sqla_AdiabaticHistoryValue_factory(),
    "BBNData": sqla_BBNDataFactory(),
    "BBNData_tags": sqla_BBNDataTagAssociation_factory(),
    "BBNDataValue": sqla_BBNDataValue_factory(),
}

drop_groups = {
    "scalar-model": ["ScalarModel_tags", "ScalarModelValue", "ScalarModel"],
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
