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

from CosmologyConcepts import beta_value

replicated_tables = [
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
    "ExponentialPotential",
    "InversePowerPotential",
    "ReflectingPotential",
    "ReclinerPotential",
    "StarobinskyPotential",
    "LambdaCDM",
    "QCD_Cosmology",
    "IntegrationSolver",
]

sharded_tables = {
    "ExponentialCoupling": "shard_key",
    "ScalarModel": "shard_key",
    "ScalarModelValue": "shard_key",
    "AdiabaticHistory": "shard_key",
    "AdiabaticHistoryValue": "shard_key",
    "BBNData": "shard_key",
    "BBNDataValue": "shard_key",
}

read_table_config = {
    "redshift": {"tables_arg": True},
    "beta_value": {"tables_arg": False},
    "M_value": {"tables_arg": False},
    "Lambda_value": {"tables_arg": False},
    "phi_value": {"tables_arg": False},
    "pi_value": {"tables_arg": False},
}

inventory_config = {
    "ScalarModel": {
        "validated": {
            "earliest_timestamp": "earliest",
            "latest_timestamp": "latest",
            "versions": "extend",
            "labels": "extend",
        },
        "unvalidated": {
            "earliest_timestamp": "earliest",
            "latest_timestamp": "latest",
            "versions": "extend",
            "labels": "extend",
        },
    },
    "AdiabaticHistory": {
        "validated": {
            "earliest_timestamp": "earliest",
            "latest_timestamp": "latest",
            "versions": "extend",
            "labels": "extend",
        },
        "unvalidated": {
            "earliest_timestamp": "earliest",
            "latest_timestamp": "latest",
            "versions": "extend",
            "labels": "extend",
        },
    },
    "BBNData": {
        "validated": {
            "earliest_timestamp": "earliest",
            "latest_timestamp": "latest",
            "versions": "extend",
            "PRyMordial_version": "extend",
            "labels": "extend",
        },
        "unvalidated": {
            "earliest_timestamp": "earliest",
            "latest_timestamp": "latest",
            "versions": "extend",
            "PRyMordial_version": "extend",
            "labels": "extend",
        },
    },
}


# configure ShardedPool to shard by beta_value
# this seems the best choice, because we know the phenomenology is mostly independent of (M, Lambda), and we won't
# always want to probe a lot of those
ShardKeyType = beta_value


# get wavenumber store id for a wavenumber object, interpreted as a shard key,
# or a proxy for it
def get_shard_key_store_id(obj):
    if isinstance(obj, beta_value):
        return obj.store_id

    raise RuntimeError(
        f'Could not determine shard key for object of type "{type(obj)}"'
    )
