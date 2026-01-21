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

import ray

from CosmologyModels.LambdaCDM import Planck2018
from Datastore.SQL.ShardedPool import ShardedPool
from Units.base import UnitsLike


def build_model_list(pool: ShardedPool, units: UnitsLike):
    params = Planck2018()

    QCD_EOS_Planck2018 = ray.get(
        pool.object_get("QCD_Cosmology", params=params, units=units)
    )

    return [
        {
            "label": "QCD_Cosmology",
            "cosmology": QCD_EOS_Planck2018,
        },
    ]
