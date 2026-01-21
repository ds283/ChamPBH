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

from CosmologyModels.GenericEOS.LambdaCDM_GenericEOS import (
    LambdaCDM_GenericEOS,
)
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from Units.base import UnitsLike


class QCD_Cosmology(LambdaCDM_GenericEOS):

    def __init__(
        self,
        store_id: int,
        units: UnitsLike,
        params,
    ):
        """
        QCD_Cosmology is a convenience wrapper that builds a ParametrizedEOS cosmology using
        a specified equation of state
        :param store_id:
        :param units:
        :param params:
        """
        LambdaCDM_GenericEOS.__init__(
            self,
            store_id,
            Xav_EOS_spline(units),
            units,
            params,
        )
