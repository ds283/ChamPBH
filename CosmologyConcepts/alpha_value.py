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

from CosmologyConcepts import DimensionlessQuantity


# the fraction alpha of the condensate's kinetic energy drained into chameleon quanta at each
# candidate (inelastic-reflection prompts 02 and 09; README §6.1 U3, §8.2 P28): a key of every
# ScalarHistory, stored in its own replicated table as beta_value is. alpha = 0 is natural mode
class alpha_value(DimensionlessQuantity):
    def __init__(self, store_id: int, value: float):
        super().__init__(store_id, value, "alpha_value")
