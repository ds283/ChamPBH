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
from CosmologyConcepts.drain_model import (
    DRAIN_MODEL_FIXED,
    check_drain_model,
    law_of,
)


# the fraction alpha of the condensate's kinetic energy drained into chameleon quanta at each
# candidate (inelastic-reflection prompts 02 and 09; README §6.1 U3, §8.2 P28): a key of every
# ScalarHistory, stored in its own replicated table. alpha = 0 is natural mode.
#
# Since one-loop-drain prompt 03 (README §8.10 decision 1, §8.11 P27) a row is the pair (value, drain_model),
# unique: drain_model is DRAIN_MODEL_FIXED ("fixed"), under which the value is alpha, 0 <= alpha < 1, or
# DRAIN_MODEL_ONE_LOOP (the measured law and its version), under which the value is the law's strength A,
# finite and > 0. The pair is checked on construction by check_drain_law under the model's law (law_of), so an
# unknown model, A = 0 under the one-loop model, or alpha >= 1 under the fixed one is a ValueError. The default
# model is "fixed", so that alpha_value(store_id, value) is the construction of before
class alpha_value(DimensionlessQuantity):
    def __init__(
        self, store_id: int, value: float, drain_model: str = DRAIN_MODEL_FIXED
    ):
        super().__init__(store_id, value, "alpha_value")
        check_drain_model(drain_model, value, "alpha_value")
        self.drain_model: str = drain_model

    @property
    def drain_law(self) -> str:
        """The drain law the model is integrated under (law_of): "fixed" or "one_loop"."""
        return law_of(self.drain_model, "alpha_value")
