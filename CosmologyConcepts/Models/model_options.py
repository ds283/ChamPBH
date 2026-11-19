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
The potentials as the drivers name them (power-law-wall prompt 06 §1.1; README §6.10 R12, P45, P47, P52).

The values of the drivers' `--models` (config/argument_parser.py), the same as the tools' `--model`
(tools/history_and_bbn.py, tools/surfing_scan.py, tools/bbn_from_store.py), each with its class, named once:
  - MODEL_EXPONENTIAL, "exponential": ExponentialModel, V = V0 exp((M/phi)^n);
  - MODEL_INVERSE_POWER, "inverse-power": InversePowerModel, V = V0 [1 + (M/phi)^n];
both with the coupling A = exp(beta (phi - phi_ref)/M_P). A potential's table is its class's name (ObjectFactories), and
an export of it goes under `<output>/<label>/<value>/` with `model` = <value> in its histories.csv (R12). No potential
is privileged here: the drivers' default `[exponential]` is the parser's, and the one place a potential is named without
being asked for.

What a driver or an export tool needs of a potential that depends on its wall is read through the model interface, from
a model built by `reference_model` at M = M_P and beta = 1, which neither hook reads (log 05 D6): its one-loop drain
model (`AbstractModel.one_loop_drain_model`) and its spectrum's wall (`AbstractModel.spectrum_wall`). A potential whose
hook has no value at an n raises the hook's own ValueError, which names the class, the n and the hook.
"""

from typing import Iterable, List

from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models.AbstractModel import AbstractModel
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyConcepts.Models.InversePowerModel import InversePowerModel
from Units import Planck_units

MODEL_EXPONENTIAL = "exponential"
MODEL_INVERSE_POWER = "inverse-power"

# each value's class, in the order the option lists them
MODEL_CLASSES = {
    MODEL_EXPONENTIAL: ExponentialModel,
    MODEL_INVERSE_POWER: InversePowerModel,
}
MODEL_OPTIONS = tuple(MODEL_CLASSES)

# each value's potential, as the drivers' printed lines and the option's help name it
MODEL_POTENTIALS = {
    MODEL_EXPONENTIAL: "V = V0 exp((M/phi)^n)",
    MODEL_INVERSE_POWER: "V = V0 [1 + (M/phi)^n]",
}


def _check(value: str) -> str:
    if value not in MODEL_CLASSES:
        raise ValueError(
            f"model_options: the potential must be one of {MODEL_OPTIONS}, not {value!r}"
        )
    return value


def model_class(value: str) -> type:
    """The class of a potential's option value; an unknown value is a ValueError naming the known ones."""
    return MODEL_CLASSES[_check(value)]


def model_table(value: str) -> str:
    """The table of a potential's option value: its class's name (ObjectFactories), the name its lookups use."""
    return model_class(value).__name__


def model_option(model) -> str:
    """
    The option value of a model object or class (an export's `model` and `<model>/` directory): the value whose class
    it is an instance of, or is. Anything else is a ValueError naming it.
    """
    cls = model if isinstance(model, type) else type(model)
    for value, known in MODEL_CLASSES.items():
        if cls is known:
            return value
    raise ValueError(
        f"model_options: {cls.__name__} is not the class of any of {MODEL_OPTIONS}"
    )


def reference_model(value: str, n: int, units=None) -> AbstractModel:
    """
    The potential's model at n, with M = M_P and beta = 1 (store ids 0), in `units` (Planck_units if None): the model
    whose hooks the drivers and the export tools read. Its hooks that depend on the wall read the class and n only.
    """
    units = Planck_units() if units is None else units
    return model_class(value)(
        0, M_value(0, units.PlanckMass), n, beta_value(0, 1.0), units
    )


def one_loop_drain_model(value: str, n: int) -> str:
    """The potential's own one-loop drain model at n (AbstractModel.one_loop_drain_model); the hook's ValueError at an
    n where the potential has no one-loop law propagates."""
    return reference_model(value, n).one_loop_drain_model


def one_loop_drain_model_over(value: str, n_values: Iterable[int]) -> str:
    """
    The one one-loop drain model of a potential over every n of `n_values` (P47): the potential's `--A-values` rows are
    looked up under it. A ValueError, naming the potential and the n, if the potential has no one-loop law at one of
    them, or if its n name more than one drain model; neither happens for the potentials of MODEL_OPTIONS at the n
    where their law is defined, but the rule is the hook's, not assumed here.
    """
    found: List[str] = []
    for n in n_values:
        try:
            drain_model = one_loop_drain_model(value, n)
        except (ValueError, NotImplementedError) as e:
            raise ValueError(
                f"the potential {value} has no one-loop drain law at n = {n}: {e}"
            ) from None
        if drain_model not in found:
            found.append(drain_model)
    if len(found) != 1:
        raise ValueError(
            f"the potential {value} names {len(found)} one-loop drain models over n = "
            f"{', '.join(str(n) for n in n_values)} ({', '.join(found) or 'none'}), not one"
        )
    return found[0]


def spectrum_wall(value: str, n: int) -> str:
    """The wall of the potential's spectrum at n (AbstractModel.spectrum_wall), a wall of
    ComputeTargets.quanta_spectrum; the hook's ValueError at an n where it has none propagates.
    """
    return reference_model(value, n).spectrum_wall
