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
The drain law, the drain model and their rules (one-loop-drain prompt 03; orchestrator note 03, ruling 1).

A leaf module: it imports nothing of CosmologyConcepts or ComputeTargets, so that `alpha_value`, which
carries the drain model as part of its key, and `ComputeTargets.ScalarHistory`, which integrates under the law,
can both import it without a cycle (ComputeTargets.ScalarHistory imports CosmologyConcepts, whose __init__
imports alpha_value). Until prompt 03 these names were defined in ComputeTargets/ScalarHistory.py, which imports
them from here, so that every `SM.<name>` still resolves to the same object. Each potential's one-loop law, its
constants and its tables are its model class's (power-law-wall prompt 07c, README §6.14 P99).

- the laws (one-loop-drain prompt 01; README §8.2, §8.8 P22): DRAIN_LAW_FIXED, alpha_eff = alpha w_gate at
  every candidate, 0 <= alpha < 1, the law to VERSION_LABEL 2026.9.0, the default; DRAIN_LAW_ONE_LOOP, alpha
  carries the law's strength A, finite and > 0, and each candidate drains by its model's own one-loop fraction
  (ComputeTargets/ScalarHistory.py's drain_alpha_effective, AbstractModel._drain_fraction).
- the drain models (README §8.10, decision 1; §8.11 P27): the value of `alpha_value.drain_model`, a key of every
  history. DRAIN_MODEL_FIXED is the fixed law; DRAIN_MODEL_ONE_LOOP_EXPONENTIAL ("one-loop-exponential-v2") names the
  exponential's one-loop law and its version, which CosmologyConcepts/Models/ExponentialModel.py defines, and
  DRAIN_MODEL_ONE_LOOP_INVERSE_POWER ("one-loop-inverse-power-v1") the power wall's, f_n(ln R) and R S_n, which
  CosmologyConcepts/Models/InversePowerModel.py defines (power-law-wall prompt 07c; README §6.14 P99, P101). Which
  one-loop model a history takes is its model's (AbstractModel.one_loop_drain_model). The names before 07c,
  "one-loop-measured-v1" (the exponential's v1, R13) and "one-loop-measured-pl-v1" (the power wall's, U11), stay known
  (RETIRED_DRAIN_MODELS) and are refused by law_of, the one place every reader of a drain model passes through.
- law_of: the one mapping from a model to its law, which refuses the retired models; check_drain_law: the rules for
  the value under a law, the one statement of them. model_of_law, the inverse that named the exponential's model as
  "the" one-loop model, was removed by power-law-wall prompt 07c (P101): nothing in production called it.
- the regimes of a candidate under the one-loop law (REGIME_*, DRAIN_REGIMES), defined here since power-law-wall
  prompt 03 so that a model's own fraction (AbstractModel._drain_fraction) can name them without importing
  ComputeTargets.ScalarHistory, which imports them from here, so that every SM.REGIME_* resolves to the same object.
"""

from math import isfinite

DRAIN_LAW_FIXED = "fixed"
DRAIN_LAW_ONE_LOOP = "one_loop"
DRAIN_LAWS = (DRAIN_LAW_FIXED, DRAIN_LAW_ONE_LOOP)

# the drain model, named in the "drain" record (README §8.10, decision 1) and stored as alpha_value's
# drain_model, beside its value (one-loop-drain prompt 03; README §8.11 P27). Each one-loop drain model names one
# potential's one-loop law and its version, which that potential's model class defines (AbstractModel._drain_fraction,
# AbstractModel.one_loop_drain_model; power-law-wall README §6.14 P99, P101). THE VERSION MOVES whenever the law, its
# constants or its tables change, so that a new version is a new key value: a lookup under the new string finds no
# history of the old version, as a lookup under a new VERSION_LABEL finds none of the old label
#   DRAIN_MODEL_ONE_LOOP_EXPONENTIAL:    ExponentialModel's, f = 1 - exp(-min(y_rise, y_fall)) above the classical
#                                        branch R S(L) (power-law-wall prompt 07c; README §6.13 U13, R14), at n = 1, 2
#   DRAIN_MODEL_ONE_LOOP_INVERSE_POWER:  InversePowerModel's, f_n(ln R) above R S_n (power-law-wall prompt 03; README
#                                        §6.5 R7), at n = 1, 2; named "one-loop-measured-pl-v1" until power-law-wall
#                                        prompt 07c (U11), its law and constants unchanged
# DRAIN_MODELS keeps its order, the fixed model, the exponential's, the power wall's, so that extract_common's sort by it
# does not move (P101)
DRAIN_MODEL_FIXED = "fixed"
DRAIN_MODEL_ONE_LOOP_EXPONENTIAL = "one-loop-exponential-v2"
DRAIN_MODEL_ONE_LOOP_INVERSE_POWER = "one-loop-inverse-power-v1"
DRAIN_MODELS = (
    DRAIN_MODEL_FIXED,
    DRAIN_MODEL_ONE_LOOP_EXPONENTIAL,
    DRAIN_MODEL_ONE_LOOP_INVERSE_POWER,
)

# the retired drain models (power-law-wall prompt 07c; README §6.12 P85, §6.14 P101): known names, refused wherever a
# drain model is read (law_of, and through it check_drain_model, alpha_value and its factory, alpha_label, the drivers,
# the plot scripts and the store tools), so that a store or an option naming one is refused, not misread
DRAIN_MODEL_RETIRED_MEASURED_V1 = "one-loop-measured-v1"
DRAIN_MODEL_RETIRED_MEASURED_PL_V1 = "one-loop-measured-pl-v1"
RETIRED_DRAIN_MODELS = {
    DRAIN_MODEL_RETIRED_MEASURED_V1: (
        "the exponential's measured one-loop law v1 read ell in the old k_ex (-d ln V/dphi E^{1/2}/2), which R13 "
        f"retires (power-law-wall README §6.12); the exponential's one-loop law is now "
        f"{DRAIN_MODEL_ONE_LOOP_EXPONENTIAL!r}"
    ),
    DRAIN_MODEL_RETIRED_MEASURED_PL_V1: (
        "the power wall's one-loop law was renamed by U11 (power-law-wall README §6.12), its law and constants "
        f"unchanged; it is now {DRAIN_MODEL_ONE_LOOP_INVERSE_POWER!r}"
    ),
}

# the law each model is integrated under (the one mapping; one-loop-drain prompt 03 §1.1)
_LAW_OF_MODEL = {
    DRAIN_MODEL_FIXED: DRAIN_LAW_FIXED,
    DRAIN_MODEL_ONE_LOOP_EXPONENTIAL: DRAIN_LAW_ONE_LOOP,
    DRAIN_MODEL_ONE_LOOP_INVERSE_POWER: DRAIN_LAW_ONE_LOOP,
}

# the regimes of a candidate under the one-loop law (one-loop-drain README §8.2): peak_estimate's classification,
# computed once, in the loop (P18), with a one-loop solution of ell <= 0 called gap (P24). Defined in
# ComputeTargets/ScalarHistory.py until power-law-wall prompt 03, which imports them from here
REGIME_ONE_LOOP = "one_loop"
REGIME_GAP = "gap"
REGIME_CLASSICAL = "classical"
REGIME_UNASSESSED = "unassessed"
DRAIN_REGIMES = (REGIME_ONE_LOOP, REGIME_GAP, REGIME_CLASSICAL, REGIME_UNASSESSED)


def law_of(drain_model: str, context: str = "law_of") -> str:
    """
    The drain law of a drain model: DRAIN_LAW_FIXED for DRAIN_MODEL_FIXED, and DRAIN_LAW_ONE_LOOP for
    DRAIN_MODEL_ONE_LOOP_EXPONENTIAL and DRAIN_MODEL_ONE_LOOP_INVERSE_POWER. A retired model (RETIRED_DRAIN_MODELS:
    "one-loop-measured-v1", R13; "one-loop-measured-pl-v1", U11) is a ValueError saying why it is retired and what
    replaces it, and any other model a ValueError naming the known ones; each message begins with context.
    """
    try:
        retired = RETIRED_DRAIN_MODELS.get(drain_model)
    except TypeError:
        retired = None
    if retired is not None:
        raise ValueError(
            f"{context}: the drain model {drain_model!r} is retired (power-law-wall prompt 07c): {retired}"
        )
    try:
        return _LAW_OF_MODEL[drain_model]
    except (KeyError, TypeError):
        raise ValueError(
            f"{context}: drain_model must be one of {DRAIN_MODELS}, not {drain_model!r}"
        ) from None


def alpha_label(value: float, drain_model: str, alpha_spelling: str = "α") -> str:
    """
    The label of an alpha_value row, from its value and its model (one-loop-drain prompt 04, README §8.12 P35;
    orchestrator note 04 ruling 2). Under DRAIN_MODEL_FIXED it is ``<alpha_spelling> = <value:g>``, the
    spelling being a parameter because the printed lines, the titles and the legends have always spelt alpha
    three ways; under any other model it is ``A = <value:g> (<drain_model>)``, whatever the spelling. The model
    has no default: a caller that forgets it is a TypeError, not a fixed-law label on a one-loop row.
    """
    if drain_model == DRAIN_MODEL_FIXED:
        return f"{alpha_spelling} = {value:g}"
    law_of(drain_model, "alpha_label")
    return f"A = {value:g} ({drain_model})"


def check_drain_law(drain_law: str, alpha: float, context: str) -> None:
    """
    The law's inputs (one-loop-drain prompt 01 §1.1; README §8.8 P22), checked at entry by
    integrate_scalar_history, integrate_history and tune_history (context, the function's name, begins
    the message), and by alpha_value and its factory on the pair (value, drain_model) (one-loop-drain
    prompt 03 §1.1). Under DRAIN_LAW_FIXED alpha is the fraction, 0 <= alpha < 1, checked as before
    one-loop-drain prompt 01, with the same message; under DRAIN_LAW_ONE_LOOP it is A, finite and > 0, with
    no upper bound (DRAIN_ALPHA_CAP clamps). A = 0 is natural mode, asked for as the fixed law at alpha = 0.
    Any other drain_law is a ValueError, and so is an alpha the law refuses.
    """
    if drain_law == DRAIN_LAW_FIXED:
        if not (0.0 <= alpha < 1.0):
            raise ValueError(
                f"{context}: alpha must satisfy 0 <= alpha < 1, not {alpha!r}"
            )
        return
    if drain_law == DRAIN_LAW_ONE_LOOP:
        if not (alpha > 0.0 and isfinite(alpha)):
            raise ValueError(
                f"{context}: under the one-loop drain law alpha carries A, which must be finite and "
                f"> 0 (A = 0 is natural mode, the fixed law at alpha = 0), not {alpha!r}"
            )
        return
    raise ValueError(
        f"{context}: drain_law must be one of {DRAIN_LAWS}, not {drain_law!r}"
    )


def check_drain_model(drain_model: str, value: float, context: str) -> str:
    """
    The pair (value, drain_model) of an alpha_value: check_drain_law under the model's law (law_of), with
    context beginning each message. Returns the law. A ValueError for an unknown model or a value the law
    refuses.
    """
    law = law_of(drain_model, context)
    check_drain_law(law, value, context)
    return law
