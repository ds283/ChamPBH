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
them from here, so that every `SM.<name>` still resolves to the same object. The law's fitted constants, the S
table and the law's long comment stay in ComputeTargets/ScalarHistory.py.

- the laws (one-loop-drain prompt 01; README §8.2, §8.8 P22): DRAIN_LAW_FIXED, alpha_eff = alpha w_gate at
  every candidate, 0 <= alpha < 1, the law to VERSION_LABEL 2026.9.0, the default; DRAIN_LAW_ONE_LOOP, alpha
  carries the law's strength A, finite and > 0, and each candidate drains by the measured law of
  ComputeTargets/ScalarHistory.py (drain_alpha_effective).
- the drain models (README §8.10, decision 1; §8.11 P27): the value of `alpha_value.drain_model`, a key of every
  history. DRAIN_MODEL_FIXED is the fixed law; DRAIN_MODEL_ONE_LOOP names the measured law and its version, the
  exponential family's (the L law of ComputeTargets/ScalarHistory.py); DRAIN_MODEL_ONE_LOOP_PL (power-law-wall
  prompt 03; README §6.6 P17) names the power wall's one-loop law, f_n(ln R) and R S_n, which
  CosmologyConcepts/Models/InversePowerModel.py defines. Which one-loop model a history takes is its model's
  (AbstractModel.one_loop_drain_model).
- law_of: the one mapping from a model to its law; check_drain_law: the rules for the value under a law, the
  one statement of them.
- the regimes of a candidate under the one-loop law (REGIME_*, DRAIN_REGIMES), defined here since power-law-wall
  prompt 03 so that a model's own fraction (AbstractModel._drain_fraction) can name them without importing
  ComputeTargets.ScalarHistory, which imports them from here, so that every SM.REGIME_* resolves to the same object.
"""

from math import isfinite

DRAIN_LAW_FIXED = "fixed"
DRAIN_LAW_ONE_LOOP = "one_loop"
DRAIN_LAWS = (DRAIN_LAW_FIXED, DRAIN_LAW_ONE_LOOP)

# the drain model, named in the "drain" record (README §8.10, decision 1) and stored as alpha_value's
# drain_model, beside its value (one-loop-drain prompt 03; README §8.11 P27). DRAIN_MODEL_ONE_LOOP names the
# measured law and its version. THE VERSION MOVES whenever drain_fraction, its constants or the S table
# (ComputeTargets/ScalarHistory.py) change, so that a new version is a new key value: a lookup under the new
# string finds no history of the old version, as a lookup under a new VERSION_LABEL finds none of the old label
#
# DRAIN_MODEL_ONE_LOOP_PL (power-law-wall prompt 03; README §6.5 R7, §6.6 P17) is the power wall's one-loop law,
# f_n(ln R) above the classical branch R S_n, defined at n = 1 and 2 by InversePowerModel, whose constants it versions:
# THE VERSION MOVES whenever they change. It joins DRAIN_MODELS after the two there, so that their order (and
# extract_common's sort by it) is unchanged for the exponential
DRAIN_MODEL_FIXED = "fixed"
DRAIN_MODEL_ONE_LOOP = "one-loop-measured-v1"
DRAIN_MODEL_ONE_LOOP_PL = "one-loop-measured-pl-v1"
DRAIN_MODELS = (DRAIN_MODEL_FIXED, DRAIN_MODEL_ONE_LOOP, DRAIN_MODEL_ONE_LOOP_PL)

# the law each model is integrated under (the one mapping; one-loop-drain prompt 03 §1.1)
_LAW_OF_MODEL = {
    DRAIN_MODEL_FIXED: DRAIN_LAW_FIXED,
    DRAIN_MODEL_ONE_LOOP: DRAIN_LAW_ONE_LOOP,
    DRAIN_MODEL_ONE_LOOP_PL: DRAIN_LAW_ONE_LOOP,
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
    DRAIN_MODEL_ONE_LOOP and DRAIN_MODEL_ONE_LOOP_PL. Any other model is a ValueError, whose message begins with
    context.
    """
    try:
        return _LAW_OF_MODEL[drain_model]
    except (KeyError, TypeError):
        raise ValueError(
            f"{context}: drain_model must be one of {DRAIN_MODELS}, not {drain_model!r}"
        ) from None


# the inverse of law_of, the one mapping from a law to its current model (one-loop-drain prompt 04, orchestrator
# note 04 ruling 4): the model a driver's or a tool's law option names. DRAIN_MODEL_ONE_LOOP is the code's current
# one-loop model, so a version bump moves this and nothing typed at a call site. Written out since power-law-wall
# prompt 03 (P17), when two models came to share the one-loop law: it still returns DRAIN_MODEL_ONE_LOOP, the
# exponential family's, for that law. A history's one-loop model is its model's (AbstractModel.one_loop_drain_model);
# the call sites that name this mapping or DRAIN_MODEL_ONE_LOOP (the drivers, the plot scripts, the argument parser and
# the store tools) are made model-aware by power-law-wall prompts 05 and 06
_MODEL_OF_LAW = {
    DRAIN_LAW_FIXED: DRAIN_MODEL_FIXED,
    DRAIN_LAW_ONE_LOOP: DRAIN_MODEL_ONE_LOOP,
}


def model_of_law(drain_law: str, context: str = "model_of_law") -> str:
    """
    The code's current drain model of a drain law, the inverse of law_of: DRAIN_MODEL_FIXED for DRAIN_LAW_FIXED
    and DRAIN_MODEL_ONE_LOOP for DRAIN_LAW_ONE_LOOP. Any other law is a ValueError, whose message begins with
    context.
    """
    try:
        return _MODEL_OF_LAW[drain_law]
    except (KeyError, TypeError):
        raise ValueError(
            f"{context}: drain_law must be one of {DRAIN_LAWS}, not {drain_law!r}"
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
