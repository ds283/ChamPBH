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
What the plot scripts read from a store before they look up any history (read-only-plots
prompt 02; README P1, U3, U4).

`plot_by_beta.py` and `plot_ScalarHistory.py` open their store read-only
(`ShardedPool(read_only=True)`) and call these functions in place of the setup each used to
carry. A read-only pool refuses a lookup that would insert a row: `ReadOnlyMiss` for a replicated
class, `ReadOnlyWrite` for a sharded one. What a refusal means depends on the lookup:

- **a parameter** (the tolerances, the tuning tolerance, `T_init`, `phi_init`, `pi_init`, the
  cosmology): the plot run was given options the build did not use, and no history in the store
  can match them. The refusal becomes `PlotOptionNotStored`, whose message names the option (or
  the constant) and the value; the scripts print it to stderr and exit 2, before any figure (U4);
- **a model** (an `ExponentialModel`, one per stored M with each stored beta at n = 1): the
  combination was never stored, so no history can use it. It is skipped, with one warning line
  each and a count; every other combination is returned, M outer and beta inner (U3). Each is
  looked up on its own, and each reference is read on its own, so that one miss cannot hide the
  others (P2).

Since jordan-normalization prompt 07 a stored history is keyed by its model, its initial data and
the tuning tolerance, and ends at T_CMB: the store holds no Lambda and no final temperature, so
there is no potential, coupling or `T_stop` to look up.

Any other exception propagates unchanged. Under Ray an actor's exception reaches `ray.get` as
`RayTaskError(...).as_instanceof_cause()`, an instance of the cause's class whose attributes are
the cause's, so the same `except` clauses catch it there.

The payload of every lookup is the one the scripts built before this module existed, key for key,
so that a store `main.py` built still matches. This module parses nothing and starts no Ray at
import; it calls `ray.get` as the scripts do, so a test drives it inside the stand-in cluster
(`StoreFixtures/standin_pool.py`).
"""

import itertools
from typing import List, NamedTuple

import ray
from datastorekit.replication import ReadOnlyMiss, ReadOnlyWrite

from MetadataConcepts import tolerance
from config.model_list import build_model_list

# the model class each --potential-type looks up (jordan-normalization prompt 07)
MODEL_CLASSES = {
    "Exponential": "ExponentialModel",
}

# the model's exponent n, the only one main.py builds
MODEL_N = 1


class PlotOptionNotStored(RuntimeError):
    """
    A parameter row the plot run needs is not in the store: the store's build (`main.py`) was not
    run with this option, or this constant, so no history in the store can match.

    `class_name` is the class that was looked up, `option` the option or constant it came from,
    and `value` the value, as given. The `ReadOnlyMiss` the pool raised is chained as `__cause__`.
    """

    def __init__(self, class_name: str, option: str, value: str):
        self.class_name = class_name
        self.option = option
        self.value = value
        super().__init__(
            f"!! plot: the store holds no {class_name} for {option} {value}; main.py was not "
            f"run with it, so no history in this store can match. Give the plot the options the "
            f"build used"
        )


def _value(x) -> str:
    return f"{x:.10g}"


def _get_parameter(ref, class_name: str, option: str, value: str):
    """Read one parameter lookup; a read-only miss of ``class_name`` is PlotOptionNotStored."""
    try:
        return ray.get(ref)
    except ReadOnlyMiss as e:
        if e.class_name != class_name:
            raise
        raise PlotOptionNotStored(class_name, option, value) from e


class PlotParameters(NamedTuple):
    atol: object
    rtol: object
    T_init: object
    phi_init: object
    pi_init: object
    tuning_tol: object


def parameters(
    pool,
    abs_tol: float,
    rel_tol: float,
    T_init_GeV: float,
    phi_init_Mp: float,
    units,
    tuning_tol: float,
) -> PlotParameters:
    """
    The tolerances, `T_init`, `phi_init` and `pi_init` (README §0.3 rows 1-5), from `--abs-tol`,
    `--rel-tol`, `--T-init-GeV`, `--phi-init-Mp` and the constant pi = 0, and the tuning tolerance
    (jordan-normalization prompt 07), which the scripts give as the constant
    `DEFAULT_TUNING_TOLERANCE` that main.py builds with. Every lookup is submitted first; each is
    then read in this order, and the first that is not stored raises `PlotOptionNotStored`.
    """
    lookups = [
        (
            pool.object_get(tolerance, tol=abs_tol),
            "tolerance",
            "--abs-tol",
            _value(abs_tol),
        ),
        (
            pool.object_get(tolerance, tol=rel_tol),
            "tolerance",
            "--rel-tol",
            _value(rel_tol),
        ),
        (
            pool.object_get("temperature", value=T_init_GeV * units.GeV, units=units),
            "temperature",
            "--T-init-GeV",
            _value(T_init_GeV),
        ),
        (
            pool.object_get(
                "phi_value",
                value=phi_init_Mp * units.PlanckMass,
                units=units,
            ),
            "phi_value",
            "--phi-init-Mp",
            _value(phi_init_Mp),
        ),
        (
            pool.object_get("pi_value", value=0.0, units=units),
            "pi_value",
            "the constant initial momentum",
            "pi = 0",
        ),
        (
            pool.object_get(tolerance, tol=tuning_tol),
            "tolerance",
            "the constant tuning tolerance",
            _value(tuning_tol),
        ),
    ]
    return PlotParameters(*(_get_parameter(*lookup) for lookup in lookups))


class StoredGrid(NamedTuple):
    beta: list
    M: list


def stored_grid(pool, units) -> StoredGrid:
    """Every stored beta and M, by `read_table` (README §0.3 row 17), each in ascending order of
    value as the factories' `read_table` returns it."""
    return StoredGrid(
        beta=ray.get(pool.read_table("beta_value")),
        M=ray.get(pool.read_table("M_value", units)),
    )


def exponential_models(
    pool,
    potential_type: str,
    M_table: list,
    beta_table: list,
    units,
) -> list:
    """
    The `--potential-type` model of every stored M with every stored beta at n = 1
    (jordan-normalization prompt 07, S6), in `itertools.product(M_table, beta_table)` order (M
    outer, beta inner), as main.py builds its grid. Each combination is looked up on its own and
    each reference read on its own (P2). The class is sharded by beta, so a read-only miss is the
    inserter's `ReadOnlyWrite` ("an insert" of `ExponentialModel`); that combination is skipped,
    with one warning line printed and a count line after the last (U3). Any other exception, a
    `ReadOnlyWrite` of any other operation or class included, propagates.
    """
    if potential_type not in MODEL_CLASSES:
        raise ValueError(f"Unknown potential type: {potential_type}")
    cls_name = MODEL_CLASSES[potential_type]

    combinations = list(itertools.product(M_table, beta_table))
    refs = [
        pool.object_get(
            cls_name, shard_key=beta, M=M, n=MODEL_N, beta=beta, units=units
        )
        for M, beta in combinations
    ]

    found = []
    skipped = 0
    for (M, beta), ref in zip(combinations, refs):
        try:
            found.append(ray.get(ref))
        except ReadOnlyWrite as e:
            if e.operation != "an insert" or e.class_name != cls_name:
                raise
            skipped += 1
            print(
                f"!! plot: no {cls_name} stored for M={_value(M.value / units.eV)} eV, "
                f"n={MODEL_N}, β={_value(beta.value)}; no history can use it; skipped"
            )
    if skipped > 0:
        print(
            f"!! plot: {skipped} of {len(combinations)} {cls_name} combinations of the stored M "
            f"and β are not stored, and were skipped"
        )
    return found


def models(pool, units) -> List[dict]:
    """The cosmologies of `config.model_list.build_model_list` (README §0.3 row 8). Its constant
    `QCD_Cosmology` not stored raises `PlotOptionNotStored`."""
    try:
        return build_model_list(pool, units)
    except ReadOnlyMiss as e:
        if e.class_name != "QCD_Cosmology":
            raise
        raise PlotOptionNotStored(
            "QCD_Cosmology",
            "the constant cosmology",
            "Planck2018 of config/model_list.py",
        ) from e
