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

- **a parameter** (the tolerances, the tuning tolerance, `T_init`, `phi_init`, `pi_init`, α, the
  cosmology): the plot run was given options the build did not use, and no history in the store
  can match them. The refusal becomes `PlotOptionNotStored`, whose message names the option (or
  the constant) and the value; the scripts print it to stderr and exit 2, before any figure (U4);
- **a model** (an `ExponentialModel`, one per stored M with each `--n-values` n and each stored
  beta): the combination was never stored, so no history can use it. It is skipped, with one
  warning line each and a count; every other combination is returned, M outer, n, and beta inner
  (U3). Each is looked up on its own, and each reference is read on its own, so that one miss
  cannot hide the others (P2). An n with no stored model at any stored (M, beta) is a parameter
  the build was not run with: `PlotOptionNotStored`, naming `--n-values` (jordan-normalization
  prompt 08).

Since jordan-normalization prompt 07 a stored history is keyed by its model, its initial data and
the tuning tolerance, and since inelastic-reflection prompt 09 by α too (`--alpha-values`, since
prompt 10), and ends at T_CMB: the store holds no Lambda and no final temperature, so
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
from typing import List, NamedTuple, Sequence

import ray
from datastorekit.replication import ReadOnlyMiss, ReadOnlyWrite

from CosmologyConcepts.drain_model import DRAIN_MODEL_ONE_LOOP
from MetadataConcepts import tolerance
from config.model_list import build_model_list

# the one model class main.py builds (jordan-normalization prompt 08 removed --potential-type)
MODEL_CLASS = "ExponentialModel"


class PlotOptionNotStored(RuntimeError):
    """
    A parameter row the plot run needs is not in the store: the store's build (`main.py`) was not
    run with this option, or this constant, so no history in the store can match.

    `class_name` is the class that was looked up, `option` the option or constant it came from,
    and `value` the value, as given. The `ReadOnlyMiss` the pool raised is chained as `__cause__`;
    an `--n-values` n with no stored model (`exponential_models`) has no single miss to chain.
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
    # one `alpha_value` row per distinct --alpha-values value, then one per distinct --A-values value, in
    # the drivers' order (a tuple): the fixed rows first, each row carrying its `drain_model`
    # (one-loop-drain prompt 04, P34)
    alphas: tuple


def parameters(
    pool,
    abs_tol: float,
    rel_tol: float,
    T_init_GeV: float,
    phi_init_Mp: float,
    units,
    tuning_tol: float,
    alpha_values: Sequence[float],
    A_values: Sequence[float] = (),
) -> PlotParameters:
    """
    The tolerances, `T_init`, `phi_init` and `pi_init` (README §0.3 rows 1-5), from `--abs-tol`,
    `--rel-tol`, `--T-init-GeV`, `--phi-init-Mp` and the constant pi = 0, the tuning tolerance
    (jordan-normalization prompt 07), from `--tuning-tolerance` (prompt 08), and one row per
    alpha of `alpha_values` (`--alpha-values`, deduplicated by the caller), a key of every history
    (inelastic-reflection prompt 09, P30; prompt 10, P34). Every lookup is submitted first; each is
    then read in this order, the alphas last and in the order given, and the first that is not
    stored raises `PlotOptionNotStored`. `alphas` is the tuple of the alpha rows.

    One-loop-drain prompt 04 (P33, P34) adds `A_values` (`--A-values`, deduplicated by the caller; none by
    default): one row per A, looked up under `DRAIN_MODEL_ONE_LOOP`, the code's current one-loop model, after
    the alpha rows and in the order given. `alphas` is then the alpha rows (model `fixed`, no model named in
    the lookup, as before) followed by the A rows. An A with no row under that model is `PlotOptionNotStored`
    naming `--A-values`, the value and the model, even when the same value is stored under `fixed`; an alpha
    with no `fixed` row is refused as before, even when the value is stored under the one-loop model.

    Under `--no-alpha-values` the parser has emptied `--alpha-values`, so `alpha_values` is empty and `alphas`
    is the A rows alone; no fixed row is looked up (no-alpha-values, 2026-11-07).
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
            "--tuning-tolerance",
            _value(tuning_tol),
        ),
    ] + [
        (
            pool.object_get("alpha_value", value=a),
            "alpha_value",
            "--alpha-values",
            _value(a),
        )
        for a in alpha_values
    ]
    # then the A rows, under the code's current one-loop model, in the order given
    lookups += [
        (
            pool.object_get("alpha_value", value=A, drain_model=DRAIN_MODEL_ONE_LOOP),
            "alpha_value",
            "--A-values",
            f"{_value(A)} ({DRAIN_MODEL_ONE_LOOP})",
        )
        for A in A_values
    ]
    rows = [_get_parameter(*lookup) for lookup in lookups]
    return PlotParameters(*rows[:6], tuple(rows[6:]))


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
    M_table: list,
    beta_table: list,
    n_values: list,
    units,
) -> list:
    """
    The `ExponentialModel` of every stored M with every n of `n_values` (`--n-values`) and every
    stored beta (jordan-normalization prompt 07, S6; prompt 08 gives n its option), in
    `itertools.product(M_table, n_values, beta_table)` order (M outer, n, beta inner), as main.py
    builds its grid. Each combination is looked up on its own and each reference read on its own
    (P2). The class is sharded by beta, so a read-only miss is the inserter's `ReadOnlyWrite` ("an
    insert" of `ExponentialModel`); that combination is skipped, with one warning line printed and
    a count line after the last (U3). Any other exception, a `ReadOnlyWrite` of any other operation
    or class included, propagates.

    An n with no stored model at any stored (M, beta) was not built: after the count line,
    `PlotOptionNotStored("ExponentialModel", "--n-values", n)` is raised for the first such n, as
    for a parameter (prompt 08). With no stored (M, beta) there is nothing to judge, and nothing is
    raised.
    """
    combinations = list(itertools.product(M_table, n_values, beta_table))
    refs = [
        pool.object_get(MODEL_CLASS, shard_key=beta, M=M, n=n, beta=beta, units=units)
        for M, n, beta in combinations
    ]

    found = []
    found_n = set()
    skipped = 0
    for (M, n, beta), ref in zip(combinations, refs):
        try:
            found.append(ray.get(ref))
            found_n.add(n)
        except ReadOnlyWrite as e:
            if e.operation != "an insert" or e.class_name != MODEL_CLASS:
                raise
            skipped += 1
            print(
                f"!! plot: no {MODEL_CLASS} stored for M={_value(M.value / units.eV)} eV, "
                f"n={n}, β={_value(beta.value)}; no history can use it; skipped"
            )
    if skipped > 0:
        print(
            f"!! plot: {skipped} of {len(combinations)} {MODEL_CLASS} combinations of the stored "
            f"M and β are not stored, and were skipped"
        )
    if len(M_table) > 0 and len(beta_table) > 0:
        for n in n_values:
            if n not in found_n:
                raise PlotOptionNotStored(MODEL_CLASS, "--n-values", str(n))
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
