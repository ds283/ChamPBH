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
Which work main.py's three stages schedule (run-integrity prompt 03; jordan-normalization
prompt 07).

The scalar stage looks up the ScalarHistory of each model in a shard bin and computes
only those whose lookup found no row (`missing_models`). Before jordan-normalization
prompt 07 its "which are missing?" filter kept every model whatever the lookup found,
so a fully stored batch was resubmitted and only the second lookup stopped the work
(`[02-scalar-stage-missing-filter-keeps-every-model]`,
`[03-step-1-first-pass-lookup-filters-nothing]`).

The adiabatic and BBN stages look up the ScalarHistory for each model in a shard bin,
query the downstream target only for the histories that did not fail, and then decide
which of those queries found nothing. Before run-integrity prompt 03 the downstream
results were zipped against the unfiltered bin, so after a failed ScalarHistory every
result was paired with the model before it. The helpers here keep each lookup result
with the (model, history) it was asked for.

This module is pure: it imports neither main.py, Ray nor a datastore, so a test
can reach the decision directly. The lookup results are read only through their
`available` and `failure` attributes.
"""

from dataclasses import dataclass
from math import exp, log
from typing import Any, Dict, List, Sequence, Tuple


@dataclass(frozen=True)
class QueryEntry:
    """One model of a shard bin, with its ScalarHistory lookup result (its history)."""

    model: Any
    history: Any


@dataclass(frozen=True)
class QueryEntries:
    """
    The entries of one shard bin whose ScalarHistory exists and did not fail.

    `unavailable` holds the models whose ScalarHistory lookup found no row;
    `skipped_failed_models` counts those whose ScalarHistory is a stored failure, which
    are left out of `entries`.
    """

    entries: List[QueryEntry]
    unavailable: List[Any]
    skipped_failed_models: int


@dataclass(frozen=True)
class MissingSelection:
    """
    The entries whose downstream result is to be computed.

    `stored_failures` counts entries whose downstream lookup returned a stored failure.
    Without `retry_failed` they are not in `missing` (computations skipped because of a
    stored failure); with it they are (computations retried).
    """

    missing: List[QueryEntry]
    stored_failures: int


def _check_lengths(what: str, expected: int, got: int) -> None:
    if expected != got:
        raise ValueError(
            f"pipeline_selection: {what}: {got} lookup results for {expected} entries. "
            f"Every lookup result must be paired with the entry it was asked for; "
            f"the lists are never truncated to the shorter."
        )


def missing_models(models: Sequence[Any], lookups: Sequence[Any]) -> List[Any]:
    """
    The scalar stage's first pass (jordan-normalization prompt 07): the models whose
    ScalarHistory lookup found no row, in order. A stored failure is not missing: the
    stage does not recompute it (its second lookup would find it and nothing would be
    computed). A fully stored batch gives [], so the stage schedules no work for it.

    :param models: the models of one shard bin, in the order the lookups were made
    :param lookups: the ScalarHistory lookup results, one per model, in the same order
    :raises ValueError: if the two lengths differ
    """
    _check_lengths("ScalarHistory lookup", len(models), len(lookups))
    return [model for model, history in zip(models, lookups) if not history.available]


def build_query_entries(
    models: Sequence[Any], history_results: Sequence[Any]
) -> QueryEntries:
    """
    Pair each model with its ScalarHistory lookup result, in order, and keep the entries
    whose history exists and did not fail.

    :param models: the models of one shard bin, in the order the ScalarHistory lookups
        were made
    :param history_results: the ScalarHistory lookup results, one per model, in the same
        order
    :raises ValueError: if the two lengths differ
    """
    _check_lengths("ScalarHistory lookup", len(models), len(history_results))

    entries = []
    unavailable = []
    skipped_failed_models = 0

    for model, history in zip(models, history_results):
        if not history.available:
            unavailable.append(model)
        elif history.failure:
            skipped_failed_models += 1
        else:
            entries.append(QueryEntry(model=model, history=history))

    return QueryEntries(
        entries=entries,
        unavailable=unavailable,
        skipped_failed_models=skipped_failed_models,
    )


def select_missing(
    entries: Sequence[QueryEntry],
    results: Sequence[Any],
    retry_failed: bool = False,
) -> MissingSelection:
    """
    Decide which entries are missing their downstream result: the entries are
    (model, history) pairs, and the result is the history's AdiabaticHistory or BBNData.

    An entry is missing if its lookup result is not available. With `retry_failed`, an
    entry whose result is a stored failure is missing too. A result with no `failure`
    attribute (AdiabaticHistory stores no failure rows) is never a stored failure.

    :param entries: the entries the downstream lookups were made for, in order
    :param results: the downstream lookup results, one per entry, in the same order
    :param retry_failed: count a stored failure as missing
    :raises ValueError: if the two lengths differ
    """
    _check_lengths("downstream lookup", len(entries), len(results))

    missing = []
    stored_failures = 0

    for entry, result in zip(entries, results):
        if not result.available:
            missing.append(entry)
        elif getattr(result, "failure", False):
            stored_failures += 1
            if retry_failed:
                missing.append(entry)

    return MissingSelection(missing=missing, stored_failures=stored_failures)


NO_FAILURE_REASON = "no failure_reason stored"


def summarise_failure_reasons(reasons: Sequence[Any]) -> List[Tuple[str, int]]:
    """
    Group failure reasons by their first clause, the text before the first ':', and count
    each group. A reason of None or "" is counted under NO_FAILURE_REASON. The groups come
    back most frequent first, ties in alphabetical order, so that the summary is stable.
    (science-readiness prompt 02)

    :param reasons: the failure_reason of each failed row
    """
    counts = {}
    for reason in reasons:
        clause = "" if reason is None else str(reason).split(":", 1)[0].strip()
        if clause == "":
            clause = NO_FAILURE_REASON
        counts[clause] = counts.get(clause, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def _seed_and_log_Omega(model: Any, phi: float, cosmology: Any) -> Tuple[Any, float]:
    """
    The seed of `model` on `cosmology` (`model.seed(cosmology)`, the start of its tuning; analytic
    and cheap), and ln Omega(phi) of the model realized at the seed's phi_ref (the normalization
    does not enter Omega). For `ExponentialModel`, ln Omega(phi) = beta (phi - phi_ref)/M_P.
    (jordan-normalization prompt 08, P11; before it, Omega was taken at phi_ref = 0.)
    """
    seed = model.seed(cosmology)
    return seed, model.realize(None, seed.phi_ref).log_Omega(phi)


def super_planckian_couplings(
    models: Sequence[Any], phi_init: Any, T_init: Any, units: Any, cosmology: Any
) -> List[Any]:
    """
    The models for which the run starts super-Planckian: those with
    ln Omega(phi*) + ln T* > ln M_P, that is Omega(phi*) T* > M_P (review H8; the Jordan
    temperature T* times Omega is the Einstein-frame temperature scale, which is compared
    with the reduced Planck mass `units.PlanckMass`). (science-readiness prompt 04)

    Omega is the model's conformal factor A, which is 1 at phi_ref. Since jordan-normalization
    prompt 08 (P11) it is evaluated at the phi_ref of the model's seed on `cosmology`
    (`model.seed(cosmology)`), which is known before any history runs; the tuned phi_ref, which a
    stored history holds, differs from it (see `warn_super_planckian`). For `ExponentialModel`,
    ln Omega(phi*) = beta (phi* - phi_ref)/M_P, so the test is

        beta (phi* - phi_ref)/M_P > ln(M_P/T*).

    With the reduced Planck mass `Units.base.REDUCED_PLANCK_MASS_EV` = 2.4353e27 eV and
    T* = 2e4 GeV, ln(M_P/T*) = 32.4331. At phi_ref = 0 that selects beta > 6.4866 at phi* = 5 M_P
    and beta > 32.4331 at phi* = 1 M_P. With the seed's phi_ref at M = 1e-3 M_P on Planck2018
    (a tracking seed, phi_ref = 0.0149 M_P at the threshold for phi* = 5 and 0.0055 M_P for
    phi* = 1) the thresholds are beta > 6.5060 at phi* = 5 M_P and beta > 32.613 at phi* = 1 M_P.
    phi_ref grows with M (0.40 M_P for the static seed at M = 0.5 M_P near beta = 7), and a
    larger phi_ref raises the threshold: 7.045 at phi* = 5 M_P and 39.00 at phi* = 1 M_P for
    M = 0.5 M_P. (Computed with the seeds of this tree, jordan-normalization prompt 08.)

    The models come back in their original order, as the same objects; nothing is
    removed from `models`.

    :param phi_init: a phi_value (or float) in the cosmology's units
    :param T_init: a temperature (or float) in the cosmology's units
    :param cosmology: the cosmology whose seed each model's phi_ref is taken from
    """
    from CosmologyConcepts.FieldValues import GetFieldValue
    from CosmologyConcepts.temperature import GetTemperature

    phi = GetFieldValue(phi_init)
    ln_T = log(GetTemperature(T_init))
    ln_Mp = log(units.PlanckMass)
    return [
        m for m in models if _seed_and_log_Omega(m, phi, cosmology)[1] + ln_T > ln_Mp
    ]


@dataclass(frozen=True)
class BBNProvenance:
    """
    The provenance of the BBNData rows a driver read (bbn-tolerance prompt 02).

    `foreign` maps each (PRyM_version, small_network) pair that differs from the
    driver's own to the number of successful rows carrying it. `not_stored` counts the
    failure rows, which store neither column, so their provenance cannot be told.
    `current` counts the successful rows that match. Rows not found in the store are
    not counted.
    """

    foreign: Dict[Tuple[Any, Any], int]
    not_stored: int
    current: int


BBN_REFRESH_ROUTE = (
    "to refresh BBN, copy the store with "
    "`python -m datastorekit.tools.sharded_store copy SRC DST` "
    "and run main.py on the copy with --drop bbn-data"
)


def foreign_bbn_provenance(
    bbn_objects: Sequence[Any], prym_version: str, small_network: bool
) -> BBNProvenance:
    """
    Count the BBNData rows whose (PRyM_version, small_network) differs from
    (`prym_version`, `small_network`), grouped by pair. BBNData lookups are keyed on the
    version label only, so a store that was not refreshed after a PRyMordial patch, or
    after a change of network, serves such rows silently (bbn-tolerance README section
    0.2 P6, U4). Only objects found in the store are considered, and `failure` is read
    before either property, since both raise on a failure row. Nothing is filtered.

    :param bbn_objects: BBNData lookup results, with or without _do_not_populate
    :param prym_version: the driver's PRYM_VERSION
    :param small_network: the network the driver runs
    """
    foreign = {}
    not_stored = 0
    current = 0
    for obj in bbn_objects:
        if not obj.available:
            continue
        if obj.failure:
            not_stored += 1
            continue
        pair = (obj.PRyM_version, obj.small_network)
        if pair == (prym_version, small_network):
            current += 1
        else:
            foreign[pair] = foreign.get(pair, 0) + 1
    return BBNProvenance(foreign=foreign, not_stored=not_stored, current=current)


def warn_foreign_bbn_provenance(
    bbn_objects: Sequence[Any], prym_version: str, small_network: bool, emit=print
) -> None:
    """
    If any successful BBNData row was made by another PRyMordial version or network
    (see `foreign_bbn_provenance`), print one warning: a line per foreign pair with its
    count, the count of failure rows whose provenance is not stored, and the refresh
    route. Print nothing otherwise. The rows are used as before; this only warns
    (bbn-tolerance README section 0.2 P6, U4).

    :param emit: called with each line; `print` by default
    """
    p = foreign_bbn_provenance(bbn_objects, prym_version, small_network)
    if not p.foreign:
        return None
    network = "small" if small_network else "full"
    emit(
        f"!! warning: {sum(p.foreign.values())} stored BBNData row(s) were not made by this "
        f"code's PRyMordial (PRyM_version={prym_version}, {network} network); they are used "
        f"as stored"
    )
    for (version, small), count in sorted(
        p.foreign.items(), key=lambda item: (-item[1], str(item[0]))
    ):
        foreign_network = {True: "small", False: "full"}.get(small, repr(small))
        emit(
            f"!! warning:   {count} x PRyM_version={version}, {foreign_network} network"
        )
    emit(
        f"!! warning:   {p.not_stored} failure row(s) store no provenance and cannot be "
        f"classified"
    )
    emit(f"!! warning: {BBN_REFRESH_ROUTE}")
    return None


def warn_super_planckian(
    models: Sequence[Any],
    phi_init: Any,
    T_init: Any,
    units: Any,
    cosmology: Any,
    emit=print,
) -> Sequence[Any]:
    """
    Print one warning per super-Planckian model (see `super_planckian_couplings`) and the
    count, and return `models` itself, unchanged. A super-Planckian start is warned about
    and computed, never skipped or refused (the user's ruling, science-readiness P6).
    (science-readiness prompt 04)

    Omega is evaluated at the phi_ref of the model's seed on `cosmology` (jordan-normalization
    prompt 08, P11), and each line says so, naming the seed's kind and its phi_ref. The tuned
    phi_ref differs from the seed's, and moves ln Omega(phi*) by -beta (phi_ref - phi_ref_seed)/M_P;
    the count line gives the sizes measured by prompt 06 (`logs/06-probes/*_secant.log`, the first
    and last phi_ref of each tuning). Since the grid is (M, n, beta), a flagged beta is warned
    about once per (M, n).

    :param cosmology: the cosmology whose seed each model's phi_ref is taken from
    :param emit: called with each line; `print` by default
    """
    from CosmologyConcepts.FieldValues import GetFieldValue
    from CosmologyConcepts.temperature import GetTemperature

    phi = GetFieldValue(phi_init)
    ln_T = log(GetTemperature(T_init))
    ln_Mp = log(units.PlanckMass)
    Mp = units.PlanckMass
    flagged = 0
    for m in models:
        seed, log_Omega = _seed_and_log_Omega(m, phi, cosmology)
        if not log_Omega + ln_T > ln_Mp:
            continue
        flagged += 1
        ratio = exp(log_Omega + ln_T - ln_Mp)
        beta = getattr(m, "_beta_float", None)
        M = getattr(m, "_M_float", None)
        n = getattr(m, "_n", None)
        beta_text = f"{beta:.5g}" if beta is not None else "?"
        M_text = f"{M / Mp:.5g} M_P" if M is not None else "?"
        n_text = f"{n}" if n is not None else "?"
        emit(
            f"!! warning: beta={beta_text}, M={M_text}, n={n_text}, "
            f"phi*={phi / Mp:.5g} M_P: Omega(phi*) T* = {ratio:.5g} M_P "
            f"(super-Planckian start), with Omega at the seed's phi_ref = "
            f"{seed.phi_ref / Mp:.5g} M_P ({seed.kind} seed)"
        )
    if flagged:
        emit(
            f"!! warning: {flagged} of {len(models)} models start super-Planckian at the seed's "
            f"phi_ref; they are computed like the others. The tuned phi_ref differs from the "
            f"seed's, moving ln Omega(phi*) by -beta (phi_ref - phi_ref_seed)/M_P, in size up to "
            f"1.4e-3 at M = 1e-3 M_P with beta <= 5.5, 6.5e-3 at beta = 25, and up to 1.0 at "
            f"M = 0.5 M_P from a static seed (jordan-normalization prompt 06's points)"
        )
    return models
