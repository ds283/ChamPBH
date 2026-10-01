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
Which work main.py's adiabatic and BBN stages schedule (run-integrity prompt 03).

Both stages look up the ScalarModel for each (potential, coupling) pair in a shard
bin, query the downstream target only for the models that did not fail, and then
decide which of those queries found nothing. Before prompt 03 the downstream
results were zipped against the unfiltered bin, so after a failed ScalarModel
every result was paired with the model before it. The helpers here keep each
lookup result with the (potential, coupling, model) it was asked for.

This module is pure: it imports neither main.py, Ray nor a datastore, so a test
can reach the decision directly. The lookup results are read only through their
`available` and `failure` attributes.
"""

from dataclasses import dataclass
from typing import Any, List, Sequence, Tuple


@dataclass(frozen=True)
class QueryEntry:
    """One (potential, coupling) pair of a shard bin, with its ScalarModel lookup result."""

    potential: Any
    coupling: Any
    model: Any


@dataclass(frozen=True)
class QueryEntries:
    """
    The entries of one shard bin whose ScalarModel exists and did not fail.

    `unavailable` holds the (potential, coupling) pairs whose ScalarModel lookup found
    no row; `skipped_failed_models` counts those whose ScalarModel is a stored failure,
    which are left out of `entries`.
    """

    entries: List[QueryEntry]
    unavailable: List[Tuple[Any, Any]]
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


def build_query_entries(
    pairs: Sequence[Tuple[Any, Any]], model_results: Sequence[Any]
) -> QueryEntries:
    """
    Pair each (potential, coupling) with its ScalarModel lookup result, in order, and
    keep the entries whose model exists and did not fail.

    :param pairs: the (potential, coupling) pairs of one shard bin, in the order the
        ScalarModel lookups were made
    :param model_results: the ScalarModel lookup results, one per pair, in the same order
    :raises ValueError: if the two lengths differ
    """
    _check_lengths("ScalarModel lookup", len(pairs), len(model_results))

    entries = []
    unavailable = []
    skipped_failed_models = 0

    for (potential, coupling), model in zip(pairs, model_results):
        if not model.available:
            unavailable.append((potential, coupling))
        elif model.failure:
            skipped_failed_models += 1
        else:
            entries.append(
                QueryEntry(potential=potential, coupling=coupling, model=model)
            )

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
    Decide which entries are missing their downstream result.

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
