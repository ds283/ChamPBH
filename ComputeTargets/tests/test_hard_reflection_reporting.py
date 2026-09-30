"""
Prompt 01 (production-readiness): the hard-reflection count is stored, read and captioned
under one name.

No Ray cluster, no datastore and no solve is needed.
"""

import json
import unittest
from collections import namedtuple
from types import SimpleNamespace
from typing import Optional

import matplotlib

matplotlib.use("Agg")
from matplotlib import pyplot as plt

from ComputeTargets.ScalarModel import HARD_REFLECTIONS_KEY, build_extra_data
from extract_common import add_ScalarModel_labels, hard_reflection_count

RHSStats = namedtuple("RHSStats", ["a", "b", "c"])


def reference_extra_data(data: dict) -> Optional[dict]:
    """Verbatim copy of the store_attr block that ScalarModel.store() held before it was factored."""
    extra_data = {}

    def store_attr(src_attr: str, dest_attr: str, min_value: Optional[int] = None):
        value = data[src_attr]

        if min_value is None or value > min_value:
            extra_data[dest_attr] = value

    store_attr("hard_reflections", "number_hard_reflections", 0)
    store_attr("level_1_entries", "number_level_1_entries", 0)
    store_attr("level_1_exits", "number_level_1_exits", 0)
    store_attr("level_2_entries", "number_level_2_entries", 0)
    store_attr("level_2_exits", "number_level_2_exits", 0)
    store_attr("level_1_boundary", "level_1_boundary")
    store_attr("level_2_boundary", "level_2_boundary")
    store_attr("level_1_max_step", "level_1_max_step")
    store_attr("level_2_max_step", "level_2_max_step")
    store_attr("number_fragments", "number_fragments", 1)

    largest_RHS_values = data["largest_RHS_values"]
    smallest_RHS_values = data["smallest_RHS_values"]
    mean_RHS_values = data["mean_RHS_values"]

    if largest_RHS_values is not None:
        extra_data["largest_RHS_values"] = largest_RHS_values._asdict()
    if smallest_RHS_values is not None:
        extra_data["smallest_RHS_values"] = smallest_RHS_values._asdict()
    if mean_RHS_values is not None:
        extra_data["mean_RHS_values"] = mean_RHS_values._asdict()

    if len(extra_data) > 0:
        return extra_data
    return None


def sample_payload(**overrides) -> dict:
    data = {
        "hard_reflections": 3,
        "level_1_entries": 4,
        "level_1_exits": 5,
        "level_2_entries": 6,
        "level_2_exits": 7,
        "level_1_boundary": 0.25,
        "level_2_boundary": 0.5,
        "level_1_max_step": 1e-3,
        "level_2_max_step": 1e-4,
        "number_fragments": 2,
        "largest_RHS_values": RHSStats(1.0, 2.0, 3.0),
        "smallest_RHS_values": RHSStats(-1.0, -2.0, -3.0),
        "mean_RHS_values": RHSStats(0.1, 0.2, 0.3),
    }
    data.update(overrides)
    return data


def stand_in_model(extra_data: Optional[dict]):
    return SimpleNamespace(
        solver=SimpleNamespace(label="stand-in solver"),
        _coupling=SimpleNamespace(name="stand-in coupling"),
        _potential=SimpleNamespace(name="stand-in potential"),
        extra_metadata=extra_data,
    )


def caption_texts(extra_data: Optional[dict]) -> list:
    fig = plt.figure()
    try:
        add_ScalarModel_labels(fig, stand_in_model(extra_data), "stand-in cosmology")
        return [t.get_text() for t in fig.texts]
    finally:
        plt.close(fig)


class TestHardReflectionReporting(unittest.TestCase):
    def test_a_builder_matches_the_old_block(self):
        cases = [
            sample_payload(),
            sample_payload(
                largest_RHS_values=None, smallest_RHS_values=None, mean_RHS_values=None
            ),
            sample_payload(hard_reflections=0, number_fragments=1),
            sample_payload(
                largest_RHS_values=None, mean_RHS_values=RHSStats(7.0, 8.0, 9.0)
            ),
        ]
        for data in cases:
            self.assertEqual(build_extra_data(data), reference_extra_data(data))

    def test_b_count_survives_the_store(self):
        stored = json.loads(json.dumps(build_extra_data(sample_payload())))
        self.assertEqual(hard_reflection_count(stored), 3)

        stored = json.loads(
            json.dumps(build_extra_data(sample_payload(hard_reflections=0)))
        )
        self.assertNotIn(HARD_REFLECTIONS_KEY, stored)
        self.assertEqual(hard_reflection_count(stored), 0)

    def test_b_reader_handles_no_extra_data(self):
        self.assertEqual(hard_reflection_count(None), 0)
        self.assertEqual(hard_reflection_count({}), 0)

    def test_c_caption_reports_the_stored_count(self):
        stored = json.loads(json.dumps(build_extra_data(sample_payload())))
        self.assertIn("Hard reflections: 3", caption_texts(stored))

    def test_c_caption_for_zero_is_unchanged(self):
        stored = json.loads(
            json.dumps(build_extra_data(sample_payload(hard_reflections=0)))
        )
        self.assertIn("Hard reflections: 0", caption_texts(stored))


if __name__ == "__main__":
    unittest.main()
