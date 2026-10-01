"""
Prompt 01 (integrator-remediation): the elastic-reflection count and the step-loop metadata are
stored, read and captioned under one name. Rewritten from production-readiness prompt 01's
test_hard_reflection_reporting.py, which pinned the region/fragment/hard-reflection block that
this prompt replaced.

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

from ComputeTargets.ScalarModel import REFLECTIONS_KEY, build_extra_data
from extract_common import add_ScalarModel_labels, reflection_count

RHSStats = namedtuple("RHSStats", ["a", "b", "c"])


def reference_extra_data(data: dict) -> Optional[dict]:
    """Verbatim copy of the store_attr block of build_extra_data (integrator-remediation prompt 01)."""
    extra_data = {}

    def store_attr(src_attr: str, dest_attr: str, min_value: Optional[int] = None):
        value = data[src_attr]

        if min_value is None or value > min_value:
            extra_data[dest_attr] = value

    store_attr("reflections", "number_reflections", 0)
    store_attr("cap_fraction", "cap_fraction")
    store_attr("cap_floor", "cap_floor")
    store_attr("cap_global_max_step", "cap_global_max_step")
    store_attr("jacobian_factor_max", "jacobian_factor_max")
    store_attr("accepted_steps", "accepted_steps")
    store_attr("steps_rejected_by_exception", "steps_rejected_by_exception", 0)

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
        "reflections": 3,
        "cap_fraction": 0.1,
        "cap_floor": 1e-11,
        "cap_global_max_step": 0.1,
        "jacobian_factor_max": 1e-4,
        "accepted_steps": 4476,
        "steps_rejected_by_exception": 2,
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


class TestReflectionReporting(unittest.TestCase):
    def test_a_builder_matches_the_block(self):
        cases = [
            sample_payload(),
            sample_payload(
                largest_RHS_values=None, smallest_RHS_values=None, mean_RHS_values=None
            ),
            sample_payload(reflections=0, steps_rejected_by_exception=0),
            sample_payload(
                largest_RHS_values=None, mean_RHS_values=RHSStats(7.0, 8.0, 9.0)
            ),
        ]
        for data in cases:
            self.assertEqual(build_extra_data(data), reference_extra_data(data))

    def test_a_no_region_or_fragment_keys(self):
        stored = build_extra_data(sample_payload())
        for key in (
            "number_hard_reflections",
            "number_level_1_entries",
            "number_level_1_exits",
            "number_level_2_entries",
            "number_level_2_exits",
            "level_1_boundary",
            "level_2_boundary",
            "level_1_max_step",
            "level_2_max_step",
            "number_fragments",
        ):
            self.assertNotIn(key, stored)

    def test_b_count_survives_the_store(self):
        stored = json.loads(json.dumps(build_extra_data(sample_payload())))
        self.assertEqual(reflection_count(stored), 3)

        stored = json.loads(json.dumps(build_extra_data(sample_payload(reflections=0))))
        self.assertNotIn(REFLECTIONS_KEY, stored)
        self.assertEqual(reflection_count(stored), 0)

    def test_b_reader_handles_no_extra_data(self):
        self.assertEqual(reflection_count(None), 0)
        self.assertEqual(reflection_count({}), 0)

    def test_c_caption_reports_the_stored_count(self):
        stored = json.loads(json.dumps(build_extra_data(sample_payload())))
        texts = caption_texts(stored)
        self.assertIn("Reflections (elastic model): 3", texts)
        self.assertFalse(any(t.startswith("Hard reflections") for t in texts))
        self.assertFalse(any(t.startswith("Solution fragments") for t in texts))

    def test_c_caption_for_zero(self):
        stored = json.loads(json.dumps(build_extra_data(sample_payload(reflections=0))))
        self.assertIn("Reflections (elastic model): 0", caption_texts(stored))


if __name__ == "__main__":
    unittest.main()
