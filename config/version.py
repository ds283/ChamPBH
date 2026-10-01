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

# The version label under which every driver script (main.py, plot_by_beta.py,
# plot_ScalarModel.py) opens the datastore. It is defined here once, and imported.

# Stores made under an earlier VERSION_LABEL are invalid and must not be reused: on 2026-09-29
# (review-remediation prompt 02) the Jordan-frame temperature law lost a spurious factor ln 10 in
# its entropy term and the low-temperature g_rho, g_S limits changed, so every stored history differs.
# On 2026-09-30 (production-readiness prompt 02) the small_network switch was wired to the flag
# PRyMordial reads: from 2026.3.0 the stored small_network describes the network that ran, and
# production runs the full network (small_network=False).
# On 2026-09-30 (production-readiness prompt 03), under the same label: from 2026.3.0 the
# AdiabaticHistory effective mass M^2_eff includes the response of the source to delta phi, and
# Q's numerator is computed in a form that is smooth where M^2_eff changes sign.
# On 2026-09-30 (run-integrity prompt 01), under the same label: lookups of ScalarModel,
# AdiabaticHistory and BBNData return only rows made under this label, and the label is defined
# here once.
# On 2026-09-30 (run-integrity prompt 02), 2026.4.0: from 2026.4.0 a failed PRyMordial solve is
# stored as a failure with its reason, not as a success, and PRyM_version is "bf24c3d+cham03+ri02".
# On 2026-10-01 (integrator-remediation prompt 01), 2026.5.0: from 2026.5.0 the scalar history is
# integrated by one Radau step loop with a kinematic step cap and an elastic reflection at the
# representable-step floor, replacing the two-region scheme, so every stored history changes.
VERSION_LABEL = "2026.5.0"

# The reserved payload key under which Datastore.object_get hands a version-keyed factory's
# build() the serial of the current version label (run-integrity prompt 01). A factory registers
# "key_on_version": True to receive it, and its build() raises if the key is absent.
VERSION_SERIAL_KEY = "_version_serial"


def require_version_serial(payload, cls_name: str) -> int:
    """
    Return the version serial that Datastore.object_get placed in a lookup payload for a
    version-keyed factory. Raise if it is absent: a keyed lookup never falls back to an
    unfiltered one. (run-integrity prompt 01)
    """
    serial = payload.get(VERSION_SERIAL_KEY, None)
    if serial is None:
        raise RuntimeError(
            f"{cls_name}.build(): the lookup payload carries no version serial under "
            f"'{VERSION_SERIAL_KEY}'. A version-keyed lookup must go through "
            f"Datastore.object_get(), and is never made unfiltered."
        )
    return serial
