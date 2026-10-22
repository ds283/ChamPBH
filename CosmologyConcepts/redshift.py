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

import math
from functools import total_ordering
from typing import Iterable, Self

from datastorekit import DatastoreObject
from config.defaults import DEFAULT_LOG_ONE_PLUS_Z_PRECISION


def log_one_plus_z_grid(
    samples_per_log10_z: int, log10_one_plus_z_high: float
) -> list[float]:
    """
    The sampling axis (jordan-normalization prompt 03, Z3): the points ln(1 + z) = k * DELTA,
    k = 0 ... K, with DELTA = ln 10 / samples_per_log10_z and
    K = round(log10_one_plus_z_high * samples_per_log10_z). The axis is anchored at the end of
    the history (ln(1 + z) = 0), so there is no lower limit.

    Each point is an int times a float, never accumulated, so that every builder of a grid gets
    the same floats bit for bit, and a point is matched in the store by its value.
    :param samples_per_log10_z: number of points per decade of 1 + z
    :param log10_one_plus_z_high: the largest log10(1 + z); the top point is the nearest multiple
    :return: the points, ascending
    """
    DELTA = math.log(10.0) / samples_per_log10_z
    K = int(round(log10_one_plus_z_high * samples_per_log10_z))
    return [k * DELTA for k in range(K + 1)]


@total_ordering
class redshift(DatastoreObject):
    def __init__(
        self,
        store_id: int,
        log_one_plus_z: float,
    ):
        """
        Represents a point of the sampling axis: ln(1 + z), with z the Einstein-frame redshift
        counted back from the end of the history (jordan-normalization prompt 03, Z1). Stored as
        ln(1 + z) so that nothing overflows however long a history runs.
        :param store_id: unique Datastore id. Should not be None
        :param log_one_plus_z: ln(1 + z); not negative, not NaN
        """
        if store_id is None:
            raise ValueError("Store ID cannot be None")
        if math.isnan(log_one_plus_z) or log_one_plus_z < 0.0:
            raise ValueError(
                f"log_one_plus_z must be a non-negative number, not {log_one_plus_z}"
            )
        DatastoreObject.__init__(self, store_id)

        self._log_one_plus_z: float = log_one_plus_z

    @property
    def log_one_plus_z(self) -> float:
        """ln(1 + z): the stored label of the point"""
        return self._log_one_plus_z

    @property
    def z(self) -> float:
        """
        z itself, derived as expm1(ln(1 + z)); inf (not an error) past float range, which is
        ln(1 + z) > 709.78
        """
        try:
            return math.expm1(self._log_one_plus_z)
        except OverflowError:
            return float("inf")

    def __float__(self):
        """
        Cast to float. Returns ln(1 + z), the stored label. (Before jordan-normalization
        prompt 03 this returned z.)
        :return:
        """
        return float(self._log_one_plus_z)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            raise NotImplementedError

        return self.store_id == other.store_id

    def __lt__(self, other):
        if not isinstance(other, type(self)):
            raise NotImplementedError

        return self._log_one_plus_z < other._log_one_plus_z

    def __hash__(self):
        return ("redshift", self.store_id).__hash__()


class redshift_array:
    def __init__(self, z_array: Iterable[redshift]):
        """
        Represents an array of redshifts
        :param store_id: unique Datastore id. Should not be None
        :param z_array: array of redshift value
        """
        # store array, sorted in descending order of ln(1 + z);
        # the conversion to set ensure that we remove any duplicates
        self._z_array = sorted(
            set(z_array), key=lambda x: x.log_one_plus_z, reverse=True
        )

    def __iter__(self):
        for z in self._z_array:
            yield z

    def __getitem__(self, key):
        return self._z_array[key]

    def __len__(self):
        return len(self._z_array)

    def __eq__(self, other):
        return not self.__ne__(other)

    def __ne__(self, other):
        if len(self._z_array) != len(other._z_array):
            return True

        if any(za != zb for za, zb in zip(self._z_array, other._z_array)):
            return True

        return False

    def __add__(self, other):
        full_set = set(self._z_array)
        full_set.update(set(other._z_array))
        return redshift_array(full_set)

    def as_float_list(self) -> list[float]:
        """the points' ln(1 + z) (float(redshift)), in the array's order"""
        return [float(z) for z in self._z_array]

    @property
    def max(self) -> redshift:
        return self._z_array[0]

    @property
    def min(self) -> redshift:
        return self._z_array[-1]

    def truncate(self, z_limit, keep: str = "lower") -> Self:
        """
        Cut the array at z_limit: a redshift, or a float, which is an ln(1 + z) (since
        jordan-normalization prompt 03). Every mode compares ln(1 + z), with the tolerance
        DEFAULT_LOG_ONE_PLUS_Z_PRECISION (1e-10, far below the grid spacing ln 10/250); before
        prompt 03 the modes compared z with DEFAULT_FLOAT_PRECISION (1e-7). So "lower" keeps a
        point at most 1e-10 above the limit.
        """
        if keep == "lower":
            return self._truncate_lower(z_limit)
        if keep == "higher":
            return self._truncate_higher(z_limit)
        if keep == "lower-strict":
            return self._truncate_lower_strict(z_limit)
        if keep == "higher-strict":
            return self._truncate_higher_strict(z_limit)
        if keep == "lower-include":
            return self._truncate_lower_include(z_limit)
        if keep == "higher-include":
            return self._truncate_higher_include(z_limit)

        raise ValueError(f'Unknown truncation mode "{keep}')

    def _truncate_lower(self, max_z) -> Self:
        tol = DEFAULT_LOG_ONE_PLUS_Z_PRECISION
        if isinstance(max_z, redshift):
            return redshift_array(
                z_array=[
                    z
                    for z in self._z_array
                    if z.log_one_plus_z <= max_z.log_one_plus_z + tol
                    or z.store_id == max_z.store_id
                ]
            )

        return redshift_array(
            z_array=[z for z in self._z_array if z.log_one_plus_z <= max_z + tol]
        )

    def _truncate_higher(self, min_z) -> Self:
        tol = DEFAULT_LOG_ONE_PLUS_Z_PRECISION
        if isinstance(min_z, redshift):
            return redshift_array(
                z_array=[
                    z
                    for z in self._z_array
                    if z.log_one_plus_z >= min_z.log_one_plus_z - tol
                    or z.store_id == min_z.store_id
                ]
            )

        return redshift_array(
            z_array=[z for z in self._z_array if z.log_one_plus_z >= min_z - tol]
        )

    def _truncate_lower_strict(self, max_z) -> Self:
        tol = DEFAULT_LOG_ONE_PLUS_Z_PRECISION
        if isinstance(max_z, redshift):
            return redshift_array(
                z_array=[
                    z
                    for z in self._z_array
                    if z.log_one_plus_z < max_z.log_one_plus_z - tol
                    and z.store_id != max_z.store_id
                ]
            )

        return redshift_array(
            z_array=[z for z in self._z_array if z.log_one_plus_z < max_z - tol]
        )

    def _truncate_higher_strict(self, min_z) -> Self:
        tol = DEFAULT_LOG_ONE_PLUS_Z_PRECISION
        if isinstance(min_z, redshift):
            return redshift_array(
                z_array=[
                    z
                    for z in self._z_array
                    if z.log_one_plus_z > min_z.log_one_plus_z + tol
                    and z.store_id != min_z.store_id
                ]
            )

        return redshift_array(
            z_array=[z for z in self._z_array if z.log_one_plus_z > min_z + tol]
        )

    def _truncate_lower_include(self, max_z) -> Self:
        tol = DEFAULT_LOG_ONE_PLUS_Z_PRECISION
        include_array = []
        included_endpoint = False
        for z in self._z_array:
            if isinstance(max_z, redshift):
                if (
                    z.log_one_plus_z <= max_z.log_one_plus_z + tol
                    or z.store_id == max_z.store_id
                ):
                    include_array.append(z)
                elif not included_endpoint:
                    include_array.append(z)
                    included_endpoint = True
            else:
                if z.log_one_plus_z <= max_z + tol:
                    include_array.append(z)
                elif not included_endpoint:
                    include_array.append(z)
                    included_endpoint = True

        return redshift_array(include_array)

    def _truncate_higher_include(self, max_z) -> Self:
        tol = DEFAULT_LOG_ONE_PLUS_Z_PRECISION
        include_array = []
        included_endpoint = False
        for z in self._z_array:
            if isinstance(max_z, redshift):
                if (
                    z.log_one_plus_z >= max_z.log_one_plus_z - tol
                    or z.store_id == max_z.store_id
                ):
                    include_array.append(z)
                elif not included_endpoint:
                    include_array.append(z)
                    included_endpoint = True
            else:
                if z.log_one_plus_z >= max_z - tol:
                    include_array.append(z)
                elif not included_endpoint:
                    include_array.append(z)
                    included_endpoint = True

        return redshift_array(include_array)

    def winnow(self, sparseness: int) -> Self:
        sparseness = int(sparseness)
        if sparseness <= 0:
            raise ValueError("sparseness must be greater than zero")
        return redshift_array(z_array=self._z_array[::-sparseness])


def check_zsample(A, B):
    A_sample: redshift_array = A if isinstance(A, redshift_array) else A.z_sample
    B_sample: redshift_array = B if isinstance(B, redshift_array) else B.z_sample

    if A_sample != B_sample:
        raise RuntimeError("Redshift sample grids are not equal")
