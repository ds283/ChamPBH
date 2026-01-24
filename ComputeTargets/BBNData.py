from collections import namedtuple
from typing import Optional, List

import ray

from CosmologyConcepts import redshift, redshift_array
from CosmologyConcepts.ConformalCouplings import AbstractCoupling
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Datastore import DatastoreObject
from MetadataConcepts import store_tag
from Units.base import UnitsLike
from config.sharding import ShardKeyType
from .ScalarModel import ScalarModelProxy, ScalarModel, ScalarModelValue

SampleValues = namedtuple(
    "SampleValues",
    [
        "raw_N",
        "T_Jordan",
        "density_NP",
        "pressure_NP",
    ],
)


@ray.remote
def compute_BBN_data(model_proxy: ScalarModelProxy, task_label: str):
    model: ScalarModel = model_proxy.get()
    cosmology: BaseCosmology = model._cosmology
    units: UnitsLike = cosmology.units

    z_grid: List[redshift] = []
    samples: List[dict] = []

    for value in model.values:
        value: ScalarModelValue

    return {
        "z_grid": z_grid,
        "samples": samples,
        "compute_time": 0.0,
    }


class BBNData(DatastoreObject):
    def __init__(
        self,
        payload,
        model_proxy: ScalarModelProxy,
        label: Optional[str] = None,
        tags: Optional[List[store_tag]] = None,
    ):
        self._model_proxy: ScalarModelProxy = model_proxy
        model: ScalarModel = model_proxy.get()
        self._coupling = model.coupling
        self._potential = model.potential

        self._label: str = label
        self._tags: Optional[List[store_tag]] = tags if tags is not None else []

        if payload is None:
            DatastoreObject.__init__(self, None)
            self._Yp_BBN = None
            self._DOverH = None
            self._HeOverH = None
            self._LiOverH = None

            self._values = None

            self._BBN_compute_time = None

            self._populated = False
        else:
            DatastoreObject.__init__(self, payload["store_id"])
            self._Yp_BBN = payload["Yp_BBN"]
            self._DOverH = payload["DOverH"]
            self._HeOverH = payload["HeOverH"]
            self._LiOverH = payload["LiOverH"]

            self._values = None

            self._populated = True

        self._compute_ref: Optional[ray.ObjectRef] = None

    @property
    def shard_key(self) -> ShardKeyType:
        return self._coupling.shard_key

    @property
    def label(self) -> str:
        return self._label

    @property
    def tags(self) -> List[store_tag]:
        return self._tags

    @property
    def potential(self) -> AbstractPotential:
        return self._potential

    @property
    def coupling(self) -> AbstractCoupling:
        return self._coupling

    @property
    def Yp_BBN(self) -> Optional[float]:
        if self._populated is False:
            raise RuntimeError("Yp_BBN has not yet been populated")

        return self._Yp_BBN

    @property
    def DOverH(self) -> Optional[float]:
        if self._populated is False:
            raise RuntimeError("DOverH has not yet been populated")

        return self._DOverH

    @property
    def HeOverH(self) -> Optional[float]:
        if self._populated is False:
            raise RuntimeError("HeOverH has not yet been populated")

        return self._HeOverH

    @property
    def LiOverH(self) -> Optional[float]:
        if self._populated is False:
            raise RuntimeError("LiOverH has not yet been populated")

        return self._LiOverH

    @property
    def values(self) -> List:
        if self._values is None:
            raise RuntimeError("values have not yet been populated")
        return self._values

    @property
    def compute_time(self) -> float:
        if self._populated is False:
            raise RuntimeError("compute_time has not yet been populated")
        return self._BBN_compute_time

    @property
    def compute(self, label: Optional[str] = None):
        if self._populated:
            raise RuntimeError("values have already been populated")

        if label is not None:
            self._label = label

        self._compute_ref = compute_BBN_data.remote(
            self._model_proxy,
            task_label=(
                self._label
                if self._label is not None
                else f"{self._potential.name}-{self._coupling.name}"
            ),
        )
        return self._compute_ref

    @property
    def store(self) -> Optional[bool]:
        if self._compute_ref is None:
            raise RuntimeError(
                "BBNData: store() called, but no compute() is in progress"
            )

        # check whether the computation has actually resolved
        resolved, unresolved = ray.wait([self._compute_ref], timeout=0)

        if len(resolved) == 0:
            return None

        # retrieve result and populate ourselves
        data = ray.get(self._compute_ref)
        self._compute_ref = None

        self._Yp_BBN = data["Yp_BBN"]
        self._DOverH = data["DOverH"]
        self._HeOverH = data["HeOverH"]
        self._LiOverH = data["LiOverH"]

        z_grid: redshift_array = data["z_grid"]
        samples = data["samples"]

        self._values = []
        for i in range(len(z_grid)):
            self._values.append(
                BBNDataValue(
                    None,
                    z=z_grid[i],
                    raw_N=samples[i].raw_N,
                    T_Jordan=samples[i].T_Jordan,
                    density_NP=samples[i].density_NP,
                    pressure_NP=samples[i].pressure_NP,
                )
            )

        self._populated = True
        return True


class BBNDataValue(DatastoreObject):
    def __init__(
        self,
        store_id: int,
        z: redshift,
        raw_N: float,
        T_Jordan: float,
        density_NP: float,
        pressure_NP: float,
    ):
        DatastoreObject.__init__(self, store_id)

        self._z: redshift = z
        self._raw_N: float = raw_N
        self._T_Jordan: float = T_Jordan

        self._density_NP: float = density_NP
        self._pressure_NP: float = pressure_NP

    @property
    def shard_key(self) -> ShardKeyType:
        return NotImplementedError

    @property
    def z(self) -> redshift:
        return self._z

    @property
    def raw_N(self) -> float:
        return self._raw_N

    @property
    def T_Jordan(self) -> float:
        return self._T_Jordan

    @property
    def density_NP(self) -> float:
        return self._density_NP

    @property
    def pressure_NP(self) -> float:
        return self._pressure_NP
