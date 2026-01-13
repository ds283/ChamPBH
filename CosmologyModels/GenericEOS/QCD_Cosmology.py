from CosmologyModels.GenericEOS.LambdaCDM_GenericEOS import (
    LambdaCDM_GenericEOS,
)
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from Units.base import UnitsLike


class QCD_Cosmology(LambdaCDM_GenericEOS):

    def __init__(
        self,
        store_id: int,
        units: UnitsLike,
        params,
    ):
        """
        QCD_Cosmology is a convenience wrapper that builds a ParametrizedEOS cosmology using
        a specified equation of state
        :param store_id:
        :param units:
        :param params:
        """
        LambdaCDM_GenericEOS.__init__(
            self,
            store_id,
            Xav_EOS_spline(units),
            units,
            params,
        )
