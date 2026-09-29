import sys, unittest
from CosmologyModels.GenericEOS.Xav_EOS_spline import Xav_EOS_spline
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import SaikawaShirai_EOS_spline
mode = sys.argv[1]
if mode == "freeze":
    Xav_EOS_spline.w = SaikawaShirai_EOS_spline.w
elif mode == "shift":
    orig = Xav_EOS_spline.w
    import numpy as np
    def w(self, T):
        # shift the table by 6 % in T
        return orig(self, T * 1.06)
    Xav_EOS_spline.w = w
suite = unittest.defaultTestLoader.loadTestsFromName("CosmologyModels.tests.test_kicking_function")
r = unittest.TextTestRunner(verbosity=0).run(suite)
print(mode, "failures", len(r.failures), "errors", len(r.errors))
for t, tb in r.failures: print("  ", t.id().split('.')[-1], tb.strip().splitlines()[-1][:160])
