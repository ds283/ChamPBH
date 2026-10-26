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

import os
import sys

# Ray's token authentication, named rather than left to Ray's default (ray-sqla-align prompt 01;
# StochasticInstantons' config/ray_auth.py, and the token half of SecondaryGWKit's
# config/ray_local.py). Since Ray 2.59, when RAY_AUTH_MODE is unset, the mode depends on how Ray
# is started:
# - a new local instance (ray.init(address="local")) runs in token mode, logs a warning saying so,
#   and writes a token if there is none. ChamPBH never starts Ray this way;
# - `ray start --head` runs in token mode, with the same warning, whenever the token file exists;
# - a driver attaching to an explicit host:port never turns token mode on.
# ChamPBH takes the second and third routes: ray_cluster.sh runs `ray start --head`, and main.py,
# plot_by_beta.py and plot_ScalarHistory.py attach with --ray-address. So on Ray 2.59 alone its
# cluster came up in token mode and refused a driver attaching by address with AuthenticationError
# (ray-sqla-align 01, V0). With the mode named, the cluster and its drivers agree, a driver sends
# the token, and Ray logs nothing.
#
# The three drivers import this module before their ray.init, and ray_cluster.sh names the mode
# for the cluster itself; tools/tests/test_ray_auth.py checks both. A value already in the
# environment (for instance `disabled`) is left as it is.
#
# Ray reads the mode into its config once, when `ray` is imported, and the drivers import `ray`
# first; so does RunDrivers/registered_run.py, which runs main.py in-process. Without a refresh the
# driver keeps `disabled` while the cluster runs in `token` mode, and every attach is refused:
# SecondaryGWKit's issue [02-the-token-mode-setting-comes-after-import-ray-so-every-ray-start-fails].
# Config.initialize("") is the refresh Ray's own _enable_token_auth makes. Nothing is imported from
# Ray here.
#
# The token is ~/.ray/auth_token, shared by every project of this user on the machine. Keep it at
# mode 600, and never print or copy it.
RAY_AUTH_MODE = "token"
if "RAY_AUTH_MODE" not in os.environ:
    os.environ["RAY_AUTH_MODE"] = RAY_AUTH_MODE
    if "ray._raylet" in sys.modules:
        sys.modules["ray._raylet"].Config.initialize("")
