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

# the type ids of the models (jordan-normalization prompt 04, M3). A model holds both the potential
# and the conformal coupling, and each model type has its own table (jordan-normalization prompt 07,
# S1): EXPONENTIAL_MODEL's is ExponentialModel (ObjectFactories/ExponentialModel.py). A
# ScalarHistory names its model by (model_type, model_serial), and this id is model_type
EXPONENTIAL_MODEL = 1
# the inverse-power-law model V_0 [1 + (M/phi)^n] (power-law-wall prompt 01, README §6.3 P1). Its table is
# InversePowerModel (ObjectFactories/InversePowerModel.py), keyed (M, n, beta), since power-law-wall prompt 05
# (README §6.9 P33); its serials are its own, so they repeat ExponentialModel's
INVERSE_POWER_MODEL = 2
