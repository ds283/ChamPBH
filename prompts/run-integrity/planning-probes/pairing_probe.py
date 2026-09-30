"""
Planning probe for the run-integrity campaign: the list logic of main.py's BBN
stage, with stand-ins. From the repository root, instant:

    ./venv/bin/python prompts/run-integrity/planning-probes/pairing_probe.py

It copies three steps of build_bbn_data_batch as they stand on 27a32bc:

- main.py:623-637, the BBN query payload, which skips failed ScalarModels;
- the vectorized lookup, which returns one BBNData per payload entry, in order;
- main.py:659-671, which zips those results against binned_batch[key], the
  UNFILTERED bin of (potential, coupling) pairs.

The adiabatic stage (main.py:377-425) has the same three steps.

Scenario: five models in one shard bin. Model 1's ScalarModel failed. Models 0
and 3 already have BBN rows. The correct set to compute is {2, 4}.
"""

from types import SimpleNamespace as NS

N = 5
pairs = [(f"V{i}", f"beta{i}") for i in range(N)]
scalar_models = [NS(i=i, available=True, failure=(i == 1)) for i in range(N)]
has_bbn = {0, 3}

# main.py:623-637
bbn_payload = [m for m in scalar_models if not m.failure]
bbn_results = [NS(model=m.i, available=(m.i in has_bbn)) for m in bbn_payload]

# main.py:659-671
zipped = list(zip(bbn_results, pairs))
missing = [pc for obj, pc in zipped if not obj.available]

correct = [
    pairs[i][0] for i in range(N) if i not in has_bbn and not scalar_models[i].failure
]

print("pairing:", [(pc[0], f"BBN result of model {obj.model}") for obj, pc in zipped])
print("main.py schedules BBN for:", [p[0] for p in missing])
print("the correct set:          ", correct)
print(
    "V1's ScalarModel failed; compute_BBN_data reads model.values, which raises "
    "RuntimeError for a failed model (ComputeTargets/ScalarModel.py:1156), outside "
    "every except clause."
)
