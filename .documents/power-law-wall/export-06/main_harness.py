"""
main.py's execute(), extracted by ast with its module's top-level imports and run on a stood-in pool: every
pool.object_get is recorded, in order, with what ray.get returns for it; run_pipeline, build_model_list and
warn_super_planckian are stood in. No Ray, no store.
"""

import ast
import contextlib
import io
from pathlib import Path
from types import SimpleNamespace


class Ref:
    def __init__(self, cls, kw):
        self.cls = cls
        self.kw = kw


def describe(cls, kw):
    """A lookup as a hashable, printable tuple: the class and its keywords (payload_data summarised)."""
    out = []
    for k in sorted(kw):
        v = kw[k]
        if k == "payload_data" and cls == "redshift":
            v = ("redshifts", len(v), v[0]["log_one_plus_z"], v[-1]["log_one_plus_z"])
        elif k == "payload_data":
            v = tuple(
                tuple(
                    (pk, getattr(pv, "_tag", pv) if not isinstance(pv, float) else pv)
                    for pk, pv in sorted(p.items())
                    if pk not in ("units",)
                )
                for p in v
            )
        elif k == "units":
            continue
        out.append((k, v))
    return (cls, tuple(out))


def resolve(ref):
    cls, kw = ref.cls, ref.kw
    if "payload_data" in kw:
        return [
            SimpleNamespace(_tag=f"{cls}{i}", cls=cls, **{k: v for k, v in p.items()})
            for i, p in enumerate(kw["payload_data"])
        ]
    if cls == "alpha_value":
        return SimpleNamespace(
            _tag=f"alpha({kw['value']},{kw.get('drain_model', 'fixed')})",
            value=kw["value"],
            drain_model=kw.get("drain_model", "fixed"),
        )
    return SimpleNamespace(_tag=f"{cls}", cls=cls, **kw)


def run_execute(main_path: Path, argv):
    tree = ast.parse(Path(main_path).read_text(), str(main_path))
    imports = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
    functions = [
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "execute"
    ]
    namespace = {"__name__": "main_harness"}
    exec(
        compile(ast.Module(body=imports, type_ignores=[]), "main.py", "exec"), namespace
    )
    exec(
        compile(ast.Module(body=functions, type_ignores=[]), "main.py", "exec"),
        namespace,
    )

    lookups = []
    pipelines = []

    class Pool:
        def object_get(self, cls, **kw):
            lookups.append(describe(cls, kw))
            return Ref(cls, kw)

    def get(x):
        if isinstance(x, list):
            return [resolve(r) for r in x]
        return resolve(x)

    namespace["ray"] = SimpleNamespace(get=get)
    namespace["args"] = namespace["create_argument_parser"]().parse_args(
        ["--database", "unused.db", *argv]
    )
    namespace["DimensionlessQuantityArray"] = lambda value_array: list(value_array)
    namespace["DimensionfulQuantityArray"] = lambda value_array: list(value_array)
    namespace["redshift_array"] = lambda z_array: list(z_array)
    namespace["build_model_list"] = lambda pool, units: [
        {"label": "QCD_Cosmology", "cosmology": "cosmology"}
    ]
    namespace["warn_super_planckian"] = lambda array, *a: array

    def run_pipeline(pool, model_data, Model_array, *rest):
        alpha = rest[7]
        pipelines.append(
            (
                (alpha.value, alpha.drain_model),
                tuple((m.cls, m.n, m.beta._tag, m.M._tag) for m in Model_array),
            )
        )

    namespace["run_pipeline"] = run_pipeline
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        namespace["execute"](Pool(), namespace["Planck_units"]())
    return lookups, pipelines, out.getvalue()


if __name__ == "__main__":
    import json
    import sys

    lookups, pipelines, printed = run_execute(Path(sys.argv[1]), sys.argv[2:])
    print(json.dumps(dict(lookups=lookups, pipelines=pipelines), default=str))
    print("----")
    print(printed)
