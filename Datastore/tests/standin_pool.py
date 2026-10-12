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

# ChamPBH's copy of DatastoreKit's stand-in pool (dk-and-rpk-migration prompt 08).
#
# Copied from datastorekit/tests/standin_pool.py at DatastoreKit's tag v0.3.0, commit
# 2f5417ea76cdcd0d15d64d80d9085429bfacad21 (https://github.com/ds283/DatastoreKit), which is
# licensed under the Apache License 2.0, as ChamPBH is. ChamPBH keeps a copy because the package's
# wheel does not ship its tests (DatastoreKit's docs/adoption/README.md §3: a client whose tests
# use the stand-in pool keeps its own copy). ChamPBH's test harness builds the package's actor
# through it (Datastore/tests/test_version_keyed_lookups.py, _TempStoreCase).
#
# Changed from the tag's file only where it must be: open_pool and open_pool_output imported the
# package's neutral test client (datastorekit.tests.client.registry), which is not installed; here
# they open a pool on ChamPBH's registry (config.datastore.factories) and tables (config.sharding:
# replicated_tables, sharded_tables, ShardKeyType, get_shard_key_store_id), under a version_label
# argument. Everything else is the tag's file unchanged. On a DatastoreKit upgrade, copy the new
# tag's file and re-apply these two changes.

"""
A real ``ShardedPool`` on stand-in shards, with no Ray.

The pattern is the source repository's fault probe of the replicated write, made reusable so that
test modules import it rather than copy it. What is real: ``ShardedPool``, the ``Datastore`` actor
code, every factory, the ``SerialPoolBroker`` and SQLite on disk. What is stood in:

- the actor classes. ``ShardedPool``'s references to ``Datastore`` and ``SerialPoolBroker`` are
  replaced by stand-ins whose ``.options(...).remote(...)`` builds the *undecorated* class
  (``__ray_metadata__.modified_class``) in-process. ``handle.method.remote(...)`` runs the method
  at once, with its arguments and its result pickled through, as Ray would, so that no two
  "actors" share an object;
- ``ray.get``. ``.remote()`` returns a ``StandinRef`` holding the pickled result, or the exception
  the call raised. ``ray.get`` of a list reads every reference and raises the first error, so an
  exception reaches the driver only after every call it submitted has run, as with Ray;
- the controlling shard. ``ShardedPool`` draws it with ``random.randrange``; ``controller`` pins
  it by shard id, or leaves the draw random when ``None``;
- actor names and ``ray.kill``. Each ``StandinCluster`` is one Ray session's namespace: an actor's
  name is reserved once its constructor has returned (a constructor that raises reserves
  nothing), and creating a name a live handle holds raises ``ValueError`` with Ray 2.43.0's
  message, before the constructor runs, as Ray checks the name before it creates the actor.
  ``ray.kill(handle, ...)`` marks the handle killed and frees its name; killing a killed handle
  does nothing, and anything that is not a stand-in handle raises ``ValueError``, as Ray refuses
  what is not an actor handle. A call through a killed handle runs nothing, is not added to the
  call log, runs no hook, and returns a ``StandinRef`` that raises ``StandinActorDied`` naming
  the actor and the method;
- whether the process is connected to Ray, as the pool sees it. ``ShardedPool``'s
  ``_ray_is_running`` is true inside ``active()``, since a stand-in cluster is a Ray session, so
  the pool's kills reach the stand-in ``ray.kill``. ``ray.is_initialized()`` is left as it is,
  and stays false: the tests assert it.

**The one way it is stricter than Ray.** Ray also frees an actor's name when the last handle to
it is collected; the stand-in frees a name only when its handle is killed. So a test that drops a
pool without closing it holds that pool's names for the rest of its cluster. A pool whose
constructor raised while building its actors (an actor's constructor that raises does so at once
here, inside the comprehension that builds them) cannot kill the actors built before it, and their
names stay reserved in that cluster.

Faults are injected per shard, method and class: ``"before"`` raises ``StandinActorDied`` without
running the call (an actor that died before committing), ``"after"`` runs the call, commits, and
then raises (an actor, or the driver, that died after the commit). Hooks see every actor call
before and after it runs.

Every patch is scoped by ``StandinCluster.active()`` with ``unittest.mock.patch``. Stores are built
in directories the caller supplies, which must be temporary.

This module is not a test module (no ``test_`` prefix); test modules import it.
"""

import contextlib
import importlib
import io
import pickle
import random as _random
import sqlite3
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from unittest import mock

import ray

sp_mod = importlib.import_module("datastorekit.SQL.ShardedPool")
ds_mod = importlib.import_module("datastorekit.SQL.Datastore")
broker_mod = importlib.import_module("datastorekit.SQL.SerialPoolBroker")

DatastoreClass = ds_mod.Datastore.__ray_metadata__.modified_class
BrokerClass = broker_mod.SerialPoolBroker.__ray_metadata__.modified_class


class StandinActorDied(Exception):
    """Stands in for a RayActorError, or for the driver being killed at this point."""


def _copy(x):
    return pickle.loads(pickle.dumps(x))


class StandinRef:
    """What ``.remote()`` returns: the call's pickled result, or the exception it raised."""

    def __init__(self, result=None, error: Optional[BaseException] = None):
        self._result = pickle.dumps(result) if error is None else None
        self._error = error
        if error is not None:
            # an exception reaches the driver pickled, as under Ray, when it can be
            try:
                self._error = pickle.loads(pickle.dumps(error))
            except Exception:
                pass

    def get(self):
        if self._error is not None:
            raise self._error
        # a fresh copy on every get, as Ray deserializes one
        return pickle.loads(self._result)


def standin_get(refs, *args, **kwargs):
    """``ray.get``: a list is read in order and raises its first error; anything else that is not
    a ``StandinRef`` is returned as it is."""
    if isinstance(refs, (list, tuple)):
        return [standin_get(r) for r in refs]
    if isinstance(refs, StandinRef):
        return refs.get()
    return refs


def _class_name(args) -> Optional[str]:
    if len(args) == 0:
        return None
    if isinstance(args[0], str):
        return args[0]
    return type(args[0]).__name__


class _Method:
    def __init__(self, handle: "Handle", name: str):
        self._handle = handle
        self._name = name

    def remote(self, *args, **kwargs):
        handle = self._handle
        cluster = handle.cluster
        cls_name = _class_name(args)
        call = (handle.shard_id, self._name, cls_name)

        # a killed actor runs nothing. The flag is read from the instance's __dict__: any
        # attribute a handle lacks is a _Method (Handle.__getattr__), which is always truthy
        if handle.__dict__.get("killed", False):
            return StandinRef(
                error=StandinActorDied(
                    f"{handle.name}.{self._name}({cls_name}) called on a killed actor"
                )
            )

        cluster.calls.append(
            {
                "shard": handle.shard_id,
                "method": self._name,
                "class": cls_name,
                "kwargs": sorted(kwargs.keys()),
                "insert_timestamp": kwargs.get("insert_timestamp"),
            }
        )

        mode = cluster.faults.get(call)
        if mode == "before":
            return StandinRef(
                error=StandinActorDied(
                    f"{handle.name}.{self._name}({cls_name}) killed before it ran"
                )
            )

        for hook in list(cluster.hooks):
            hook(handle.shard_id, self._name, cls_name, "before")

        fn = getattr(handle.obj, self._name)
        try:
            result = fn(*_copy(args), **_copy(kwargs))
        except Exception as e:
            return StandinRef(error=e)

        for hook in list(cluster.hooks):
            hook(handle.shard_id, self._name, cls_name, "after")

        if mode == "after":
            return StandinRef(
                error=StandinActorDied(
                    f"{handle.name}.{self._name}({cls_name}) killed after it ran"
                )
            )

        return StandinRef(result=result)


class Handle:
    """A stand-in actor handle: ``handle.method.remote(...)`` calls ``obj.method`` at once."""

    def __init__(
        self, obj, name: str, cluster: "StandinCluster", shard_id: Optional[int]
    ):
        self.obj = obj
        self.name = name
        self.cluster = cluster
        self.shard_id = shard_id

    def __getattr__(self, name):
        return _Method(self, name)


class _Options:
    def __init__(self, cls, name, cluster):
        self._cls = cls
        self._name = name
        self._cluster = cluster

    def remote(self, *args, **kwargs):
        shard_id = None
        if self._name is not None and self._name.startswith("shard"):
            shard_id = int(self._name[len("shard") : len("shard") + 4])
        cluster = self._cluster
        # Ray refuses a name a live actor holds before it creates anything
        if self._name is not None and self._name in cluster.names:
            raise ValueError(
                f"The name {self._name} (namespace=None) is already taken. Please use a "
                f"different name or get the existing actor using "
                f"ray.get_actor('{self._name}', namespace='None')"
            )
        # constructor arguments are passed as they are: the Datastore actor receives the broker's
        # handle, which must stay the one broker
        handle = Handle(self._cls(*args, **kwargs), self._name, cluster, shard_id)
        # reserved only once the constructor has returned
        if self._name is not None:
            cluster.names[self._name] = handle
        return handle


def standin_kill(actor, no_restart: bool = True):
    """
    ``ray.kill``: mark a stand-in handle killed and free its name in its cluster, if it is the
    name's live holder. Killing a killed handle does nothing. Anything that is not a stand-in
    ``Handle`` raises ``ValueError``, as Ray's ``ray.kill`` refuses what is not an actor handle.
    """
    if not isinstance(actor, Handle):
        raise ValueError(
            "ray.kill() only supported for actors. For tasks, try ray.cancel(). "
            f"Got: {type(actor)}."
        )
    # read from __dict__: any attribute a handle lacks is a _Method, which is always truthy
    if actor.__dict__.get("killed", False):
        return
    actor.__dict__["killed"] = True
    names = actor.cluster.names
    if actor.name is not None and names.get(actor.name) is actor:
        del names[actor.name]


class _StandinActorClass:
    def __init__(self, cls, cluster):
        self._cls = cls
        self._cluster = cluster

    def options(self, name=None, **_):
        return _Options(self._cls, name, self._cluster)


class _StandinRandom:
    """ShardedPool's ``random``: ``randrange`` returns the index of the pinned controller."""

    def __init__(self, cluster: "StandinCluster"):
        self._cluster = cluster

    def randrange(self, n):
        cluster = self._cluster
        # inside StandinCluster.pin_controller, the draw is resolved against the pool making the
        # write, which may still be under construction
        writing = getattr(cluster, "_writing_pool", None)
        if (
            getattr(cluster, "open_controller", None) is not None
            and writing is not None
        ):
            return list(writing._shards.keys()).index(cluster.open_controller)
        if cluster.controller is None or cluster.pool is None:
            return _random.randrange(n)
        # ShardedPool swaps entry i of list(self._shards.keys()) to the end and pops it
        return list(cluster.pool._shards.keys()).index(cluster.controller)


class StandinCluster:
    """
    The stand-in actors of one or more pools, their faults, hooks and call log.

    Use ``with cluster.active():`` around everything that touches a pool.
    """

    def __init__(self):
        # (shard_id, method, class name) -> "before" | "after"
        self.faults: Dict[Tuple[Optional[int], str, Optional[str]], str] = {}
        # hook(shard_id, method, class name, "before" | "after")
        self.hooks: List[Callable] = []
        self.calls: List[dict] = []
        # the shard id to make the controlling shard, or None for a random draw
        self.controller: Optional[int] = None
        self.pool = None
        # pin_controller's state: the shard id pinned inside the block, and the pool whose
        # replicated write is running
        self.open_controller: Optional[int] = None
        self._writing_pool = None
        # actor name -> the live handle that holds it, in this cluster (one Ray session)
        self.names: Dict[str, Handle] = {}

    @contextlib.contextmanager
    def active(self):
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(ray, "get", standin_get))
            stack.enter_context(mock.patch.object(ray, "kill", standin_kill))
            # a stand-in cluster is a Ray session: the pool's kills are made
            stack.enter_context(
                mock.patch.object(sp_mod, "_ray_is_running", lambda: True)
            )
            stack.enter_context(
                mock.patch.object(
                    sp_mod, "Datastore", _StandinActorClass(DatastoreClass, self)
                )
            )
            stack.enter_context(
                mock.patch.object(
                    sp_mod, "SerialPoolBroker", _StandinActorClass(BrokerClass, self)
                )
            )
            stack.enter_context(
                mock.patch.object(sp_mod, "random", _StandinRandom(self))
            )
            yield self

    @contextlib.contextmanager
    def pin_controller(self, shard_id: int):
        """
        Inside the block, the controlling shard of every replicated write is ``shard_id``, resolved
        against the pool making the write rather than against ``self.pool``. A write made inside
        ``ShardedPool``'s constructor (the version row) has no ``self.pool`` to resolve against:
        ``open_pool`` sets it only once the constructor returns. ``controller`` and its behaviour
        are unchanged.
        """
        original = sp_mod.ShardedPool._replicated_write
        cluster = self

        def wrapped(pool, *args, **kwargs):
            previous = getattr(cluster, "_writing_pool", None)
            cluster._writing_pool = pool
            try:
                return original(pool, *args, **kwargs)
            finally:
                cluster._writing_pool = previous

        previous_pin = getattr(self, "open_controller", None)
        self.open_controller = shard_id
        try:
            with mock.patch.object(sp_mod.ShardedPool, "_replicated_write", wrapped):
                yield self
        finally:
            self.open_controller = previous_pin

    def fault(self, shard_id: int, method: str, cls_name: str, when: str = "before"):
        if when not in ("before", "after"):
            raise ValueError(f'unknown fault mode "{when}"')
        self.faults[(shard_id, method, cls_name)] = when

    def clear_faults(self):
        self.faults.clear()

    def open_pool(
        self,
        primary: Path,
        shards: int = 3,
        version_label: str = "standin",
        **kwargs,
    ):
        """Open (or create) a pool on ``primary`` under ``version_label``, with ChamPBH's registry
        and table lists."""
        from config.datastore import factories
        from config.sharding import (
            replicated_tables,
            sharded_tables,
            ShardKeyType,
            get_shard_key_store_id,
        )

        # the registry is the client's; a test may give a different one
        kwargs.setdefault("factories", factories)
        with contextlib.redirect_stdout(io.StringIO()):
            pool = sp_mod.ShardedPool(
                version_label=version_label,
                db_name=primary,
                ShardKeyType=ShardKeyType,
                ShardKeyStoreIdGetter=get_shard_key_store_id,
                replicated_tables=replicated_tables,
                sharded_tables=sharded_tables,
                shards=shards,
                **kwargs,
            )
        self.pool = pool
        return pool

    def open_pool_output(
        self,
        primary: Path,
        shards: int = 3,
        version_label: str = "standin",
        **kwargs,
    ):
        """
        As ``open_pool``, but return ``(pool, printed)``: what the constructor printed, which
        ``open_pool`` discards (the check at open prints what it repaired). If the constructor
        raises, what it printed before raising is kept as ``self.last_output``.
        """
        from config.datastore import factories
        from config.sharding import (
            replicated_tables,
            sharded_tables,
            ShardKeyType,
            get_shard_key_store_id,
        )

        # the registry is the client's; a test may give a different one
        kwargs.setdefault("factories", factories)
        out = io.StringIO()
        self.last_output = ""
        try:
            with contextlib.redirect_stdout(out):
                pool = sp_mod.ShardedPool(
                    version_label=version_label,
                    db_name=primary,
                    ShardKeyType=ShardKeyType,
                    ShardKeyStoreIdGetter=get_shard_key_store_id,
                    replicated_tables=replicated_tables,
                    sharded_tables=sharded_tables,
                    shards=shards,
                    **kwargs,
                )
        finally:
            self.last_output = out.getvalue()
        self.pool = pool
        return pool, out.getvalue()

    def close_pool(self, pool=None):
        pool = pool if pool is not None else self.pool
        with contextlib.redirect_stdout(io.StringIO()):
            pool.__exit__(None, None, None)
        if pool is self.pool:
            self.pool = None

    def replica_ids(self, pool=None) -> List[int]:
        """The shard ids other than the pinned controller, in the order they are submitted."""
        pool = pool if pool is not None else self.pool
        shard_ids = list(pool._shards.keys())
        i = shard_ids.index(self.controller)
        shard_ids[i], shard_ids[-1] = shard_ids[-1], shard_ids[i]
        shard_ids.pop()
        return shard_ids


# ------------------------------------------------------------------------------------------------
# reading a store's files directly, mode=ro
# ------------------------------------------------------------------------------------------------


def _read(path: Path, sql: str, params=()) -> List[tuple]:
    conn = sqlite3.connect(f"{Path(path).as_uri()}?mode=ro", uri=True)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def table_columns(path: Path, table: str) -> List[str]:
    return [row[1] for row in _read(path, f'PRAGMA table_info("{table}")')]


def shard_rows(pool, table: str) -> Dict[int, List[tuple]]:
    """Every column of every row of ``table`` on each shard, in a canonical order."""
    out = {}
    for sid, path in sorted(pool._shard_db_files.items()):
        columns = table_columns(path, table)
        order = ", ".join(f'"{c}"' for c in columns)
        out[sid] = _read(path, f'SELECT * FROM "{table}" ORDER BY {order}')
    return out


def shard_snapshot(pool, tables) -> Dict[str, Dict[int, List[tuple]]]:
    return {table: shard_rows(pool, table) for table in tables}


def store_checksums(primary: Path) -> Dict[str, str]:
    """
    The SHA-256 of every file in the directory of the primary ``primary``, by file name: the
    primary, its shards and any journal beside them (a check that writes nothing leaves every one
    unchanged, and creates no file). A stand-in store is the only thing in its directory; the caller
    makes sure of that.
    """
    import hashlib

    directory = Path(primary).parent
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(directory.iterdir())
        if p.is_file()
    }


def in_flight_records(pool) -> List[dict]:
    """The primary's ``replication_in_flight`` rows, as dicts."""
    path = pool.primary
    columns = table_columns(path, "replication_in_flight")
    return [
        dict(zip(columns, row))
        for row in _read(path, 'SELECT * FROM "replication_in_flight"')
    ]
