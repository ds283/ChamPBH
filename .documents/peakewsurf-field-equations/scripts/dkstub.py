"""Stub datastorekit (not installed in this venv) so the pipeline's modules import; only DatastoreObject is used."""

import sys, types

_dk = types.ModuleType("datastorekit")


class DatastoreObject:
    def __init__(self, store_id=None, *a, **k):
        self._my_id = store_id

    @property
    def store_id(self):
        return self._my_id


_dk.DatastoreObject = DatastoreObject
sys.modules.setdefault("datastorekit", _dk)
