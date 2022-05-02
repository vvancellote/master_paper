import msgpack
from storage import StoreType


class NameKey(object):
    def __init__(
        self, path: str = "", stype: StoreType = StoreType.NONE, ttl: int = 3600
    ) -> None:
        self._path = path
        self._stype = stype.value
        self._ttl = ttl
        return

    @property
    def path(self) -> str:
        return self._path

    @path.setter
    def path(self, value: str) -> None:
        self._path = value
        return

    @property
    def stype(self) -> StoreType:
        return StoreType(self._stype)

    @stype.setter
    def stype(self, st) -> None:
        self._stype = st.value

    @property
    def ttl(self) -> int:
        return self._ttl

    @ttl.setter
    def ttl(self, ttl) -> None:
        self._ttl = ttl
        return

    def get_key(self) -> str:
        d = (self._path, self._stype, self._ttl)
        return msgpack.packb(d)

    def set_key(self, v: str) -> None:
        d = msgpack.unpackb(v)
        self.path = d[0]
        self.stype = d[1]
        self.ttl = d[2]
        return

    @classmethod
    def get_wildcard(cls, wildstr: str) -> str:
        return msgpack.packb((wildstr))
