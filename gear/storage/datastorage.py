from typing import Any, List
from gear.tools.serializer import CompactedPickler, Pickler, Serializer
import redis


class DataStorage(object):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        simple_serializer: Serializer = Pickler(),
        compression_serializer: Serializer = CompactedPickler(),
    ) -> None:
        self.con = redis.Redis(host=host, port=port, db=0)
        self.host = host
        self.port = port
        self.simple_serializer = simple_serializer
        self.compression_serializer = compression_serializer

    def __repr__(self) -> str:
        return f"{self.__class__}({self.__dict__})"

    def __str__(self) -> str:
        t = type(self)
        return f"{t.__name__}({self.host},{self.port})"

    def set(self, key: str, datum: Any) -> None:
        self.con.set(key, self.compression_serializer.encode(datum))
        return

    def sadd(self, key: str, datum: Any) -> None:
        self.con.sadd(key, self.simple_serializer.encode(datum))
        return

    def get(self, key: str) -> Any:
        payload = self.con.get(key)
        if payload == None:
            return None
        return self.compression_serializer.decode(payload)

    def bulk_set(self, data: dict) -> None:
        pipeline = self.con.pipeline()
        for k, v in data.items():
            pipeline.set(k, self.compression_serializer.encode(v))
        pipeline.execute()
        return

    def bulk_sadd(self, key: str, data: set) -> None:
        pipeline = self.con.pipeline()
        for v in data:
            pipeline.sadd(key, self.simple_serializer.encode(v))
        pipeline.execute()
        return

    def smembers(self, key: str) -> List:
        from_store = self.con.smembers(key)
        ret_val = list()
        for i in from_store:
            ret_val.append(self.simple_serializer.decode(i))
        return ret_val

    def enqueue(self, key: str, datum: Any) -> None:
        self.con.rpush(key, self.simple_serializer.encode(datum))
        return

    def dequeue(self, key: str, timeout: int = 0) -> Any:
        item = self.con.blpop(key, timeout)
        if item:
            return self.simple_serializer.decode(item[1])
        return item

    def get_unique_id(self, key: str) -> str:
        val = self.con.incr(key)
        return f"{key}:{val}"

    def get_keys(self, key: str) -> List:
        try:
            ks = self.con.keys(key)
        except:
            return None

        ret_keys = list()
        for i in reversed(ks):
            ret_keys.append(i.decode())
        return ret_keys

    def delete(self, key: str) -> None:
        self.con.delete(key)
        return

    def clear_keys(self, wildcard: str) -> None:
        all_keys = self.get_keys(wildcard)
        for i in all_keys:
            self.delete(i)
        return
