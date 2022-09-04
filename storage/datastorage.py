# -*- coding: utf-8 -*-

""" datastorage.py. External Data Storage (@) 2022
This module encapsulates all Parsl configuration stuff in order to provide a
cluster configuration based in number of nodes and cores per node.
This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

# COPYRIGHT SECTION
__author__ = "Diego Carvalho"
__copyright__ = "Copyright 2022"
__credits__ = ["Diego Carvalho"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Diego Carvalho"
__email__ = "d.carvalho@ieee.org"
__status__ = "Research"

import sys
import fnmatch
from io import BytesIO
from enum import Enum
from typing import Any, List

from functools import lru_cache

import redis

from aioredis.client import Pipeline

from tools.serializer import (
    CloudPicklerSerializer,
    CompactedPicklerSerializer,
    NoneSerializer,
    PicklerSerializer,
)


class StoreType(Enum):
    NONE = 0
    PLAIN = 1
    COMPRESSED = 2
    CODE = 3


class Borg:
    _shared_state = {}

    def __init__(self):
        self.__dict__ = self._shared_state


class DataStorage(Borg):

    # Constructor
    def __init__(
        self, store_name: str, host: str = "localhost", port: int = 6379, ex: int = 3600
    ) -> None:
        """DataStorage Constructor

        Args:
            store_name (str): Name of the dictionary used to track the encoding
            host (str, optional): Host where the RedisServer is running. Defaults to "localhost".
            port (int, optional): Port number to be used contating the RedisServer. Defaults to 6379.
        """
        super().__init__()
        # Init object's local status
        self.con = redis.Redis(
            host=host,
            port=port,
            db=0,
            health_check_interval=30,
            socket_timeout=10,
            socket_keepalive=True,
            socket_connect_timeout=10,
            retry_on_timeout=True,
        )
        self.store_name = store_name
        self.host = host
        self.port = port
        self.expire = ex
        self.root_diretory = f"/{self.store_name}/keys"
        self.queue_diretory = f"/{self.store_name}/queue"
        self.unique_id_tag = f"/{self.store_name}/id"
        self.keyname_map = dict()
        self.serializer = dict()
        self.chunk_size = 256 * 1024

        # Build the Serializer Virtual Table
        self.serializer[StoreType.NONE.value] = NoneSerializer()
        self.serializer[StoreType.PLAIN.value] = PicklerSerializer()
        self.serializer[StoreType.COMPRESSED.value] = CompactedPicklerSerializer()
        self.serializer[StoreType.CODE.value] = CloudPicklerSerializer()
        return

    # Private methods
    def __repr__(self) -> str:
        return f"{self.__class__}({self.__dict__})"

    def __str__(self) -> str:
        t = type(self)
        return f"{t.__name__}({self.store_name},{self.host},{self.port})"

    def __find_mapped_key(self, key: str) -> str:
        """Find the internal key representation (with coding in the format "key:coding")

        Args:
            key (str): the user key representation

        Returns:
            str: returns a pair with (mapped_key, coding value) or (None, None) if it fails
        """

        # Check if the key is already on L1 cache
        skey = key if type(key) is str else str(key, "utf-8")
        if skey not in self.keyname_map:
            # So, fetch the key from external store
            k = f"{self.root_diretory}/{skey}"
            ext_data = self.con.hmget(k, ["data", "dtsize", "coding", "chunks"])
            if ext_data is None or ext_data[0] is None:
                # Here, the key is not outthere, so None will be returned
                return None, None, None, None
            else:
                # Happy, since the key is outthere. Map it from the ext_coding info
                self.keyname_map[skey] = {
                    "data": ext_data[0].decode(),
                    "dtsize": int(ext_data[1].decode()),
                    "coding": int(ext_data[2].decode()),
                    "chunks": int(ext_data[3].decode()),
                }  # Update L1 metadata cache
        # The coding is already here, build a mapped key and its coding
        mapped_key = self.keyname_map[skey]["data"]
        dtsize = self.keyname_map[skey]["dtsize"]
        coding = self.keyname_map[skey]["coding"]
        chunks = self.keyname_map[skey]["chunks"]

        return mapped_key, dtsize, coding, chunks

    def _key_data_update(
        self,
        key: str,
        datum: Any,
        coding: StoreType,
        pipe: Pipeline,
        ex: int,
        update_func,
    ):
        # Creates a internal key representation with User's key and Store Type Encoding
        encoded_data = self.serializer[coding.value].encode(datum)

        s_datum = len(encoded_data)
        chunks = int(s_datum / self.chunk_size)
        tail_size = int(s_datum % self.chunk_size)

        skey = key if type(key) is str else str(key, "utf-8")
        mapped_key = f"/{self.store_name}/data/{skey}"

        self.keyname_map[skey] = {
            "data": mapped_key,
            "dtsize": s_datum,
            "coding": coding.value,
            "chunks": chunks,
        }

        # Encode and Store the datum into the set key. Update the key map store
        for kk, vv in self.keyname_map[skey].items():
            k = f"{self.root_diretory}/{skey}"
            pipe.hset(k, kk, vv)
        pipe.expire(k, ex)

        with BytesIO(encoded_data) as stream:
            # Read every chunch from the DataStore
            for this_chunk in range(chunks):
                self.con.expire(f"{mapped_key}:{this_chunk}", self.expire)
                buffer = stream.read(self.chunk_size)
                update_func(f"{mapped_key}:{this_chunk}", buffer)
            # Read the tail from the DataStore
            if tail_size > 0:
                self.con.expire(f"{mapped_key}:{chunks}", self.expire)
                buffer = stream.read(tail_size)
                update_func(f"{mapped_key}:{chunks}", buffer)

        # Update the central directory
        pipe.sadd(self.root_diretory, key)
        # Execute the communication pipeline
        return

    def _key_data_get(self, key: str, get_func) -> Any:
        # Get the internal key representation
        mapped_key, data_size, coding, chunks = self.__find_mapped_key(key)

        # Check if there is a datum named key in the DataStore
        if mapped_key is None:
            return None

        encoded_data = bytes()
        with BytesIO() as stream:
            # Read every chunch from the DataStore
            for this_chunk in range(chunks):
                self.con.expire(f"{mapped_key}:{this_chunk}", self.expire)
                buffer = get_func(f"{mapped_key}:{this_chunk}")
                stream.write(buffer)
            # Read the tail from the DataStore
            if int(data_size % self.chunk_size) > 0:
                self.con.expire(f"{mapped_key}:{chunks}", self.expire)
                buffer = get_func(f"{mapped_key}:{chunks}")
                stream.write(buffer)
            encoded_data = stream.getvalue()

        return encoded_data

    # Public methods
    def set(
        self,
        key: str,
        datum: Any,
        coding: StoreType = StoreType.COMPRESSED,
        ex: int = None,
    ) -> None:
        """Set the memory key with datum

        Args:
            key (str): User key
            datum (Any): the datum
            coding (StoreType, optional): The encoding type to be used. Defaults to StoreType.COMPRESSED.
            ex (int): expiration in seconds, default to 3600.
        """
        # Set the expiration
        ex = ex if ex else self.expire

        if key in self.keyname_map:
            self.delete(key)

        # Initiate the communication pipeline
        with self.con.pipeline() as pipe:
            self._key_data_update(key, datum, coding, pipe, ex, pipe.set)
            # Execute the communication pipeline
            pipe.execute()
        return

    def get(self, key: str, ex: int = 3600) -> Any:
        """get a data stored into a key in the storage memory

        Args:
            key (str): the key to look for
            ex (int): expiration in seconds, default to 3600.

        Returns:
            Any: None if not found, the stored value otherwise
        """
        # try to find a key mapping in the L1 or in the external memory
        mapped_key, dtsize, coding, n_chunks = self.__find_mapped_key(key)
        # if it is none, no key has been found... Return None
        if not mapped_key:
            return None

        # So, we have a key, let's look for a datum
        payload = self._key_data_get(key, self.con.get)
        # if it is none, so this key is note in the storage memory... Return None
        # Actually, it should be an error, but forward it to the upper layers
        if payload is None:
            return None
        # So, habemus datum, decode, refresh both keys and return it
        self.con.expire(key, ex)
        self.con.expire(mapped_key, ex)
        return self.serializer[coding].decode(payload)

    def get_keys(self, wkey: str) -> List:
        """Retrung arbitrary filtered key list from the storage memory

        Args:
            wkey (str): key wildcard

        Returns:
            List: List with keys found into the storage (using the wildcard key)
        """
        # Try to fetch from the storage memory, return None if not found
        key_set = self.con.smembers(self.root_diretory)
        decoded_keys = list()
        for i in sorted(list(key_set)):
            decoded_keys.append(i.decode())

        # Build and return a list of keys. Reversed to respect the creation order and
        # decoded from bytes to strings
        return fnmatch.filter(decoded_keys, wkey)

    def enqueue(
        self,
        key: str,
        datum: Any,
        coding: StoreType = StoreType.COMPRESSED,
        ex: int = 3600,
    ) -> None:
        """Enqueue a datum into a queue

        Args:
            key (str): key is the queue name
            datum (Any): datum to be enqueued
            coding (StoreType, optional): The encoding type to be used. Defaults to StoreType.COMPRESSED.
        """
        # Creates an internal key representation with User's key and Store Type Encoding
        skey = key if type(key) is str else str(key, "utf-8")
        mapped_key = f"/{self.store_name}/data/{skey}"

        self.keyname_map[skey] = {
            "data": mapped_key,
            "dtsize": 0,
            "coding": coding.value,
            "chunks": 0,
        }

        # Encode the data and push it into the queue
        encoded_data = self.serializer[coding.value].encode(datum)

        with self.con.pipeline() as pipe:
            for kk, vv in self.keyname_map[skey].items():
                k = f"{self.root_diretory}/{skey}"
                pipe.hset(k, kk, vv)
            pipe.expire(k, ex)
            pipe.rpush(mapped_key, encoded_data)
            pipe.expire(mapped_key, ex)
            pipe.sadd(self.root_diretory, key)
            pipe.execute()

        return

    def dequeue(
        self, key: str, coding: StoreType = StoreType.COMPRESSED, timeout: int = 0
    ) -> Any:
        """Dequeue an item from the queue

        Args:
            key (str): key is the queue name
            timeout (int, optional): Timeout in seconds (if the queue is empty). Defaults to 0 that means: wait for ever.

        Returns:
            Any: The datum dequeued from the queue, otherwise None
        """
        # try to find a key mapping in the L1 or in the external memory
        mapped_key, data_size, coding, chunks = self.__find_mapped_key(key)

        if mapped_key is None:
            return None

        # try to pop an element from the queue
        item = self.con.blpop(mapped_key, timeout)
        # if found, decode the datum, else, return None
        if item:
            return self.serializer[coding].decode(item[1])
        return item

    def delete_queue(self, key: str) -> None:
        mapped_key, data_size, coding, chunks = self.__find_mapped_key(key)
        self.con.delete(mapped_key)
        self.con.srem(self.root_diretory, key)
        return

    def delete(self, key: str) -> None:
        # try to find a key mapping in the L1 or in the external memory
        # if it has been found, delete the storage key, the key from the storage mapping
        # and from L1
        skey = key if type(key) is str else str(key, "utf-8")
        mapped_key, dtsize, coding, n_chunks = self.__find_mapped_key(key)

        if mapped_key:
            with self.con.pipeline() as pipe:
                k = f"{self.root_diretory}/{skey}"

                for this_chunk in range(n_chunks):
                    pipe.delete(f"{mapped_key}:{this_chunk}")
                # Read the tail from the DataStore
                if int(dtsize % self.chunk_size) > 0:
                    pipe.delete(f"{mapped_key}:{n_chunks}")

                pipe.delete(k)
                pipe.srem(self.root_diretory, skey)
                pipe.execute()
            self.keyname_map.pop(skey, None)

        return

    def reset_datastore(self) -> None:
        for k in self.con.smembers(self.root_diretory):
            self.delete(k)
        with self.con.pipeline() as pipe:
            pipe.delete(self.unique_id_tag)
            pipe.delete(self.root_diretory)
            pipe.execute()
        return
