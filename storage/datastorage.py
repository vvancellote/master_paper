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

from typing import Any, List
from tools.serializer import (
    CompactedPicklerSerializer,
    PicklerSerializer,
    CloudPicklerSerializer,
    NoneSerializer,
)
import redis
from enum import Enum


class StoreType(Enum):
    NONE = 0
    PLAIN = 1
    COMPRESSED = 2
    CODE = 3


class DataStorage(object):

    # Constructor
    def __init__(
        self,
        store_name: str,
        host: str = "localhost",
        port: int = 6379,
    ) -> None:
        """DataStorage Constructor

        Args:
            store_name (str): Name of the dictionary used to track the encoding
            host (str, optional): Host where the RedisServer is running. Defaults to "localhost".
            port (int, optional): Port number to be used contating the RedisServer. Defaults to 6379.
        """
        # Init object's local status
        self.con = redis.Redis(host=host, port=port, db=0)
        self.store_name = store_name
        self.host = host
        self.port = port
        self.keyname_map = dict()
        self.serializer = dict()

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
        return f"{t.__name__}({self.host},{self.port})"

    def __map_key(self, key: str, coding: StoreType) -> str:
        """Update L1 key cache only with key to internal with coding type

        Args:
            key (str): external key
            coding (StoreType): coding type

        Returns:
            str: returns a string with the mapped key
        """
        assert key not in self.keyname_map
        data_key = self.generate_unique_id(key)
        self.keyname_map[key] = {
            "data": data_key,
            "coding": coding.value,
        }  # Update L1 metadata cache
        return data_key

    def __find_mapped_key(self, key: str) -> str:
        """Find the internal key representation (with coding in the format "key:coding")

        Args:
            key (str): the user key representation

        Returns:
            str: returns a pair with (mapped_key, coding value) or (None, None) if it fails
        """

        # Check if the key is already on L1 cache
        if key not in self.keyname_map:
            # So, fetch the key from external store
            ext_data = self.con.hmget(key, ["data", "coding"])
            if ext_data is None or ext_data[0] is None:
                # Here, the key is not outthere, so None will be returned
                return None, None
            else:
                # Happy, since the key is outthere. Map it from the ext_coding info
                self.keyname_map[key] = {
                    "data": ext_data[0].decode(),
                    "coding": int(ext_data[1].decode()),
                }  # Update L1 metadata cache
        # The coding is already here, build a mapped key and its coding
        mapped_key = self.keyname_map[key]["data"]
        coding = self.keyname_map[key]["coding"]

        return mapped_key, coding

    def __read_keys_from_storage(self, key: str) -> List:
        """Read arbitrary key from the storage memory

        Args:
            key (str): key wildcard

        Returns:
            List: List with keys found into the storage (using the wildcard key)
        """
        # Try to fetch from the storage memory, return None if not found
        try:
            ks = self.con.keys(f"{key}")
        except:
            return None

        # Build and return a list of keys. Reversed to respect the creation order and
        # decoded from bytes to strings
        ret_keys = list()
        for i in reversed(ks):
            ret_keys.append(i.decode())
        return ret_keys

    # Public methods
    def set(
        self, key: str, datum: Any, coding: StoreType = StoreType.COMPRESSED
    ) -> None:
        """Set the memory key with datum

        Args:
            key (str): User key
            datum (Any): the datum
            coding (StoreType, optional): The encoding type to be used. Defaults to StoreType.COMPRESSED.
        """
        # Creates a internal key representation with User's key and Store Type Encoding
        mapped_key = self.__map_key(key, coding)
        # Initiate the communication pipeline
        pipeline = self.con.pipeline()
        # Encode and Store the datum. Update the key map store
        pipeline.hset(key, mapping=self.keyname_map[key])
        encoded_data = self.serializer[coding.value].encode(datum)
        pipeline.set(mapped_key, encoded_data)
        # Execute the communication pipeline
        pipeline.execute()
        return

    def bulk_set(self, data: dict, coding: StoreType = StoreType.COMPRESSED) -> None:
        """Set various keys at the same time

        Args:
            data (dict): a dictionary where keys are keys and values are data
            coding (StoreType, optional): The encoding type to be used. Defaults to StoreType.COMPRESSED.
        """
        # Initiate the communication pipeline
        pipeline = self.con.pipeline()
        # Loop over each key...
        for k, v in data.items():
            # Creates a internal key representation with User's key and Store Type Encoding
            mapped_key = self.__map_key(k, coding)
            # Encode and Store the datum. Update the key map store
            pipeline.hset(self.store_name, k, self.keyname_map[k])
            encoded_data = self.serializer[coding.value].encode(v)
            pipeline.set(mapped_key, encoded_data)
        # Execute the communication pipeline
        pipeline.execute()
        return

    def get(self, key: str) -> Any:
        """get a data stored into a key in the storage memory

        Args:
            key (str): the key to look for

        Returns:
            Any: None if not found, the stored value otherwise
        """
        # try to find a key mapping in the L1 or in the external memory
        mapped_key, coding = self.__find_mapped_key(key)
        # if it is none, no key has been found... Return None
        if not mapped_key:
            return None

        # So, we have a key, let's look for a datum
        payload = self.con.get(mapped_key)
        # if it is none, so this key is note in the storage memory... Return None
        # Actually, it should be an error, but forward it to the upper layers
        if payload is None:
            return None
        # So, habemus datum, decode and return it
        return self.serializer[coding].decode(payload)

    def sadd(self, key: str, datum: Any, coding: StoreType = StoreType.PLAIN) -> None:
        """Add the datum to a memory Set

        Args:
            key (str): the user's key
            datum (Any): the datum to be inserted to the set
            coding (StoreType, optional): The encoding type. Defaults to StoreType.PLAIN.
        """
        # Creates a internal key representation with User's key and Store Type Encoding
        mapped_key = self.__map_key(key, coding)
        # Initiate the communication pipeline
        pipeline = self.con.pipeline()
        # Encode and Store the datum into the set key. Update the key map store
        pipeline.hset(self.store_name, key, self.keyname_map[key])
        encoded_data = self.serializer[coding.value].encode(datum)
        pipeline.sadd(mapped_key, encoded_data)
        # Execute the communication pipeline
        pipeline.execute()
        return

    def bulk_sadd(
        self, key: str, data: set, coding: StoreType = StoreType.COMPRESSED
    ) -> None:
        """Add bulk data into a memory set

        Args:
            key (str): memory set key
            data (set): a set containing all elements to be stored in the memory set
            coding (StoreType, optional): The encoding type to be used. Defaults to StoreType.COMPRESSED.
        """
        # Initiate the communication pipeline
        pipeline = self.con.pipeline()
        # Create a internal key representation with User's key and Store Type Encoding
        mapped_key = self.__map_key(key, coding)
        # Loop over the data set
        for v in data:
            # Encode and Store the datum. Update the key map store
            encoded_data = self.serializer[coding.value].encode(v)
            pipeline.sadd(mapped_key, encoded_data)
        # Execute the communication pipeline
        pipeline.execute()
        return

    def smembers(self, key: str) -> List:
        """Fetch all members of the set named key

        Args:
            key (str): set name

        Returns:
            List: List with all members in the set. None if there is no such set
        """
        # try to find a key mapping in the L1 or in the external memory
        mapped_key, coding = self.__find_mapped_key(key)
        # try to fetch a set from the storage memory
        from_store = self.con.smembers(mapped_key)

        # Loop over the returned list and decode each datum.
        ret_val = list()
        for i in from_store:
            ret_val.append(self.serializer[coding].decode(i))
        return ret_val

    def enqueue(
        self, key: str, datum: Any, coding: StoreType = StoreType.COMPRESSED
    ) -> None:
        """Enqueue a datum into a queue

        Args:
            key (str): key is the queue name
            datum (Any): datum to be enqueued
            coding (StoreType, optional): The encoding type to be used. Defaults to StoreType.COMPRESSED.
        """
        # Creates an internal key representation with User's key and Store Type Encoding
        mapped_key = self.__map_key(key, coding)
        # Encode the data and push it into the queue
        encoded_data = self.serializer[coding.value].encode(datum)
        self.con.rpush(mapped_key, encoded_data)
        self.con.hset()
        return

    def dequeue(self, key: str, timeout: int = 0) -> Any:
        """Dequeue an item from the queue

        Args:
            key (str): key is the queue name
            timeout (int, optional): Timeout in seconds (if the queue is empty). Defaults to 0 that means: wait for ever.

        Returns:
            Any: The datum dequeued from the queue, otherwise None
        """
        # try to find a key mapping in the L1 or in the external memory
        mapped_key, coding = self.__find_mapped_key(key)
        # try to pop an element from the queue
        item = self.con.blpop(mapped_key, timeout)
        # if found, decode the datum, else, return None
        if item:
            return self.serializer[coding].decode(item[1])
        return item

    def generate_unique_id(self, key: str) -> str:
        """Create a new unique id based on key

        Args:
            key (str): the basename of the unique id.

        Returns:
            str: A string based on the key with an unique interger
        """
        # Creates a internal key representation with User's key and Store Type Encoding
        mapped_key = self.__map_key(key, StoreType.PLAIN)
        self.con.hset(self.store_name, key, self.keyname_map[key])
        # Increment the current counter on key
        val = self.con.incr(mapped_key)
        # Build the returning unique id
        return f"{key}#{val}"

    def get_keys(self, key: str) -> List:
        ks = self.__read_keys_from_storage(f"{key}:*")

        ret_keys = list()
        for i in reversed(ks):
            key_name = i.split(":")[0]
            mapped_key, _ = self.__find_mapped_key(key_name)
            if mapped_key:
                ret_keys.append(key_name)
        return ret_keys

    def delete(self, key: str) -> None:
        # try to find a key mapping in the L1 or in the external memory
        # if it has been found, delete the storage key, the key from the storage mapping
        # and from L1
        mapped_key, _ = self.__find_mapped_key(key)
        if mapped_key:
            self.con.delete(mapped_key)
            self.con.hdel(self.store_name, key)
            self.keyname_map.pop(key, None)
        return

    def reset_datastore(self) -> None:
        for i in self.con.hgetall(self.store_name).keys():
            mapped_key, _ = self.__find_mapped_key(i.decode())
            self.con.delete(mapped_key)
        self.con.delete(self.store_name)
        return
