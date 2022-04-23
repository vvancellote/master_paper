import pickle
import cloudpickle
from abc import ABC, abstractmethod
from typing import Any, List

import blosc


class Serializer(ABC):
    """Base class for serializers.
    Serializers must implement the serialize and deserialize methods.
    The protocol is to return None if something wrong happens underground.
    """

    def __init__(self):
        pass

    def encode(self, value):
        """Encode value."""
        try:
            value = self.serialize(value)
        except:
            value = None
        return value

    def decode(self, value):
        """Decode value."""
        try:
            result = self.deserialize(value)
        except:
            result = None
        return result

    @abstractmethod
    def serialize(self, value):
        pass

    @abstractmethod
    def deserialize(self, value):
        pass


class Pickler(Serializer):
    """The pickle serializer."""

    protocol = 5

    def serialize(self, value):
        """Encode value to pickle format."""
        return pickle.dumps(value, protocol=self.protocol)

    def deserialize(self, value):
        """Decode pickled value to Python object."""
        return pickle.loads(value)


class CloudPickler(Serializer):
    """The cloudpickle serializer."""

    def serialize(self, value):
        """Encode value to pickle format."""
        return cloudpickle.dumps(value)

    def deserialize(self, value):
        """Decode pickled value to Python object."""
        return pickle.loads(value)


class CompactedPickler(Serializer):
    """The compacted pickle serializer."""

    def __init__(self, protocol: int = 5, cname: str = "blosclz"):
        self.protocol = protocol
        self.cname = cname

    def serialize(self, value):
        """Encode value to pickle format."""
        return blosc.compress(
            pickle.dumps(value, protocol=self.protocol), cname=self.cname
        )

    def deserialize(self, value):
        """Decode pickled value to Python object."""
        return pickle.loads(blosc.decompress(value))
