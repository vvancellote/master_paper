# -*- coding: utf-8 -*-

""" serializer.py. Serializer for the External Data Storage (@) 2022
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


class NoneSerializer(Serializer):
    def serialize(self, value):
        """Encode value to Plain format."""
        return value

    def deserialize(self, value):
        """Decode Plain value to Python object."""
        return value


class PicklerSerializer(Serializer):
    """The pickle serializer."""

    protocol = 5

    def serialize(self, value):
        """Encode value to pickle format."""
        return pickle.dumps(value, protocol=self.protocol)

    def deserialize(self, value):
        """Decode pickled value to Python object."""
        return pickle.loads(value)


class CloudPicklerSerializer(Serializer):
    """The cloudpickle serializer."""

    def serialize(self, value):
        """Encode value to pickle format."""
        return cloudpickle.dumps(value)

    def deserialize(self, value):
        """Decode pickled value to Python object."""
        return pickle.loads(value)


class CompactedPicklerSerializer(Serializer):
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
