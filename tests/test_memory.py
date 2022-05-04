# -*- coding: utf-8 -*-

""" test_memory.py. Tests for the External Data Storage (@) 2022
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

import gear.storage as ds
import gear.storage.memory as mm
import gear.tools.serializer as sz


def test_memory():
    redis = mm.RedisServer(port=7777)
    redis.start_redis_server()
    d = ds.DataStorage("local", port=redis.port)
    d.set("tstset", "test")
    v = d.get("tstset")
    assert v == "test", "Should be str(test)"
    d.reset_datastore()
    redis.stop_redis_server()
