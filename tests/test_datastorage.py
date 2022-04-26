# -*- coding: utf-8 -*-

""" test_datastorage.py. Testes for the External Data Storage (@) 2022
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


def test_set_get():
    d = ds.DataStorage("local")
    d.set("tstset", "test")
    v = d.get("tstset")
    assert v == "test", "Should be str(test)"
    d.reset_datastore()


def test_sadd_smembers():
    d = ds.DataStorage("local")
    d.sadd("tstset", "test1")
    d.sadd("tstset", "test2")
    d.sadd("tstset", "test2")
    v = d.smembers("tstset")
    assert sorted(v) == sorted(["test2", "test1"]), "Should be ['test1', 'test2']"
    d.reset_datastore()


def test_bulk_set():
    d = ds.DataStorage("local")
    a = dict()
    for i in range(20):
        a[f"k{i}"] = i
    d.bulk_set(a)
    for k, v in a.items():
        sv = d.get(k)
        assert sv == v, f"Should be {v}"
    d.reset_datastore()


def test_serializer():
    d = ds.DataStorage("local")
    d.set("tstset", "test", ds.StoreType.PLAIN)
    v = d.get("tstset")
    assert v == "test", "Should be str(test)"
    d.reset_datastore()


def test_queue():
    d = ds.DataStorage("local")
    for i in range(10):
        d.enqueue("tstq", i)
    for i in range(10):
        j = d.dequeue("tstq")
        assert i == j, f"Should be {i} == {j}"
    d.reset_datastore()


def test_keys():
    d = ds.DataStorage("local")
    d.reset_datastore()
    key_orig = ["a", "b"]
    for i in key_orig:
        d.set(i, 1)
    k = d.get_keys("*")
    for i in k:
        assert i in key_orig, f"Should be {i} in {key_orig}"
    assert len(k) == len(key_orig), f"Should be {len(key_orig)}"
    d.reset_datastore()


def test_inc_keys():
    d = ds.DataStorage("local")
    v = d.generate_unique_id("test")
    assert v == "test#1", "Should be test#1"
    v = d.generate_unique_id("test")
    assert v == "test#2", "Should be test#2"
    v = d.generate_unique_id("test")
    assert v == "test#3", "Should be test#3"
    d.reset_datastore()
