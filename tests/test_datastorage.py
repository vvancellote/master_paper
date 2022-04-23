import gear.storage as ds
import gear.tools.serializer as sz


def test_set_get():
    d = ds.DataStorage()
    d.set("tstset", "test")
    v = d.get("tstset")
    assert v == "test", "Should be str(test)"
    d.clear_keys("*")


def test_sadd_smembers():
    d = ds.DataStorage()
    d.sadd("tstset", "test1")
    d.sadd("tstset", "test2")
    d.sadd("tstset", "test2")
    v = d.smembers("tstset")
    assert sorted(v) == sorted(["test2", "test1"]), "Should be ['test1', 'test2']"
    d.clear_keys("*")


def test_bulk_set():
    d = ds.DataStorage()
    a = dict()
    for i in range(20):
        a[f"k{i}"] = i
    d.bulk_set(a)
    for k, v in a.items():
        sv = d.get(k)
        assert sv == v, f"Should be {v}"
    d.clear_keys("*")


def test_serializer():
    d = ds.DataStorage(
        simple_serializer=sz.Pickler(), compression_serializer=sz.Pickler()
    )
    d.set("tstset", "test")
    v = d.get("tstset")
    assert v == "test", "Should be str(test)"
    d.clear_keys("*")


def test_queue():
    d = ds.DataStorage()
    for i in range(10):
        d.enqueue("tstq", i)
    for i in range(10):
        j = d.dequeue("tstq")
        assert i == j, f"Should be {i} == {j}"
    d.clear_keys("*")


def test_keys():
    d = ds.DataStorage()
    key_orig = ["a", "b"]
    for i in key_orig:
        d.set(i, 1)
    k = d.get_keys("*")
    for i in k:
        assert i in key_orig, f"Should be {i} in {key_orig}"
    assert len(k) == len(key_orig), f"Should be {len(key_orig)}"
    d.clear_keys("*")


def test_inc_keys():
    d = ds.DataStorage()
    v = d.get_unique_id("test")
    assert v == "test:1", "Should be test:1"
    v = d.get_unique_id("test")
    assert v == "test:2", "Should be test:3"
    v = d.get_unique_id("test")
    assert v == "test:3", "Should be test:3"
    d.clear_keys("*")
