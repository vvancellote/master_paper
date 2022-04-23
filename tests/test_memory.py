import gear.storage as ds
import gear.tools.serializer as sz
import gear.storage.memory as mm


def test_memory():
    redis = mm.RedisServer(port=7777)
    redis.start_redis_server()
    d = ds.DataStorage(port=redis.port)
    d.set("tstset", "test")
    v = d.get("tstset")
    assert v == "test", "Should be str(test)"
    d.clear_keys("*")
    redis.stop_redis_server()
