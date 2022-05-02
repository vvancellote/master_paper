# control redis-server


import subprocess
import time
import redis


class RedisServer(object):
    def __init__(self, host="localhost", port=7777, logfile=None) -> None:
        self.host = host
        self.port = port
        self.logfile = logfile
        return

    def start_redis_server(self) -> None:
        log_filename = (
            self.logfile if self.logfile else f"redis-{self.host}-{self.port}.log"
        )
        with open(log_filename, "a+") as log:
            subprocess.Popen(
                ["redis-server", "--port", f"{self.port}"],
                close_fds=True,
                stdout=log,
                stderr=log,
            )

        time.sleep(2)
        return

    def stop_redis_server(self) -> None:
        con = redis.Redis(host=self.host, port=self.port, db=0)
        con.shutdown(False)
        return
