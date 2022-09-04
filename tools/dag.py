from typing import Any
import os
import glob

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.monitoring import MonitoringHub
from parsl.executors import HighThroughputExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.addresses import address_by_hostname, address_by_query, address_by_route

from storage import DataStorage

from itertools import islice


def chunk(arr_range, arr_size):
    arr_range = iter(arr_range)
    return iter(lambda: tuple(islice(arr_range, arr_size)), ())


class CircularList(object):
    def __init__(self, slots: int) -> None:
        if not slots:
            raise ValueError
        self.list = [None for _ in range(slots)]
        self.index = 0
        self.max_index = len(self.list) - 1

    def next(self) -> Any:
        if self.index == self.max_index:
            self.index = 0
        else:
            self.index += 1
        return self.list[self.index]

    def current(self, value: Any) -> None:
        self.list[self.index] = value
        return


class Barrier(object):
    pass


def get_parsl_config(kind: str, monitoring: bool = False) -> Config:
    conf = None
    env_str = str()

    with open("parsl.env", "r") as reader:
        env_str = reader.read()

    conf = (
        Config(executors=[ThreadPoolExecutor(max_threads=8, label=kind, managed=False)])
        if kind == "local_threads"
        else Config(
            executors=[
                HighThroughputExecutor(
                    label=kind,
                    provider=LocalProvider(
                        channel=LocalChannel(),
                        init_blocks=1,
                        max_blocks=1,
                        worker_init=env_str,
                    ),
                    max_workers=22,
                )
            ],
            monitoring=None
            if not monitoring
            else MonitoringHub(
                hub_address=address_by_hostname(),
                hub_port=55055,
                monitoring_debug=False,
                resource_monitoring_interval=10,
            ),
            strategy=None,
        )
    )

    return conf


def decode_meta_name(file_name):
    info = os.path.basename(file_name).split(".")
    return info[0]


def populate_workflow():
    memory = DataStorage("bus")
    memory.reset_datastore()
    file_list = list()
    for i in sorted(glob.glob("busdata/*.zip")):
        file_list.append(decode_meta_name(i))
    memory.set("WORKFLOW", file_list)


def remove_done_workflow():
    memory = DataStorage("bus")
    os.system("rm -f database/*-journal")
    work_list = memory.get("WORKFLOW")
    if work_list == None:
        print("Empty worklist. Nothing to do.")
        return
    status = memory.get_keys("STATUS-*")
    for i in status:
        item_to_remove = str(i[7:])
        print(f"Removing jornal {item_to_remove}.")
        memory.delete(item_to_remove)
        os.system(f"rm -f database/{item_to_remove}.feather")

    for i in sorted(glob.glob("database/*.feather")):
        item_to_remove = decode_meta_name(i)
        if item_to_remove in work_list:
            print(f"Removing processed file {item_to_remove}.")
            memory.delete(item_to_remove)
            work_list.remove(item_to_remove)
    memory.set("WORKFLOW", work_list)
    return
