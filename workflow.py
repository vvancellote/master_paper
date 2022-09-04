import logging
import os
import statistics
import sys
from storage import DataStorage
from tools.dag import (
    get_parsl_config,
    remove_done_workflow,
    CircularList,
    populate_workflow,
)

import parsl

from applications import (
    release_shared_memory,
    dump_entries_into_database,
    filter_entries_pipeline,
    read_unique_entries_from_file,
    calculate_dayly_statistics,
    dump_statistics,
)


def main():

    memory = DataStorage("bus")

    if len(sys.argv) == 2 and sys.argv[1] == "init":
        memory.reset_datastore()
        populate_workflow()

    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    logging.info(f"Workflow STARTING.")
    parsl.load(get_parsl_config("htex_Local"))
    logging.info(f"Workflow IS RUNNING.")

    result = list()

    remove_done_workflow()

    worklist = memory.get("WORKFLOW")

    pool = CircularList(23)

    database_dir = "../processed/database"
    metadata_dir = "../processed/metadata"
    statistics_dir = "../processed/statdata"

    for file_id, zip_file_name in enumerate(worklist):
        f0 = read_unique_entries_from_file(
            f"busdata/{zip_file_name}.zip", pool.next(), metadata_dir
        )
        f0 = filter_entries_pipeline(f0)
        f0 = dump_entries_into_database(f0, database_dir)
        f0 = calculate_dayly_statistics(f0, statistics_dir)
        f0 = release_shared_memory(f0)

        pool.current(f0)
        result.append(f0)

        freq = 100
        if (file_id % freq) == (freq / 2):
            f = dump_statistics(f"GENERAL-{file_id}", f0, statistics_dir)

    for ready_data in result:
        meta_group, meta_day = ready_data.result()
        tag = f"{meta_group}-{meta_day}"
        logging.info(f"Workflow FINISHED with {tag}")

    f = dump_statistics(f"GENERAL-{len(worklist)}", "END", statistics_dir)
    logging.info(f"Workflow DUMPING STATS.")
    f.result()


if __name__ == "__main__":
    main()
