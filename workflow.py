import logging
import os
import statistics
import sys
from storage import DataStorage
from tools.dag import (
    chunk,
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

    stat_result = list()

    remove_done_workflow()

    worklist = memory.get("WORKFLOW")

    pool = CircularList(23)

    database_dir = "../processed/database"
    metadata_dir = "../processed/metadata"
    statistics_dir = "../processed/statdata"

    for ch_id, chunk_list in enumerate(chunk(worklist, 50)):
        stat_queue = f"Q{ch_id}"
        result = list()
        for zip_file_name in chunk_list:
            f0 = read_unique_entries_from_file(
                f"busdata/{zip_file_name}.zip", pool.next(), metadata_dir, stat_queue
            )
            f0 = filter_entries_pipeline(f0, stat_queue)
            f0 = dump_entries_into_database(f0, database_dir, stat_queue)
            f0 = calculate_dayly_statistics(f0, statistics_dir, stat_queue)
            f0 = release_shared_memory(f0, stat_queue)

            pool.current(f0)
            result.append(f0)

        f = dump_statistics(stat_queue, "END", statistics_dir, inputs=result)
        stat_result.append(f)

    for ready_data in stat_result:
        tag = ready_data.result()
        logging.info(f"Workflow FINISHED with {tag}")


if __name__ == "__main__":
    main()
