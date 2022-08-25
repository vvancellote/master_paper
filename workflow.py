import logging
import os

import parsl


def main():
    from storage import DataStorage
    from tools.dag import get_parsl_config, remove_done_workflow, CircularList
    from applications import (
        release_shared_memory,
        loop_over_metadata_phase_2,
        loop_over_metadata_phase_1,
        dump_entries_into_database,
        filter_entries_pipeline,
        read_unique_entries_from_file,
    )

    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    logging.info(f"Workflow STARTING.")
    parsl.load(get_parsl_config("htex_Local"))
    logging.info(f"Workflow IS RUNNING.")

    result = list()

    memory = DataStorage("bus")

    remove_done_workflow()

    worklist = memory.get("WORKFLOW")

    pool = CircularList(22)

    for zip_file_name in worklist:
        future = release_shared_memory(
            loop_over_metadata_phase_2(
                loop_over_metadata_phase_1(
                    # dump_entries_into_datastore(
                    dump_entries_into_database(
                        filter_entries_pipeline(
                            read_unique_entries_from_file(
                                f"busdata/{zip_file_name}.zip", pool.next()
                            )
                        )
                    )
                    # )
                )
            )
        )

        pool.current(future)
        result.append(future)

    for ready_data in result:
        meta_group, meta_day = ready_data.result()
        tag = f"{meta_group}-{meta_day}"
        logging.info(f"Workflow FINISHED with {tag}")


if __name__ == "__main__":
    main()
