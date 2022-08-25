""" applications.py. Bus Reconstruction Applications (@) 2022
This module encapsulates all Parsl applications used in the reconstruction 
processes.
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

from typing import Any, Tuple
from parsl import python_app

# Let's test the lint


@python_app
def read_unique_entries_from_file(
    zip_file_name: str, next_pipe: Any = None
) -> Tuple[str, str]:
    import zipfile
    import json as js
    from collections import defaultdict
    from storage import DataStorage
    from tools.dag import decode_meta_name

    def decode_entries_in_file(file_name, f, null_DATA):
        data = dict()
        data["DATA"] = null_DATA
        try:
            data = js.load(f)
        except ValueError:
            # includes simplejson.decoder.JSONDecodeError
            # invalid JSON numbers are encountered
            pass
        return data["DATA"]

    null_DATA = [[]]
    unique_entries = set()
    error_metatadata = defaultdict(list)
    memory = DataStorage("bus")

    tag = decode_meta_name(zip_file_name)
    meta_group, meta_day = tag[:2], tag[3:]  # format: G1-2017-07-12

    memory.set(f"STATUS:{tag}", "read_unique_entries_from_file")

    try:
        with zipfile.ZipFile(zip_file_name) as file_handler:
            for file_name in file_handler.namelist():
                meta_hour = decode_meta_name(file_name)
                h_tag = f"{meta_day}:{meta_hour}"
                try:
                    with file_handler.open(file_name, "r") as f:
                        entries_in_minute_file = decode_entries_in_file(
                            file_name, f, null_DATA
                        )
                        if (
                            len(entries_in_minute_file) == 0
                            or entries_in_minute_file == null_DATA
                        ):
                            error_metatadata["FAIL"].append(h_tag)
                            continue
                        for each_entry in entries_in_minute_file:
                            error_metatadata[each_entry[1]].append(h_tag)
                            unique_entries.add(
                                (
                                    str(
                                        each_entry[0]
                                    ),  # convert the GPS time into a string
                                    str(
                                        each_entry[1]
                                    ),  # convert the bus index into a string
                                    str(
                                        each_entry[2]
                                    ),  # convert the busline or service to str
                                    float(
                                        each_entry[3]
                                    ),  # convert the latitude into a float
                                    float(
                                        each_entry[4]
                                    ),  # convert the longitude into a float
                                    float(
                                        each_entry[5]
                                    ),  # convert the velocity into a float
                                )
                            )
                except:  # zipfile.BadZipFile
                    error_metatadata["FAIL"].append(h_tag)
                    continue
    except:  # BadZipFile
        error_metatadata["FILE"].append(tag)

    memory.set(f"PH1:{tag}", error_metatadata)
    memory.set(tag, unique_entries)
    return (meta_group, meta_day)


@python_app
def filter_entries_pipeline(data_future) -> Tuple[str, str]:
    from collections import defaultdict
    from storage import DataStorage

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"
    memory = DataStorage("bus")

    memory.set(f"STATUS:{tag}", "filter_entries_pipeline")

    unique_entries = memory.get(tag)

    new_filtered_unique_set = set()
    error_metatadata = defaultdict(list)

    for each_entry in unique_entries:
        gps_date, bus_id, bus_line, latitude, longitude, velocity = each_entry
        found = False

        if velocity < 0.0 or velocity > 120.0:
            error_metatadata[bus_id].append(
                f"'VELOCITY', '{gps_date}', '{bus_line}', {latitude}, {longitude}, {velocity}"
            )
            found = True

        if latitude < -23.09022 or latitude > -22.73224:
            error_metatadata[bus_id].append(
                f"'LATITUDE', '{gps_date}', '{bus_line}', {latitude}, {longitude}, {velocity}"
            )
            found = True

        if longitude < -43.82515 or longitude > -43.07808:
            error_metatadata[bus_id].append(
                f"'LONGITUDE', '{gps_date}', '{bus_line}', {latitude}, {longitude}, {velocity}"
            )
            found = True

        if found:
            continue

        new_filtered_unique_set.add(
            (gps_date, bus_id, bus_line, latitude, longitude, velocity)
        )

    memory.set(f"PH2:{tag}", error_metatadata)
    memory.set(f"{tag}", new_filtered_unique_set)
    return (meta_group, meta_day)


@python_app
def dump_entries_into_database(data_future) -> Tuple[str, str]:
    import logging
    from storage import DataStorage, SqlStorage

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"
    memory = DataStorage("bus")
    database = SqlStorage(tag)

    memory.set(f"STATUS:{tag}", "dump_entries_into_database")

    unique_entries = memory.get(tag)

    if unique_entries == None:
        logging.info(
            f"dump_entries_into_database: cannot find {tag} into the memmory store"
        )
        return (meta_group, meta_day)

    database.create_table(
        "GPSDATA",
        "DATE TEXT,BUSID TEXT,LINE TEXT,LATITUDE REAL,LONGITUDE REAL,VELOCITY REAL",
    )

    for each_entry in unique_entries:
        gps_date, bus_id, bus_line, latitude, longitude, velocity = each_entry
        data = f"'{gps_date}', '{bus_id}', '{bus_line}', {latitude}, {longitude}, {velocity}"
        database.insert_into("GPSDATA", data)

    database.commit()
    return (meta_group, meta_day)


@python_app
def dump_entries_into_datastore(data_future) -> Tuple[str, str]:
    import logging
    import pandas as pd
    from storage import DataStorage, SqlStorage

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"
    memory = DataStorage("bus")
    database = SqlStorage(tag)

    memory.set(f"STATUS:{tag}", "dump_entries_into_database")

    unique_entries = memory.get(tag)

    if unique_entries == None:
        logging.info(
            f"dump_entries_into_datafile: cannot find {tag} into the memmory store"
        )
        return (meta_group, meta_day)

    df = pd.DataFrame(
        unique_entries,
        columns=["DATE", "BUSID", "LINE", "LATITUDE", "LONGITUDE", "VELOCITY"],
    )

    df["DATE"] = pd.to_datetime(df["DATE"])

    df.to_hdf(
        f"hdf5/{meta_day}.h5",
        key="GPSDATA",
        mode="a",
        data_columns=["DATE", "BUSID", "LINE"],
    )

    return (meta_group, meta_day)


@python_app
def loop_over_metadata_phase_1(data_future) -> Tuple[str, str]:
    from storage import DataStorage, SqlStorage

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"

    memory = DataStorage("bus")
    database = SqlStorage(tag)

    memory.set(f"STATUS:{tag}", "loop_over_metadata_phase_1")

    error_metatadata = memory.get(f"PH1:{tag}")

    database.create_table("METADATAPH1", "BUSID TEXT, VAL TEXT")

    for key, val in error_metatadata.items():
        for i in val:
            data = f"'{key}', '{i}'"
            database.insert_into("METADATAPH1", data)

    database.commit()
    memory.delete(f"PH1:{tag}")
    return (meta_group, meta_day)


@python_app
def loop_over_metadata_phase_2(data_future) -> Tuple[str, str]:
    from storage import DataStorage, SqlStorage

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"

    memory = DataStorage("bus")
    database = SqlStorage(tag)

    memory.set(f"STATUS:{tag}", "loop_over_metadata_phase_2")

    error_metatadata = memory.get(f"PH2:{tag}")
    # f"LONGITUDE, {gps_date}, {bus_line}, {latitude}, {longitude}, {velocity}"

    database.create_table(
        "METADATAPH2",
        "BUSID TEXT, MOTIF TEXT, DATE TEXT, LINE TEXT, LATITUDE REAL, LONGITUDE REAL, VELOCITY REAL",
    )

    for key, val in error_metatadata.items():
        for i in val:
            data = f"'{key}', {i}"
            database.insert_into("METADATAPH2", data)

    database.commit()
    memory.delete(f"PH2:{tag}")
    return (meta_group, meta_day)


@python_app
def release_shared_memory(data_future) -> Tuple[str, str]:
    from storage import DataStorage

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"

    memory = DataStorage("bus")
    memory.delete(tag)
    memory.delete(f"STATUS:{tag}")

    return (meta_group, meta_day)


@python_app
def calculate_dayly_statistics(data_future) -> Tuple[str, str]:
    import logging
    from storage import DataStorage, SqlStorage

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"
    memory = DataStorage("bus")
    database = SqlStorage(tag)

    memory.set(f"STATUS:{tag}", "calculate_dayly_statistics")

    unique_entries = memory.get(tag)

    if unique_entries == None:
        logging.info(
            f"dump_entries_into_database: cannot find {tag} into the memmory store"
        )
        return (meta_group, meta_day)

    database.create_table(
        "GPSDATA",
        "DATE TEXT,BUSID TEXT,LINE TEXT,LATITUDE REAL,LONGITUDE REAL,VELOCITY REAL",
    )

    for each_entry in unique_entries:
        gps_date, bus_id, bus_line, latitude, longitude, velocity = each_entry
        data = f"'{gps_date}', '{bus_id}', '{bus_line}', {latitude}, {longitude}, {velocity}"
        database.insert_into("GPSDATA", data)

    database.commit()
    return (meta_group, meta_day)
