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

from typing import Any, List, Tuple
from parsl import python_app

# Let's test the lint


@python_app
def read_unique_entries_from_file(
    zip_file_name: str,
    next_pipe: Any = None,
    database_dir: str = "database",
    directory: str = "metadata",
    squeue: str = "Q",
) -> Tuple[str, str]:
    import zipfile
    import json as js
    import pandas as pd
    from storage import DataStorage
    from tools.dag import decode_meta_name
    from os.path import isdir, isfile
    from os import mkdir
    import time

    start = time.time()

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
    error_metatadata = dict()
    error_metatadata["MOTIF"] = list()
    error_metatadata["FILENAME"] = list()
    error_metatadata["EXTRAINFO"] = list()

    memory = DataStorage("bus")

    if not isdir(directory):
        mkdir(directory)

    tag = decode_meta_name(zip_file_name)
    meta_group, meta_day = tag[:2], tag[3:]  # format: G1-2017-07-12

    memory.set(f"STATUS-{tag}", "read_unique_entries_from_file")

    # if isfile(f"{directory}/{tag}-ERROR-PH1.parquet"):
    #     if isfile(f"{database_dir}/{tag}.parquet"):
    #         df = pd.read_parquet(f"{database_dir}/{tag}.parquet")

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
                            error_metatadata["MOTIF"].append("DECODE FAIL")
                            error_metatadata["FILENAME"].append(str(file_name))
                            error_metatadata["EXTRAINFO"].append(str(h_tag))
                            continue
                        for each_entry in entries_in_minute_file:
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
                    error_metatadata["MOTIF"].append("FIELDERROR")
                    error_metatadata["FILENAME"].append(str(file_name))
                    error_metatadata["EXTRAINFO"].append(str(h_tag))

                    continue
    except:  # BadZipFile
        error_metatadata["MOTIF"].append("BADZIPFILE")
        error_metatadata["FILENAME"].append(str(zip_file_name))
        error_metatadata["EXTRAINFO"].append(str(tag))

    df = pd.DataFrame(error_metatadata)
    df.to_parquet(f"{directory}/{tag}-ERROR-PH1.parquet")

    memory.set(tag, unique_entries)

    end = time.time()
    meta_stat = dict()
    meta_stat["DATASET"] = tag
    meta_stat["FUNC"] = "read_unique_entries_from_file"
    meta_stat["TIME"] = end - start
    memory.enqueue(f"{squeue}-METASTAT", meta_stat)

    return (meta_group, meta_day)


@python_app
def filter_entries_pipeline(data_future: Any, squeue: str) -> Tuple[str, str]:
    import pandas as pd
    import geopandas as gpd
    from storage import DataStorage
    import time

    start = time.time()

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"
    memory = DataStorage("bus")

    memory.set(f"STATUS-{tag}", "filter_entries_pipeline")

    unique_entries = memory.get(tag)

    area_gpd = gpd.read_file("regions/Limite_de_Bairros.geojson")

    df = pd.DataFrame(
        unique_entries,
        columns=["DATE", "BUSID", "LINE", "LAT", "LONG", "VELOCITY"],
    )

    df["latitude"] = df["LAT"].astype(str)
    df["longitude"] = df["LONG"].astype(str)

    df = df.assign(
        geometry=(
            "POINT Z (" + df["longitude"] + " " + df["latitude"] + " " + "0.00000)"
        )
    )

    cp_union = gpd.GeoDataFrame(
        df.loc[:, [c for c in df.columns if c != "geometry"]],
        geometry=gpd.GeoSeries.from_wkt(df["geometry"]),
        crs="epsg:4326",
    )

    dfjoin = gpd.sjoin(cp_union, area_gpd, how="left")

    dfjoin.drop(
        columns=[
            "latitude",
            "longitude",
            "geometry",
            "Área",
            "AREA_PLANE",
            "LINK",
            "SHAPESTArea",
            "SHAPESTLength",
        ],
        inplace=True,
    )

    memory.set(f"{tag}", pd.DataFrame(dfjoin))

    end = time.time()
    meta_stat = dict()
    meta_stat["DATASET"] = tag
    meta_stat["FUNC"] = "filter_entries_pipeline"
    meta_stat["TIME"] = end - start
    memory.enqueue(f"{squeue}-METASTAT", meta_stat)

    return (meta_group, meta_day)


@python_app
def dump_entries_into_database(
    data_future: Any, directory: str = "database", squeue: str = "Q"
) -> Tuple[str, str]:
    import logging
    from storage import DataStorage
    from os.path import isdir
    from os import mkdir
    import time

    start = time.time()

    if not isdir(directory):
        mkdir(directory)

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"
    memory = DataStorage("bus")

    memory.set(f"STATUS-{tag}", "dump_entries_into_database")

    data_frame = memory.get(tag)

    if type(data_frame) is type(None):
        logging.info(
            f"dump_entries_into_database: cannot find {tag} into the memmory store"
        )
        return (meta_group, meta_day)

    data_frame.to_parquet(f"{directory}/{tag}.parquet")

    end = time.time()
    meta_stat = dict()
    meta_stat["DATASET"] = tag
    meta_stat["FUNC"] = "dump_entries_into_database"
    meta_stat["TIME"] = end - start
    memory.enqueue(f"{squeue}-METASTAT", meta_stat)

    return (meta_group, meta_day)


@python_app
def release_shared_memory(data_future: Any, squeue: str) -> Tuple[str, str]:
    from storage import DataStorage
    import time

    start = time.time()

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"

    memory = DataStorage("bus")
    memory.delete(tag)
    memory.delete(f"STATUS-{tag}")

    end = time.time()
    meta_stat = dict()
    meta_stat["DATASET"] = tag
    meta_stat["FUNC"] = "release_shared_memory"
    meta_stat["TIME"] = end - start
    memory.enqueue(f"{squeue}-METASTAT", meta_stat)

    return (meta_group, meta_day)


@python_app
def calculate_dayly_statistics(
    data_future: Any, directory: str = "statdata", squeue: str = "Q"
) -> Tuple[str, str]:
    import logging
    from storage import DataStorage
    import numpy as np
    import pandas as pd
    from os.path import isdir
    from os import mkdir
    import time

    start = time.time()

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"

    if not isdir(directory):
        mkdir(directory)

    def haversine(lat1, lon1, lat2, lon2, to_radians=True, earth_radius=6371):
        """
        slightly modified version: of http://stackoverflow.com/a/29546836/2901002

        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees or in radians)

        All (lat, lon) coordinates must have numeric dtypes and be of equal length.

        """
        if to_radians:
            lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])

        a = (
            np.sin((lat2 - lat1) / 2.0) ** 2
            + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2.0) ** 2
        )

        return earth_radius * 2 * np.arcsin(np.sqrt(a))

    meta_group, meta_day = data_future
    tag = f"{meta_group}-{meta_day}"
    memory = DataStorage("bus")

    statistics_dict = dict()

    data_frame = memory.get(tag)

    if type(data_frame) is type(None):
        logging.info(
            f"dump_entries_into_database: cannot find {tag} into the memmory store"
        )
        return (meta_group, meta_day)

    data_frame["NEWDATE"] = pd.to_datetime(
        data_frame["DATE"], format="%m-%d-%Y %H:%M:%S", errors="coerce"
    )

    data_frame["NDATE"] = data_frame["NEWDATE"]

    data_frame.index = data_frame["NEWDATE"]

    data_frame.sort_index(inplace=True)

    busid_list = list(data_frame["BUSID"].unique())

    new_columns = list(data_frame.columns)
    new_columns.append("DIST")
    new_columns.append("INTERVAL")
    new_columns.append("AVGSPEED")

    data_frame_result = pd.DataFrame(columns=new_columns)
    data_frame_list = list()

    statistics_dict["DAY"] = tag
    statistics_dict["N_OBS"] = len(data_frame)
    statistics_dict["N_BUS"] = len(busid_list)

    if len(busid_list) > 0:

        for bus in busid_list:
            slice = data_frame[data_frame["BUSID"] == bus]
            bus_df = pd.DataFrame(slice[slice["REGIAO_ADM"].notna()])
            bus_df["DIST"] = haversine(
                bus_df["LAT"],
                bus_df["LONG"],
                bus_df["LAT"].shift(),
                bus_df["LONG"].shift(),
            )
            bus_df["INTERVAL"] = bus_df["NDATE"].diff()
            bus_df["AVGSPEED"] = bus_df["DIST"] / (
                bus_df["INTERVAL"] / np.timedelta64(1, "h")
            )

            bus_df.index = np.arange(len(bus_df))

            data_frame_list.append(bus_df)

        data_frame_result = pd.concat(data_frame_list, ignore_index=True)

        data_frame_result.drop(
            columns=[
                "NEWDATE",
                "NDATE",
                "index_right",
            ],
            inplace=True,
        )

        statistics_dict["FILT_OBS"] = len(data_frame_result)
        statistics_dict["DIST_AVG"] = data_frame_result["DIST"].mean()
        statistics_dict["DIST_STD"] = data_frame_result["DIST"].std()
        try:
            statistics_dict["DIST_MAX"] = data_frame_result["DIST"].max()
            statistics_dict["DIST_MAX_BUS"] = data_frame_result.iloc[
                data_frame_result["DIST"].idxmax()
            ]["BUSID"]
        except:
            statistics_dict["DIST_MAX"] = np.nan
            statistics_dict["DIST_MAX_BUS"] = "NONE"

        statistics_dict["INTERVAL_AVG"] = data_frame_result["INTERVAL"].mean()
        statistics_dict["INTERVAL_STD"] = data_frame_result["INTERVAL"].std()
        try:
            statistics_dict["INTERVAL_MIN"] = data_frame_result["INTERVAL"].min()
            statistics_dict["INTERVAL_MAX"] = data_frame_result["INTERVAL"].max()
            statistics_dict["INTERVAL_MIN_BUS"] = data_frame_result.iloc[
                data_frame_result["INTERVAL"].idxmin()
            ]["BUSID"]
            statistics_dict["INTERVAL_MAX_BUS"] = data_frame_result.iloc[
                data_frame_result["INTERVAL"].idxmax()
            ]["BUSID"]
        except:
            statistics_dict["INTERVAL_MIN"] = np.nan
            statistics_dict["INTERVAL_MAX"] = np.nan
            statistics_dict["INTERVAL_MIN_BUS"] = "NONE"
            statistics_dict["INTERVAL_MAX_BUS"] = "NONE"

        statistics_dict["AVGSPEED_AVG"] = data_frame_result["AVGSPEED"].mean()
        statistics_dict["AVGSPEED_STD"] = data_frame_result["AVGSPEED"].std()
        try:
            statistics_dict["AVGSPEED_MIN"] = data_frame_result["AVGSPEED"].min()
            statistics_dict["AVGSPEED_MAX"] = data_frame_result["AVGSPEED"].max()
            statistics_dict["AVGSPEED_MIN_BUS"] = data_frame_result.iloc[
                data_frame_result["AVGSPEED"].idxmin()
            ]["BUSID"]
            statistics_dict["AVGSPEED_MAX_BUS"] = data_frame_result.iloc[
                data_frame_result["AVGSPEED"].idxmax()
            ]["BUSID"]
        except:
            statistics_dict["AVGSPEED_MIN"] = np.nan
            statistics_dict["AVGSPEED_MAX"] = np.nan
            statistics_dict["AVGSPEED_MIN_BUS"] = "NONE"
            statistics_dict["AVGSPEED_MAX_BUS"] = "NONE"

        statistics_dict["VELOCITY_AVG"] = data_frame_result["VELOCITY"].mean()
        statistics_dict["VELOCITY_STD"] = data_frame_result["VELOCITY"].std()
        try:
            statistics_dict["VELOCITY_MIN"] = data_frame_result["VELOCITY"].min()
            statistics_dict["VELOCITY_MAX"] = data_frame_result["VELOCITY"].max()
            statistics_dict["VELOCITY_MIN_BUS"] = data_frame_result.iloc[
                data_frame_result["VELOCITY"].idxmin()
            ]["BUSID"]
            statistics_dict["VELOCITY_MAX_BUS"] = data_frame_result.iloc[
                data_frame_result["VELOCITY"].idxmax()
            ]["BUSID"]
        except:
            statistics_dict["VELOCITY_MIN"] = np.nan
            statistics_dict["VELOCITY_MAX"] = np.nan
            statistics_dict["VELOCITY_MIN_BUS"] = "NONE"
            statistics_dict["VELOCITY_MAX_BUS"] = "NONE"

        data_frame_result.to_parquet(f"{directory}/{tag}.parquet")

    memory.enqueue(f"{squeue}-STATS", statistics_dict)

    end = time.time()
    meta_stat = dict()
    meta_stat["DATASET"] = tag
    meta_stat["FUNC"] = "calculate_dayly_statistics"
    meta_stat["TIME"] = end - start
    memory.enqueue(f"{squeue}-METASTAT", meta_stat)

    return (meta_group, meta_day)


@python_app
def dump_statistics(
    squeue: str, directory: str = "statdata", inputs: List = []
) -> Tuple[str, str]:
    from storage import DataStorage
    from collections import defaultdict
    import pandas as pd
    import time

    start = time.time()

    memory = DataStorage("bus")
    statistics_dict = defaultdict(list)
    meta_statistics_dict = defaultdict(list)

    memory.enqueue(f"{squeue}-STATS", "END_SENTINEL")
    memory.enqueue(f"{squeue}-METASTAT", "END_SENTINEL")

    item = memory.dequeue(f"{squeue}-STATS")
    while item != "END_SENTINEL":
        for k, v in item.items():
            statistics_dict[k].append(v)
        item = memory.dequeue(f"{squeue}-STATS")

    df = pd.DataFrame(statistics_dict)
    df.to_parquet(f"{directory}/{squeue}-STATS.parquet")

    item = memory.dequeue(f"{squeue}-METASTAT")
    while item != "END_SENTINEL":
        for k, v in item.items():
            meta_statistics_dict[k].append(v)
        item = memory.dequeue(f"{squeue}-METASTAT")

    meta_statistics_dict["DATASET"].append("ALL_DATASETS")
    meta_statistics_dict["FUNC"].append("dump_statistics")
    end = time.time()
    meta_statistics_dict["TIME"].append(end - start)

    df = pd.DataFrame(meta_statistics_dict)
    df.to_parquet(f"{directory}/{squeue}-METASTAT.parquet")

    memory.delete_queue(f"{squeue}-STATS")
    memory.delete_queue(f"{squeue}-METASTAT")

    return squeue
