import json
import logging
import sqlite3 as sql

#
# SetEncoder Class
#
class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


class SqlStorage(object):
    def __init__(self, db_name: str = "storageset.db") -> None:
        self.bus_id_set = set()
        self.db_connection = sql.connect(db_name)
        query = f"""
            CREATE TABLE METADATA (
                DATE TEXT PRIMARY KEY, LINE TEXT, 
                LATITUDE REAL, LONGITUDE REAL, VELOCITY REAL
            ); 
            """
        self.db_connection.execute(query)
        return

    def __len__(self) -> int:
        return len(self.bus_id_set)

    def __create_db_entry__(self, bus_id: str) -> None:
        tag = bus_id.upper()
        query = f"""
            CREATE TABLE {tag} (
                DATE TEXT PRIMARY KEY, LINE TEXT, 
                LATITUDE REAL, LONGITUDE REAL, VELOCITY REAL
            ); 
            """
        self.db_connection.execute(query)
        return

    def add(self, value) -> None:
        obs_date, bus_id, bus_line, latitude, longitude, velocity = value
        if bus_id not in self.bus_id_set:
            self.bus_id_set.add(bus_id)
            self.__create_db_entry__(bus_id)
        tag = bus_id.upper()
        query = f"""
            INSERT INTO {tag} VALUES (
                '{obs_date}', '{bus_line}', {latitude}, {longitude}, {velocity}
            ); 
            """
        try:
            self.db_connection.execute(query)
        except sql.IntegrityError:
            logging.info(f"Duplicated entry {value}")
        return

    def bulk_add(self, set_value: set) -> None:
        for i in set_value:
            self.add(i)

        self.db_connection.commit()
        return

    def len(self) -> int:
        return len(self.bus_id_set)

    def dump(self, file_name):
        with open(file_name, "w") as f:
            json.dump(self.bus_id_set, f, cls=SetEncoder)
        self.db_connection.commit()
        return
