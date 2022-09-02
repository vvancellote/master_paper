import logging
import sqlite3 as sql
import pandas as pd
from typing import Any


class SqlStorage(object):
    def __init__(self, key: str, directory: str = "database") -> None:
        self.key = key
        self.db_name = f"{directory}/{key}.db"
        self.db_connection = sql.connect(self.db_name)
        return

    def create_table(self, table_name: str, columns: str) -> None:
        query = f"CREATE TABLE {table_name} ({columns});"
        self.db_connection.execute(query)
        return

    def insert_into(self, table_name: str, data: str) -> None:
        query = f"INSERT INTO {table_name} VALUES ({data});"
        try:
            self.db_connection.execute(query)
        except sql.IntegrityError:
            # inserted a duplicate into a primary key column
            raise sql.IntegrityError
        except sql.OperationalError:
            logging.info(
                f"ERROR OperationalError: in ClusterDatabase - {self.db_name} TABLE:{table_name}, ({data})"
            )
        except ValueError:
            logging.info(
                f"ERROR ValueError: in ClusterDatabase - {self.db_name} TABLE:{table_name}, ({data})"
            )
        return

    def fetch_dataframe(self, table_name: str) -> Any:
        df = pd.read_sql(f"SELECT * FROM {table_name}", self.db_connection)
        return df

    def dump_dataframe(self, table_name: str, df):
        df.to_sql(table_name, self.db_connection, if_exists="replace", index=False)
        return

    def commit(self):
        self.db_connection.commit()
        return
