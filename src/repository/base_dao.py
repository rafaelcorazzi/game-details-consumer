from typing import Any
import pandas as pd
import psycopg2
import os
from configuration import configuration

configs: Any = configuration.get_configs()


class Repository:
    @staticmethod
    def connect():
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**configs['postgres'])
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        print("Connection successful")
        return conn

    @staticmethod
    def copy_from_file(conn, table, json):
        # Save the dataframe to disk
        df = pd.read_json(json)
        df.to_csv("tmp_dataframe.csv", index=False, header=False)
        f = open("tmp_dataframe.csv", 'r')
        cursor = conn.cursor()
        try:
            cursor.copy_from(f, table, sep=",")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            os.remove("tmp_dataframe.csv")
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1

        cursor.close()
        os.remove("tmp_dataframe.csv")