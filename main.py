import argparse
import os
import sys
from typing import Optional, List
import pandas as pd
import psycopg2 
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

class ExtractCSV:
    """
    A class for parsing the csv file columns, data types, and records

    Attributes
        csv_path (str): path to the csv file
        columns (Index[str]): an index object with all column names as strings
        data_types (Series): a pandas series of all data types for each column
        data (list[Series]): a list of pandas series representing each record in the csv file 
    """
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.data = []

        try:
            self.__df = pd.read_csv(self.csv_path)
            self.columns = self.__df.columns
            self.data_types = self.__df.dtypes

            for i in range(len(self.__df)):
                self.data.append(self.__df.iloc[i])

        except FileNotFoundError:
            print(f'Opps the csv file path {self.csv_path} does not exist!')
            sys.exit()


class DBConnection:
    """
    A data class used to represent a database connection.
    """
    def __init__(self, db_name:str, user:str, host: str, password: str, port:str):
        self.db_name: str = db_name
        self.user: str = user
        self.host: str = host
        self.password: str = password
        self.port: str = port 


class PopulateDB:
    """
    A class for all database operations

    Attributes
        db_conn (DBConnection): database connection data based on the DBConnection class
        db_name (str): name of the database to create

    Methods
        test_db_conn(): void: fetches all the records and prints the to stdout
        create_new_db(): void: creates a new database name based on db_name; exists program with message if there's an error
        create_table(): void: 
    """
    def __init__(self,db_conn: DBConnection, db_name: str):
        self.db_conn = db_conn
        self.db_name: str = db_name
        # open db connection
        self.__conn = psycopg2.connect(
            f"dbname='postgres' user='{db_conn.user}' host='{db_conn.host}' password='{db_conn.password}' port='{db_conn.port}'"
        )
        self.__conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)

    def create_new_db(self):
        try:
            self.__conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with self.__conn.cursor() as curr:
                curr.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.db_name)))
        except psycopg2.Error as e:
            print(f"An error occured trying to create a new db: {e}")
            sys.exit()
        finally:
            print(f"successfully created database: {self.db_name}")
            self.__conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)

    def create_table(self, columns: List[str]):
        pass
        

    def test_db_conn(self):
        # Fetch all the records
        curr = self.__conn.cursor()
        curr.execute('SELECT * FROM public."User" ORDER BY id ASC')
        records = curr.fetchall()
        
        for record in records:
            print(f'{record[0]}, {record[1]}')

        curr.close()
        self.__conn.close()


if __name__ == "__main__":
    # parse command line options
    parser = argparse.ArgumentParser()
        
    parser.add_argument("-c", "--csv_path", help="path to csv file", required=True)
    parser.add_argument("-d", "--db_conn", help="postgress db connection string", required=True)
    parser.add_argument("-n", "--db_name", help="name of database to populate", required=True)

    args = parser.parse_args()

    # load env variables to create new DBConnection 
    load_dotenv()
    db_conn = DBConnection(os.getenv("DB_NAME"), os.getenv("USER"), os.getenv("HOST"), os.getenv("PASSWORD"), os.getenv("PORT"))

    # *Accessing the db connection operations
    # db = PopulateDB(db_conn, args.db_name)
    # db.create_new_db()
    # db.test_db_conn()

    csv_data = ExtractCSV(args.csv_path)
    
    # *NOTE: Access the type of the id field
    # *print(csv_data.data_types["id"])

    # *NOTE: Access the data within row 0, col "id"
    # *print(csv_data.data[0]["id"])
