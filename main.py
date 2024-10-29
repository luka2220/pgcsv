import argparse
import os
import csv
import sys
from typing import Optional, List
import psycopg2 
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

class ExtractCSV:
    """
    A class for opening and parsing the csv file
    """
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.columns: List[str] = []
        self.types: List[str] = []
        self.data = []

        try:
            with open(self.csv_path, newline='') as self.csv_file:
                reader = csv.reader(self.csv_file, delimiter=',')
                r_num = 0

                for row in reader:
                    if r_num == 0:
                        self.columns = row
                        r_num += 1
                        continue
                    elif r_num == 1:
                        for v in row:
                            self.types.append(type(v))
                            r_num += 1
                    self.data.append(row)

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
    def __init__(self, csv_path: str, db_conn: DBConnection, db_name: str):
        self.csv_path: str = csv_path
        self.db_conn = db_conn
        self.db_name: str = db_name
        # open db connection
        self.conn = psycopg2.connect(
            f"dbname='postgres' user='{db_conn.user}' host='{db_conn.host}' password='{db_conn.password}' port='{db_conn.port}'"
        )
        self.conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)

    def create_new_db(self):
        try:
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with self.conn.cursor() as curr:
                curr.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.db_name)))
        except psycopg2.Error as e:
            print(f"An error occured trying to create a new db: {e}")
        finally:
            print(f"successfully created database: {self.db_name}")
            self.conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)

    def create_table(self, columns: List[str]):
        pass
        

    def test_db_conn(self):
        # Fetch all the records
        curr = self.conn.cursor()
        curr.execute('SELECT * FROM public."User" ORDER BY id ASC')
        records = curr.fetchall()
        
        for record in records:
            print(f'{record[0]}, {record[1]}')

        curr.close()
        self.conn.close()


    def inspect(self):
        print(f"csv_path = {self.csv_path}, type={type(self.csv_path)}")
        print(f"db_conn = {self.db_conn}, type={type(self.db_conn)}")
        print(f"db_name = {self.db_name}, type={type(self.db_name)}")


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

    # db = PopulateDB(args.csv_path, db_conn, args.db_name)
    # db.create_new_db()
    # db.test_db_conn()

    csv_data = ExtractCSV(args.csv_path)
    for i, col in enumerate(csv_data.columns):
        print(f"{col}: {csv_data.types[i]}")
