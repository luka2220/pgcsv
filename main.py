import argparse
import os
from typing import Optional
import psycopg2 
from dotenv import load_dotenv

class DBConnection:
    """
    A class used to represent a database connection.
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
            f"dbname='{db_conn.db_name}' user='{db_conn.user}' host='{db_conn.host}' password='{db_conn.password}' port='{db_conn.port}'"
        )
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

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

    db = PopulateDB(args.csv_path, db_conn, args.db_name)
    # db.inspect()
    db.test_db_conn()
