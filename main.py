import argparse
import os
import sys
import pandas as pd
import sqlalchemy.exc
from dotenv import load_dotenv
from sqlalchemy import create_engine, Engine, text


class ExtractCSV:
    """
    A class for parsing the csv file columns, and converting the DF to a postgres table

    Attributes
        csv_path (str): path to the csv file
        engine (sqlalchemy.Engine): engine object for communicating with the DB
        table (str): name to create the table in the DB
        data (list[Series]): a list of pandas series representing each record in the csv file

    Methods
        csv_to_slq(): converts the dataframe to a postgres table
    """

    def __init__(self, csv_path: str, engine: Engine, table: str):
        self.csv_path = csv_path
        self.engine = engine
        self.table = table
        self.data = []

        try:
            self._df = pd.read_csv(self.csv_path)
            self.columns = self._df.columns
            self.data_types = self._df.dtypes

            for i in range(len(self._df)):
                self.data.append(self._df.iloc[i])

        except FileNotFoundError as e:
            print(f"FileNotFoundError: Check the csv file path for {self.csv_path}\n\t> {e}")
            sys.exit()

    def csv_to_sql(self):
        try:
            self._df.to_sql(self.table, self.engine)
        except sqlalchemy.exc.OperationalError as e:
            print(f"\nsqlalchemy.exc.OperationalError: Error communicating with the database:\n\t> {e}")
            sys.exit()
        except ValueError as e:
            print(f"\nValueError: Most likely table name already exists... If so delete it or use a new name:\n\t> {e}")
            sys.exit()


class DBConnection:
    """
    A data class used to represent a database connection.

    Attributes
        _url (str): configured DB connection string
    Methods
        connect (): creates a postgres db engine from the connection string
    """

    def __init__(self, db_name: str, user: str, host: str, password: str, port: str):
        self._url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

    def create_engine(self) -> Engine:
        return create_engine(self._url, echo=True)


if __name__ == "__main__":
    # parse command line options & arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--csv_path", help="path to csv file", required=True)
    parser.add_argument(
        "-n",
        "--db_name",
        help="name of database to store data.\nNOTE: Make sure the database name exist in the DB",
        required=True,
    )
    parser.add_argument(
        "-t", "--table",
        help="name of the table to create and populate.\nNOTE: Make surer the table does NOT exist in the DB",
        required=True
    )

    args = parser.parse_args()

    # load env variables to create new DBConnection
    load_dotenv('.env')

    db_conn = DBConnection(
        args.db_name,
        os.getenv("USER"),
        os.getenv("HOST"),
        os.getenv("PASSWORD"),
        os.getenv("PORT"),
    )

    csv_data = ExtractCSV(args.csv_path, db_conn.create_engine(), args.table)
    csv_data.csv_to_sql()
