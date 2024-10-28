import argparse

class PopulateDB:
    def __init__(self, csv_path, db_conn, db_name):
        self.csv_path = csv_path
        self.db_conn = db_conn
        self.db_name = db_name

    def inspect(self):
        print(f"csv_path = {self.csv_path}, type={type(self.csv_path)}")
        print(f"db_conn = {self.db_conn}, type={type(self.db_conn)}")
        print(f"db_name = {self.db_name}, type={type(self.db_name)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
        
    parser.add_argument("-c", "--csv_path", help="path to csv file", required=True)
    parser.add_argument("-d", "--db_conn", help="postgress db connection string", required=True)
    parser.add_argument("-n", "--db_name", help="name of database to populate", required=True)

    args = parser.parse_args()
    db = PopulateDB(args.csv_path, args.db_conn, args.db_name)
    db.inspect()
