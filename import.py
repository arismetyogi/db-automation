import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import creds
import datetime as dt


class DataHandler:
    def __init__(self, folder_path, table_name, conn_params, csv_params):
        self.folder_path = folder_path
        self.table_name = table_name
        self.conn_params = conn_params
        self.csv_params = csv_params
        self.conn = psycopg2.connect(**conn_params)  # Unpack the dictionary here

    @staticmethod
    def clean_colname(df):
        # force column names to be lower case, no spaces, no dashes
        df.columns = [x.lower().replace(" ", "_").replace("-", "_").replace(r"/", "_")
                      .replace("\\", "_").replace(".", "_").replace("$", "").replace("%", "") for x in
                      df.columns]
        return df

    def read_csv_files(self):
        data_frames = {}
        for file in os.listdir(self.folder_path):
            if file.endswith(".csv"):
                file_path = os.path.join(self.folder_path, file)
                df = pd.read_csv(file_path, **self.csv_params)  # Unpack the dictionary here
                df = self.clean_colname(df)  # Clean column names
                data_frames[file] = df
        return data_frames

    @staticmethod
    def handle_nat_values(df):
        df.fillna(value=pd.NaT, inplace=True)  # Replace NaN with NaT (Not a Time)
        df.replace({pd.NaT: None}, inplace=True)  # Replace NaT with None (NULL in database)
        return df

    @staticmethod
    def get_column_type(column):
        column_type = column.dtype.name
        if column_type in ['object', 'str']:
            return 'TEXT'
        elif column_type == 'float64':
            return 'FLOAT'
        elif column_type == 'int64':
            return 'NUMERIC'
        elif column_type == 'datetime64[ns]':
            return 'TIMESTAMP'
        else:
            return 'TEXT'

    def insert_into_postgresql(self, df):
        # Handle NaT values in the DataFrame
        df = self.handle_nat_values(df)

        # Create a cursor object
        cursor = self.conn.cursor()

        cols = ",".join([f'"{col}"' for col in df.columns])
        # SQL statement for creating table
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({cols});"

        # Generate the SQL statement for bulk insert
        values = [tuple(x) for x in df.to_numpy()]
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(self.table_name), sql.SQL(cols)
        )

        # Use psycopg2.extras.execute_values for bulk insert
        execute_values(cursor, insert_query.as_string(self.conn), values)

        # Commit the transaction
        self.conn.commit()

    def process_files(self):
        t1 = dt.datetime.now()
        try:
            # Read CSV files from the folder
            data_frames = self.read_csv_files()

            # Insert each DataFrame into the PostgreSQL table
            for file, df in data_frames.items():
                print(f"Inserting data from {file} into {self.table_name}...")
                self.insert_into_postgresql(df)
                print(f"Data from {file} inserted successfully.")
        finally:
            # Close the connection
            self.conn.close()
        t2 = dt.datetime.now()
        print(f"Time elapsed to write db was {t2 - t1}")


if __name__ == '__main__':
    folder_path = '.downloads/stock_card/update'  # Update with your folder path
    table_name = 'stock_card2'  # Define your target table name

    # Database connection parameters
    conn_params = {
            'dbname': creds.db_name,
            'user': creds.db_username,
            'password': creds.db_password,
            'host': 'localhost',
            'port': '5432'
    }

    # read_csv parameters
    csv_params = {
            'usecols': lambda x: x in ['liph_date', 'stock_date', 'transaction_type', 'transaction_number', 'pos_outlet_id', 'item_id', 'type', 'entry_unit', 'out_unit', 'quantity'],
            'delimiter': ';',
            'encoding': 'cp1252',
            'low_memory': False,
            'parse_dates': ['liph_date', 'stock_date'],
            'date_parser': lambda x: pd.to_datetime(x)
    }

    data_handler = DataHandler(folder_path, table_name, conn_params, csv_params)
    data_handler.process_files()
