import pandas as pd
import os
import psycopg2
from datetime import datetime as dtime
import creds


def clean_colname(dataframe):
    # force column names to be lower case, no spaces, no dashes
    dataframe.columns = [x.lower().replace(" ", "_").replace("-", "_").replace(r"/", "_")
                         .replace("\\", "_").replace(".", "_").replace("$", "").replace("%", "") for x in
                         dataframe.columns]
    return dataframe.columns


def _get_column_type(column):
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
        return 'VARCHAR(35)'


class CSVtoPostgres:
    def __init__(self, folder_path, db_params, table_name, columns, chunk_size, **csv_params):
        self.folder_path = folder_path
        self.db_params = db_params
        self.table_name = table_name
        self.columns = columns
        self.csv_params = csv_params
        self.chunk_size = chunk_size
        self.dataframes = []

    def __enter__(self):
        file_list = []
        for dirpath, _, filenames in os.walk(self.folder_path):
            for filename in filenames:
                if filename.endswith('.csv'):
                    file_path = os.path.join(dirpath, filename)
                    file_list.append(file_path)

        chunks = [file_list[i:i + self.chunk_size] for i in range(0, len(file_list), self.chunk_size)]

        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()

        for chunk in chunks:
            for file_path in chunk:
                df = pd.read_csv(file_path, usecols=self.columns, **self.csv_params)  # usecols=self.columns,
                self.dataframes.append(df)
                print(f'\n{file_path} loaded')

            merged_df = pd.concat(self.dataframes, ignore_index=True)

            # Replace NaN values with NULL, so they can be inserted into the database
            merged_df = merged_df.where(pd.notnull(merged_df), None)
            clean_colname(merged_df)

            # Build the SQL query for creating the table
            cols = merged_df.columns
            types = [_get_column_type(merged_df[col]) for col in merged_df.columns]
            col_types = list(zip(cols, types))
            sql_cols = ', '.join([f'{col[0]} {col[1]}' for col in col_types])
            create_table_query = f'CREATE TABLE IF NOT EXISTS {self.table_name} ({sql_cols});'
            cur.execute(create_table_query)

            # Build the SQL query for inserting the data into the table
            cols = ','.join(cols.values)
            values = ", ".join(["%s" for i in range(len(merged_df.columns))])
            insert_query = f'INSERT INTO {self.table_name} ({cols}) VALUES ({values})'  # ON CONFLICT (no_transaksi, kode_sap_produk) DO NOTHING RETURNING *
            # print(f'insert query: {insert_query}')
            data = [tuple(row) for row in merged_df.to_numpy()]
            # print(data[0])
            cur.executemany(insert_query, data)
            print(f'\nData Inserted')

            self.dataframes = []

            conn.commit()
        cur.close()
        conn.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        for df in self.dataframes:
            del df

table_name = 'sales'
columns = [
    'KODE OUTLET', 'KODE OBAT', 'NOMOR TRANSAKSI', 'LIPH', 'DISCOUNT', 'QTY OBAT BEBAS', 'OBAT BEBAS', 'QTY RESEP UPDS',
    'RESEP UPDS', 'QTY RISK SELLING', 'RISK SELLING', 'QTY RISK SELLING CREDIT', 'RISK SELLING CREDIT',
    'QTY RESEP TUNAI', 'RESEP TUNAI', 'QTY RESEP KREDIT', 'RESEP KREDIT', 'QTY OPTIK', 'OPTIK', 'QTY TOTAL',
    'TOTAL OMSET'
]
dateparse = lambda x: pd.to_datetime(x, dayfirst=True, format='mixed')


def main():
    with CSVtoPostgres(
            './.downloads/sales/new'
            , db_params={
                'host': 'localhost'
                , 'port': 5432
                , 'database': creds.db_name
                , 'user': creds.db_username
                , 'password': creds.db_password
            }
            # , skipfooter=1 #must disable low_memory
            , table_name=table_name
            , columns=columns
            , chunk_size=1
            , delimiter=','
            , encoding='cp1252'
            , low_memory=False
            , parse_dates=['LIPH']
            , date_parser=dateparse
    ):
        pass


if __name__ == '__main__':
    main()
