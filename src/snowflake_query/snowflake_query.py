import pandas as pd
import snowflake.connector as snow
from cryptography.hazmat.primitives import serialization
import warnings
warnings.filterwarnings("ignore")
from snowflake.connector.pandas_tools import pd_writer

def snow_query(key_path=None, username=None, warehouse=None, account=None, query=None):

    if key_path == None:
        print("Private Key Path is Needed")
        return 0
    elif username == None:
        print('Snowflake Username is Required')
        return 0
    elif warehouse == None:
        print('Warehouse is Required')
        return 0
    elif account == None:
        print('Snowflake Account Name is Required')
        return 0
    elif query == None:
        print('SQL Query is Required')
        return 0


    with open(key_path, "rb") as key:
        p_key= serialization.load_pem_private_key(
            key.read(), password=None

        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    conn = snow.connect(
        user=username,
        private_key=pkb,
        warehouse=warehouse,
        account=account
        )

    df = pd.read_sql(query, con=conn)
    return df


def get_table_metadata(df):
    def map_dtypes(x):
        if (x == 'object') or (x=='category'):
            return 'VARCHAR'
        elif 'date' in x:
            return 'DATETIME'
        elif 'int' in x:
            return 'NUMERIC'
        else:
            print("cannot parse pandas dtype")
    sf_dtypes = [map_dtypes(str(s)) for s in df.dtypes]
    table_metadata = ", ". join([" ".join([y.upper(), x]) for x, y in zip(sf_dtypes, list(df.columns))])
    return table_metadata


def df_to_snowflake_table(db_name, schema_name, table_name, operation, df, conn):
    if operation=='create_replace':
        df.columns = [c.upper() for c in df.columns]
        table_metadata = get_table_metadata(df)
        print(table_metadata)
        conn.cursor().execute(f"CREATE OR REPLACE TABLE {db_name}.{schema_name}.{table_name} ({table_metadata})")
        snow.pandas_tools.write_pandas(conn, df, table_name.upper())
    elif operation=='insert':
        table_rows = str(list(df.itertuples(index=False, name=None))).replace('[','').replace(']','')
        conn.cursor().execute(f"INSERT INTO {table_name} VALUES {table_rows}")

    return 0

def create_conn(key_path=None, username=None, warehouse=None,
               account=None, db_name=None,
               schema_name=None):

    if key_path == None:
        print("Private Key Path is Needed")
        return 0
    elif username == None:
        print('Snowflake Username is Required')
        return 0
    elif warehouse == None:
        print('Warehouse is Required')
        return 0
    elif account == None:
        print('Snowflake Account Name is Required')
        return 0
    elif db_name == None:
        print('DB Name is Required')
        db_name = input("Enter DB Name: ")
    elif schema_name == None:
        print('Schema Name is Required')
        schema_name = input("Enter Schema Name: ")


    with open(key_path, "rb") as key:
        p_key= serialization.load_pem_private_key(
            key.read(), password=None
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    conn = snow.connect(
        user=username,
        private_key=pkb,
        warehouse=warehouse,
        account=account,
        database=db_name,
        schema=schema_name
    )


    return conn


def snow_write(db_name=None, schema_name=None, tb_name=None,
               operation='create_replace',
               df=pd.DataFrame(),
               key_path=None, username=None, warehouse=None,
               account=None):


    conn = create_conn(key_path=key_path, username=username, warehouse=warehouse,
               account=account, db_name=db_name, schema_name=schema_name)


    df_to_snowflake_table(db_name, schema_name, tb_name, operation, df, conn)
    print(f"{db_name}.{schema_name}.{tb_name} is successfully updated")

    return 0

