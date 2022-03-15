import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
import warnings
warnings.filterwarnings("ignore")

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

    conn = snowflake.connector.connect(
        user=username,
        private_key=pkb,
        warehouse=warehouse,
        account=account
        )

    df = pd.read_sql(query, con=conn)
    return df
