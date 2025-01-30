import os
import configparser
from IPython import get_ipython
import pandas as pd
import json

config = configparser.ConfigParser()
config.read("config.ini")



def get_column_names(schemas,ds_name,sorting_key="column_position"):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key= lambda col:col[sorting_key] )

    return [col["column_name"] for col in columns]

def read_csv(file, schemas, chunksize=10_000):
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]
    columns = get_column_names(schemas, ds_name)
    df_reader = pd.read_csv(file, names=columns, chunksize=chunksize)
def to_sql(df, db_conn_url, ds_name):
    df.to_sql(
        ds_name,
        db_conn_url,
        if_exists="append",
        index=False
    )
def db_loader(src_base_dir, db_conn_url, ds_name):
    schemas = json.load(open(f'{src_base_dir}\\schemas.json'))
    files = glob.glob(f"{src_base_dir}\\{ds_name}\\part-*")
    if len(files) == 0:
        raise NameError(f"No files found for {ds_name}")
    
    for file in files:
        df_reader = read_csv(file, schemas)
        for i, df in enumerate(df_reader):
            print(f"Populating chunk {i} of {ds_name}")
            to_sql(df,db_conn_url, ds_name)

schemas = json.load(open("data\\retail_db\\schemas.json"))
columns = get_column_names(schemas=schemas,ds_name='orders')

df = pd.read_csv(
    "data\\retail_db\\orders\\part-00000",
    names=columns
)

db_config = config["database"]
database = db_config.get("database")
username = db_config.get("username")
password = db_config.get("password")
server = db_config.get("server")
portnumber = db_config.getint("portnumber")  
db_name = db_config.get("db_name")

conn_url = f"{database}://{username}:{password}@localhost:{portnumber}/{db_name}"


# os.environ.update({'DB_HOST': 'localhost'})
# ipython = get_ipython()

# ipython.run_line_magic("env","DB_HOST=localhost")
# ipython.run_line_magic("load_ext","sql")
# ipython.run_line_magic("config","SqlMagic.style = \'_DEPRECATED_DEFAULT\'")
# ipython.run_line_magic("env",f"DATABASE_URL={conn_url}")
# print(ipython.run_line_magic("sql","SELECT * FROM orders LIMIT 10;"))
# ipython.run_line_magic("sql","TRUNCATE TABLE orders")
# print(os.environ.get("DB_HOST"))
# query = "SELECT * FROM orders Limit 10;"
# print(pd.read_sql(query,conn_url))

df.to_sql(
    "orders",
    conn_url,
    if_exists="replace",
    index=False
)

print(pd.read_sql('orders', conn_url))