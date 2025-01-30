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