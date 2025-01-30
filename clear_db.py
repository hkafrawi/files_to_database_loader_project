import os
from IPython import get_ipython
from dotenv import load_dotenv

load_dotenv()

ipython = get_ipython()

database = os.environ.get("DB_TYPE")
username = os.environ.get("DB_USER")
password = os.environ.get("DB_PASSWORD")
host = os.environ.get("DB_HOST")
portnumber = os.environ.get("DB_PORT")
db_name = os.environ.get("DB_NAME")

conn_url = f"{database}://{username}:{password}@{host}:{portnumber}/{db_name}"

ipython.run_line_magic("load_ext","sql")
ipython.run_line_magic("config","SqlMagic.style = \'_DEPRECATED_DEFAULT\'")
ipython.run_line_magic("env",f"DATABASE_URL={conn_url}")
ipython.run_line_magic("sql","TRUNCATE TABLE orders")

