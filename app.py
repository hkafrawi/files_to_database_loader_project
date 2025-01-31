import os
import configparser
from IPython import get_ipython
import pandas as pd
import json
import re
import sys
from dotenv import load_dotenv
import glob
import multiprocessing

load_dotenv()

def get_column_names(schemas,ds_name,sorting_key="column_position"):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key= lambda col:col[sorting_key] )

    return [col["column_name"] for col in columns]

def read_csv(file, schemas, chunksize=10_000):
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]
    columns = get_column_names(schemas, ds_name)
    df_reader = pd.read_csv(file, names=columns, chunksize=chunksize)

    return df_reader

def to_sql(df, db_conn_url, ds_name):
    if ds_name == "products":
        df["product_description"] = df["product_description"].fillna("No description available")
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

def process_dataset(args):
    src_base_dir = args[0]
    conn_url = args[1]
    ds_name = args[2]

    try:
        print(f"Processing {ds_name}")
        db_loader(src_base_dir, conn_url, ds_name)
    except NameError as e:
        print(e)
        pass
    except Exception as e:
        print(e)
        pass
    
def process_files(ds_names=None):
    database = os.environ.get("DB_TYPE")
    username = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    host = os.environ.get("DB_HOST")
    portnumber = os.environ.get("DB_PORT")
    db_name = os.environ.get("DB_NAME")

    conn_url = f"{database}://{username}:{password}@{host}:{portnumber}/{db_name}"

    src_base_dir = os.environ.get("SRC_BASE_DIR")

    schemas = json.load(open(f"{src_base_dir}\\schemas.json"))

    if not ds_names:
        ds_names = schemas.keys()

    pprocesses = len(ds_names) if len(ds_names) < 8 else 8
    pool = multiprocessing.Pool(pprocesses)
    pd_args = []
    for ds_name in ds_names:
        pd_args.append((src_base_dir, conn_url, ds_name))
    pool.map(process_dataset, pd_args)
        
if __name__ == "__main__":
    if len(sys.argv) == 2:
        ds_names = json.loads(sys.argv[1])
        process_files(ds_names)
    else:
        process_files()


