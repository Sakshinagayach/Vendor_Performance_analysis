#data frame banake daset mein dalna

import pandas as pd
import os
from sqlalchemy import create_engine 
import logging 
import time

logging.basicConfig(
    filename = "/Users/Sakshi.Nagayach/Downloads/LOGS/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode ="a"
)

engine = create_engine ('sqlite:///inventory.db')

def ingest_db(df,table_name,engine):
    '''this fuction will ingest the dataframe into database table '''
    df.to_sql(table_name, con=engine, if_exists="replace", index = False)

def load_row_data():
  '''this function will load the data CSV as dataframe and ingest into db'''
  start = time.time()
  for file in os.listdir("/Users/Sakshi.Nagayach/Downloads/VDA"):
    if ".csv" in file:
        df = pd.read_csv("/Users/Sakshi.Nagayach/Downloads/VDA/"+file)
        logging.info(f"ingesting {file} in db")
        ingest_db(df,file[:-4],engine)   
    end = time.time()
    total_time = (end - start)/60
    logging.info("===============ingesting complete in db=======================")
    logging.info(f"\ntotal time taken {total_time} minites")

if __name__ == '__main__':
    load_row_data()