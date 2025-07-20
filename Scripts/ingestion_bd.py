# Pandas: for csv reading
import pandas as pd
# OS: for file and folder operation
import os
#sqlalchemy; To connect to SQlite
from sqlalchemy import create_engine
# logging: To track progress and errors
import logging
# time : to calculate how long the process takes
import time

# create log folder
if not os.path.exists("logs"):
    os.makedirs("logs")

# Reset logging handlers to avoid duplicate logs
for handler in logging.root.handlers[:]:
    logging.root.removedHandler(handler)

# Setting up logging. Write logs into file and readable format
logging.basicConfig(
    filename = "logs/ingestion_bd.log",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filemode="a" # 'a' = append the file
)

# Create SQLite engine (This create and connects to 'inventory.db')
engine = create_engine('sqlite:///inventory.db')

# This funtion will read large data in chunks and load into database
def ingest_db(file_path, table_name, engine):
    chunksize = 10000 #Load 10000 rows at a time to avoid memory issue
    chunk_num = 1
    first_chunk = True # First one replace the table next ones append
    try: # Read CSV in chunks
        for chunk in pd.read_csv(file_path, encoding = 'latin1',chunksize = chunksize): #for the first chunk replace existing table (if any)
            if first_chunk:
                chunk.to_sql(table_name, con=engine, if_exists='replace', index = False)
                first_chunk = False
            else: # for the est to append to existing table
                chunk.to_sql(table_name, con=engine, if_exists='append', index = False)
            # log after inserting each chunk
            logging.info(f"{table_name} - Intrested chunk {chunk_num}")
            logging.getLogger().handlers[0].flush() #flush loggs immediately
            chunk_num += 1 # immediately write log to file
    except Exception as e: 
        # log any errors during ingestion
        logging.error(f"Errors while ingesting {table_name}: {e}")
        logging.getLogger().handlers[0].flush()

# Load and ingest all csvs
def load_raw_data():
    start = time.time()
    # loop through all csv file in 'data' folders
    for file in os.listdir('data'):
        if file.endswith('.csv') and not file.startswith('._'): # Ignore temp file
            file_path = os.path.join('data',file)
            # skip empty file
            if os.path.getsize(file_path) == 0:
                logging.warning(f" Skipping empty file: {file}")
                continue
                
            logging.info(f"Starting ingestion for {file}")
            ingest_db(file_path, file[:-4], engine) # Use file name without (.csv) as table name
            logging.info(f"Finished ingestion for {file}")
            logging.getLogger().handlers[0].flush()
            
    end = time.time() # end timer
    total_time = (end - start) / 60 # total time in minutes
    logging.info("--------------Ingestion Complete ---------------------")
    logging.info(f"Total time taken: {total_time: .2f} minutes")
    logging.getLogger().handlers[0].flush()

# Run the script
load_raw_data()

# checking tables name
from sqlalchemy import inspect

inspector = inspect(engine)
print(inspector.get_table_names())

# Checking how many row were inserted in each table

from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM purchases"))
    print("Rows in purchases table:", result.scalar())

with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM begin_inventory"))
    print("Rows in begin_inventory table:", result.scalar())

with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM end_inventory"))
    print("Rows in end_inventory table:", result.scalar())

with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM purchase_prices"))
    print("Rows in purchase_prices table:", result.scalar())

with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM sales"))
    print("Rows in sales table:", result.scalar())

with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM vendor_invoice"))
    print("Rows in vendor_invoice table:", result.scalar())