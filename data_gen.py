from multiprocessing import Process, Queue
from sqlalchemy import create_engine
from api_caller import APICaller
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import datetime
import random
import time
import os


def load_data():
    load_dotenv()
    engine = create_engine(
        f'postgresql://{os.getenv("POSTGRES_USER")}:'
        f'{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:'
        f'{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}'
    )

    # Connect to the engine
    conn = engine.connect()

    # Query the database
    pizza_df = pd.read_sql("SELECT * FROM pizza", conn)
    orders_per_hour_weekday = pd.read_sql("SELECT * FROM orders_per_hour", conn)
    
    return pizza_df, orders_per_hour_weekday

def start_api_caller():
    pizza_df, orders_per_hour_weekday = load_data()
    api_caller = APICaller(pizza_df)

    total_processes = 16
    
    previous_hour = int(datetime.datetime.now().strftime("%H")) - 1
    
    while True:
        current_hour = int(datetime.datetime.now().strftime("%H"))
        if current_hour != previous_hour and current_hour > 9 and current_hour <= 23:
            previous_hour = current_hour
            
            # Your target number
            target_number = orders_per_hour_weekday[
                (orders_per_hour_weekday['order_weekday'] == datetime.datetime.now().strftime("%A"))
                &
                (orders_per_hour_weekday['order_hour'] == datetime.datetime.now().hour)
            ]['order_count'].max()

            target_number = target_number if not np.isnan(target_number) else random.randint(0, 300)

            # Standard deviation (smaller values mean the number is more likely to be close to the target)
            std_dev = int(target_number * .3)

            # Generate a random number close to the target number
            random_number = np.random.normal(loc=target_number, scale=std_dev)

            # If you need the number to be an integer, you can round it
            random_number_rounded = int(round(random_number))

            # Generate a list of random timestamps
            timestamps = [random.randint(0, 3599) for _ in range(random_number_rounded)]

            # Sort the timestamps in ascending order
            timestamps.sort()
            
            q = Queue()
            for t in timestamps:
                q.put(t)
            
            processes = []
            for process_id in range(1, total_processes + 1):
                p = Process(target=api_caller.gen_order, args=(q, process_id))
                p.start()
                processes.append(p)
                    
            for p in processes:
                p.join()
            
        else:
            wait_time = 3600 - (int(datetime.datetime.now().strftime("%M")) * 60 + int(datetime.datetime.now().strftime("%S")))
            print(f"[+] Generator sleeping for {wait_time} seconds to start a new hour")
            time.sleep(wait_time)
            

if __name__ == '__main__':
    start_api_caller()