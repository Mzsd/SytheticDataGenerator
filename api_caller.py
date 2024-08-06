from bs4 import BeautifulSoup as bs4
from fake_headers import Headers
from datetime import datetime
from faker import Faker
import requests
import random
import urllib
import time
import json


class Person:
    
    def __init__(self, postcodes_df) -> None:
        fake = Faker("en_GB")
        self.name = fake.name()
        self.address = postcodes_df.iloc[random.randint(0, len(postcodes_df))]['postcode']
        self.email = fake.email()
        self.phone = fake.phone_number().replace(' ', '').replace('(', '').replace(')', '').replace('+44', '')
        self.phone = self.phone if len(self.phone) == 11 else self.phone + str(random.randint(0, 9))
        
    def get_json(self):
        return {
            "name": self.name,
            "address": self.address,
            "email": self.email,
            "phone": self.phone
        }

class APICaller():
    
    def __init__(self, pizza_df, postcodes_df) -> None:
        self.pizza_df = pizza_df
        self.postcodes_df = postcodes_df
        self.numbers = list(range(0, len(self.pizza_df)))
    
    def convert_user_info(self, input_data, token):
        encoded_data = {k: urllib.parse.quote_plus(v) for k, v in input_data.items()}
        encoded_data['token'] = token
        return "&".join([f"{k}={v}" for k, v in encoded_data.items()])

    
    def convert_pizza_orders(self, input_data):
        output_list = []
        for item in input_data:
            pizza_details = json.loads(item['pizza'])
            pizza_name = pizza_details['pizza_name'].lower().replace(" ", "-")
            pizza_size = pizza_details['pizza_size'].lower()
            pizza_size = "medium" if pizza_size == "m" else "large" if pizza_size == "l" else "small"
            quantity = item['quantity']
            output_list.append(f"pizza_size={pizza_size}&pizza_type={pizza_name}&quantity={quantity}")
        return "&".join(output_list)

    ### Functions to generate random pizza data
    #
    #
    def select_random_pizza(self, penalty_factor=0.1):
        weights = (1 / self.pizza_df['unit_price']).to_list()
        
        max_weight = max(weights)

        penalized_weights = [weight if weight == max_weight else weight * penalty_factor for weight in weights]
        return random.choices(self.numbers, penalized_weights)[0]
    
    def get_random_number_pizzas(self, first_prob=0.9):
        first_prob = 0.9
        pizza_weights = [first_prob] + [
            (1 - first_prob) / (i - 1 ) ** 2
            for i in self.numbers[2:]
        ]

        return random.choices(self.numbers[1:], pizza_weights)[0]
    
    def get_random_qty_pizzas(self, first_prob=0.8):
        qty = list(range(1, 11))
        pizza_weights = [first_prob] + [
            (1 - first_prob) / (i - 1 ) ** 2
            for i in qty[1:]
        ]

        return random.choices(qty, pizza_weights)[0]
    
    def gen_order(self, queue, process_id=0):
        time.sleep(process_id)
        order_num = 0
        while not queue.empty():
            order_num += 1
            timestamp = queue.get()
            time_now = datetime.now()
            wait_time = timestamp - (int(time_now.strftime("%M")) * 60 + int(time_now.strftime("%S")))
            timestamp_datetime = datetime.now().replace(minute=timestamp // 60, second=timestamp % 60)
                        
            if wait_time > 0:
                print(f"\n[+] Process {process_id} waiting for {wait_time} seconds to generate order")
                time.sleep(wait_time)

            print(f"\n[+] Process {process_id} generating order at {datetime.now().strftime('%H:%M:%S')} and original timestamp {timestamp_datetime.strftime('%H:%M:%S')}")
            number_pizzas = self.get_random_number_pizzas()
            
            order = [
                {
                    "pizza": self.pizza_df.iloc[self.select_random_pizza()][['pizza_name', 'pizza_size']].to_json(), 
                    "quantity": self.get_random_qty_pizzas()
                }
                for _ in range(number_pizzas)
            ]
                        
            order_str = self.convert_pizza_orders(order)
            
            print(f"\n[+] Process {process_id} generated order: {order_str}")

            headers = Headers(
                    # accept="application/json",
                    browser="chrome",  # Generate only Chrome UA
                    headers=True  # generate misc headers
                ).generate()
            headers['accept'] = 'application/json'
            
            # Call the first order API
            response = requests.post(
                'http://127.0.0.1:8000/first_order', 
                headers=headers, 
                json=order_str
            )
            
            if response.status_code == 200:
                soup = bs4(response.text, 'html.parser')
                token = soup.find_all('input')[-1].attrs['value']
                
                checkout_str = self.convert_user_info(Person(self.postcodes_df).get_json(), token)
                time.sleep(2)
                response = requests.post(
                    'http://127.0.0.1:8000/checkout', 
                    headers=headers,
                    json=checkout_str
                )
                
                if response.status_code == 200:
                    print(f"\n[+] Process {process_id} order completed successfully: {response.json()}")
                else:
                    print(f"\n[+] Process {process_id} order failed: {response.json()['message']}")
                
        print(f"\n\n[+] Total orders generated by process {process_id}: {order_num}\n")