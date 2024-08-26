from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime

import json
import time
import random

fake = Faker()

def generate_transactions():
    user = fake.simple_profile()

    return {
        "transaction_id": fake.uuid4(),
        "product_id": random.choice(["product1, product2, product3", "product4, product5, product6", "product7, product8, product9"]),
        "product_name":random.choice(["Shirt", "Pants", "Socks", "Jacket", "Shoes","T-shirt","Belt","Bag","Watch"]),
        "product_category":random.choice(["Fashion","Accessories","Home"]),
        "product_price": round(random.uniform(10, 1000), 2),
        "product_quantity": random.randint(1, 10),
        "product_brand": random.choice(["Adidas","Nike","Reebok","Puma","Vans"]),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "customer_id": user["username"],
        "transaction_date": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        "payment_method": random.choice(["Credit Card", "PayPal", "Bank Transfer"])
    }

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print("User record {} successfully produced to {} [{}] at offset {}".format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def main():
    topic = "Transactions"
    producer = SerializingProducer({"bootstrap.servers": "localhost:9092"})

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 120:
        try:
            transaction = generate_transactions()
            transaction['total_amount'] = transaction['product_price'] * transaction['product_quantity']

            print(transaction)

            producer.produce(topic, key=transaction['transaction_id'], value=json.dumps(transaction),on_delivery=delivery_report)

            producer.poll(0)

            time.sleep(5)

        except BufferError:
            print("Buffer full. Retrying...")
            time.sleep(1)

        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()