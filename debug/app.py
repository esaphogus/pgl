import os
from time import sleep
import json
from pytz import timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests, sys
import time
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from socket import timeout
from datetime import datetime, timedelta
import random

TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')

def main():
    # root_url = "https://finance.yahoo.com/quote/CL=F?p=CL=F&.tsrc=fin-srch"
    # req = requests.Session()
    # agent = {
    #     "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, Like Gecko) Chrome/78.0.3904.87 Safari/537.36"
    # }
    # while True:
    #     try:
    #         req_doc = req.get(root_url, headers=agent, timeout=30.0).text
    #         price = get_price(req_doc)
    #         date = datetime.datetime.now().replace(tzinfo=timezone('UTC'))
    #         transaction = {
    #                 "timestamp" : str(date.strftime("%Y-%m-%d %H:%M:%S")),
    #                 "price" : str(price)
    #                 }

    #         producer.send(TRANSACTIONS_TOPIC, value=transaction)
    #         # print(transaction)  # DEBUG
    #         time.sleep(60)
    #     except timeout:
    #         print("timeout, retry after 60s")
    #         time.sleep(60)
    #         continue
    #     except KafkaError:
    #         print("broker, retry after 60s")
    #         time.sleep(60)
    #         continue
    #     except:
    #         print(sys.exc_info())
    #         time.sleep(60)
    #         continue

    start_date = datetime(2020, 8, 5, 0, 0)
    end_date = datetime(2021, 8, 5, 0, 0)

    for single_date in daterange(start_date, end_date):
        date = single_date.replace(tzinfo=timezone('UTC'))
        transaction = {
                "timestamp" : str(date.strftime("%Y-%m-%d %H:%M:%S")),
                "price" : str(round(random.uniform(60.10,70.11), 2))
                }
        print(transaction)
        producer.send(TRANSACTIONS_TOPIC, value=transaction)

def daterange(start_date, end_date):
    delta = timedelta(hours=1)
    while start_date < end_date:
        yield start_date
        start_date += delta

def get_price(req_doc):
    soup = BeautifulSoup(req_doc, "html.parser")
    price = soup.find("span", {"class":"Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)"}).text

    return float(price)



if __name__ == '__main__':
    print("starting")
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda value: json.dumps(value).encode(),
    )

    main()
