"""Example Kafka consumer."""

import json
import os

from kafka import KafkaConsumer, KafkaProducer
import pyodbc

TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')

def connect():
    connStr = pyodbc.connect('''DRIVER={FreeTDS}; \
                            SERVER=host.docker.internal; \
                            PORT=57155; \
                            UID=erm; \
                            PWD=Jakarta1!; \
                            DATABASE=ERM; 
                            Trusted_Connection=yes''')
    return connStr

def insert_row(conn, timestamp, midrate, curr):
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO dbo.midrate(timestamp,midrate,curr) values (?, ?,?)""", 
                    timestamp, 
                    midrate,
                    curr)

if __name__ == '__main__':
    conn = connect()

    consumer = KafkaConsumer(
        TRANSACTIONS_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
    )

    for message in consumer:
        transaction: dict = message.value
        print(transaction['Tanggal'], transaction['Middle Rate'], transaction['Mata Uang'])  # DEBUG
        insert_row(conn, transaction['Tanggal'], transaction['Middle Rate'], transaction['Mata Uang'])
