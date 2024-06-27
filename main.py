from threading import Thread
from Kafka.connection import kafka_connection
from Database.connection import databaseConnection
import asyncio
import json
from keep_alive import keep_alive

from helper.confirm_email import confirm_email
from helper.close_account import close_account
from helper.forgot_password import forgot_password
from helper.order_receipt import order_receipt


async def start_processing(consumer):
    print("Started cosuming messages ...")

    while True:
        async for msg in consumer:
            consumed_message = json.loads(msg.value.decode())
            if consumed_message["operation"] == "keep-alive":
                print("keep-alive!")
                break

            data = consumed_message["data"]
            if consumed_message["operation"] == "confirm_email":
                confirm_email(data)
            elif consumed_message["operation"] == "close_account":
                close_account(data)
            elif consumed_message["operation"] == "order_receipt":
                order_receipt(data)
            elif consumed_message["operation"] == "forgot_password":
                forgot_password(data)


def connect_mongodb():
    try:
        databaseConnection().connect()
    except Exception as e:
        print("Error", str(e))
        databaseConnection().disconnect()


async def consume_from_kafka():
    try:
        # Connect with Kafka
        consumer = await kafka_connection.connect()
        await start_processing(consumer)

    except Exception as e:
        # Disconnect with Kafka
        await kafka_connection.disconnect()
        print("Error :", str(e))


if __name__ == "__main__":
    keep_alive()
    print("Running others ...")
    # connect_mongodb()
    t = Thread(target=asyncio.run(consume_from_kafka()))
    t.start()
