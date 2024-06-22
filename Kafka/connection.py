import os
import ssl
from dotenv import find_dotenv, load_dotenv
from aiokafka import AIOKafkaConsumer


# Load dotenv
load_dotenv(find_dotenv(".env"))


# KafkaConnection
class KafkaConnection:
    def __init__(self):
        self.KAFKA_MAIL_TOPIC = str(os.getenv("KAFKA_MAIL_TOPIC"))
        self.KAFKA_bootstrap_servers = str(os.getenv("KAFKA_bootstrap_servers"))
        self.KAFKA_sasl_mechanism = str(os.getenv("KAFKA_sasl_mechanism"))
        self.KAFKA_security_protocol = str(os.getenv("KAFKA_security_protocol"))
        self.KAFKA_sasl_plain_username = str(os.getenv("KAFKA_sasl_plain_username"))
        self.KAFKA_sasl_plain_password = str(os.getenv("KAFKA_sasl_plain_password"))
        self.consumer = None

    async def connect(self):
        self.consumer = AIOKafkaConsumer(
            self.KAFKA_MAIL_TOPIC,
            bootstrap_servers=self.KAFKA_bootstrap_servers,
            sasl_mechanism=self.KAFKA_sasl_mechanism,
            security_protocol=self.KAFKA_security_protocol,
            sasl_plain_username=self.KAFKA_sasl_plain_username,
            sasl_plain_password=self.KAFKA_sasl_plain_password,
            ssl_context=ssl.create_default_context(),
        )
        await self.consumer.start()
        print("Kafka Connected Connected!")
        return self.consumer

    async def disconnect(self):
        if self.consumer:
            await self.consumer.stop()
            print("Kafka Disconnected ...")


kafka_connection = KafkaConnection()
