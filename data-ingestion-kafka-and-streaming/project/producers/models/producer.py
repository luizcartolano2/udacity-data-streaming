"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            'bootstrap.servers': 'PLAINTEXT://0.0.0.0:9092,PLAINTEXT://0.0.0.0:9093,PLAINTEXT://0.0.0.0:9094',
            'schema.registry.url': 'http://0.0.0.0:8081'
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    @staticmethod
    def is_topic_already_created(client, topic_name):
        """
        Method to check if a topic already exists.
        :param client: .
        :param topic_name: .
        :return:
        """
        # list topics from client
        list_of_topics = client.list_topics(timeout=5.0).topics
        return topic_name in list_of_topics

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient(
            {"bootstrap.servers": self.broker_properties['bootstrap.servers']})

        if self.is_topic_already_created(client, self.topic_name):
            logger.info(f"Topic already exists: {self.topic_name}")
            return

        futures = client.create_topics(
            [
                NewTopic(
                    self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic created: {topic}")
            except Exception as exc:
                logger.error(f"Failed to create topic {topic}: {exc}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer is not None:
            logger.info("Flushing producer")
            self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
