"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer


BROKER_URL = 'PLAINTEXT://localhost:9092'
SCHEMA_REGISTRY = 'http://localhost:8081'

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([
        # 'cta_turnstiles',
        # 'cta_station'
    ])

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
            'bootstrap.servers': BROKER_URL,
            'schema.registry.url': SCHEMA_REGISTRY,
            "group.id": topic_name,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({'bootstrap.servers': BROKER_URL})
        print(self.topic_name)
        topic = NewTopic(self.topic_name,
                         num_partitions=1,
                         replication_factor=1)

        fs = client.create_topics([topic])

        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f'Topic {topic} created')
            except Exception as e:
                logger.error(f'Failed to create topic {topic}: {e}')

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        try:
            self.producer.flush()
            logger.info('Producer closed successfully!')
        except Exception as e:
            logger.error(F'Producer close incomplete: {e}')

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
