from confluent_kafka import Consumer
from utils.config_reader import ConfigReader
from utils.logger import logger, logger_agg
import time

cfg = ConfigReader()
topic = cfg["consume_topic_phase3"]

consumer_props = cfg["consumer_props"].copy()
consumer_props['group.id'] = consumer_props.get('group.id')
consumer_props['auto.offset.reset'] = 'earliest'

from confluent_kafka import TopicPartition

def _on_assign(consumer, partitions):
    logger_agg.info(f"[kafka_consumer3] ASSIGNED partitions={partitions}")
    try:
        committed = consumer.committed(partitions, timeout=10)
        logger_agg.info(f"[kafka_consumer3] COMMITTED={committed}")
    except Exception as e:
        logger_agg.warning(f"[kafka_consumer3] committed() failed: {e}")

def _on_revoke(consumer, partitions):
    logger_agg.warning(f"[kafka_consumer3] REVOKED partitions={partitions}")


def kafka_consumer3(group_id_override=None):
    try:
        logger_agg.info("STEP 1 - Kafka consumer-kafka_consumer phase 3 initialized.")

        props = consumer_props.copy()
        props["enable.auto.commit"] = False

        if group_id_override:
            props["group.id"] = group_id_override

        consumer = Consumer(props)
        logger_agg.info("STEP 2 - Kafka consumer-kafka_consumer phase 3 initialized.")

        consumer.subscribe([topic], on_assign=_on_assign, on_revoke=_on_revoke)
        logger_agg.info("STEP 3 - Kafka consumer-kafka_consumer phase 3 subscribed to topic: " + str(cfg["consume_topic_phase3"]))
        
        return consumer

    except Exception as e:
        logger_agg.error("An error occurred during Kafka consumer-kafka_consumer initialization or subscription: " + str(e))
        logger_agg.debug("Error details:", exc_info=True)
        
        return None