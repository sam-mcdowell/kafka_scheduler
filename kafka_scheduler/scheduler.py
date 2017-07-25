from kafka_scheduler.queues import InternalQueue, InputQueue
from kafka_scheduler.events import ScheduledEvent, ScheduledEventReference

from kafka.client import KafkaClient
from kafka.common import ConsumerTimeout
from kafka.consumer import KafkaConsumer
from kafka.producer import SimpleProducer

from math import log, floor
import json
from logging import ERROR, basicConfig, getLogger
import os
from time import time

SCHEDULER_QUEUE_FORMAT = "scheduler_{}s"
CONSUMER_GROUP = "schedulers"
LOG_FILENAME = 'kafka_scheduler.log'


class Scheduler():

    def __init__(self,
                 kafka_hosts,
                 input_topic="scheduled_events",
                 number_of_queues=15):
        self.kafka_hosts = kafka_hosts
        self.input_topic = input_topic
        self.number_of_queues = number_of_queues
        self.queues = []
        self.configure_internal_queues()
        self.configure_input_queue()
        self.start_workers()

    def configure_internal_queues(self):
        """
        configures the internal queues used hold references to events in the input queue
        """
        for i in range(self.number_of_queues):
            client = KafkaClient(hosts=self.kafka_hosts)
            queue_name = SCHEDULER_QUEUE_FORMAT.format(2**i)
            client.ensure_topic_exists(queue_name)
            indexed_consumer = IndexedConsumer(self.input_topic, self.kafka_hosts)
            queue_consumer = KafkaConsumer(
                queue_name,
                bootstrap_servers=self.kafka_hosts,
                group_id=queue_name,
                consumer_timeout_ms=2000,
                auto_commit_enable=False,
            )
            queue_producer = SimpleProducer(client)
            queue_duration = 2**i
            self.queues.append(
                InternalQueue(
                    queue_consumer,
                    indexed_consumer,
                    queue_producer,
                    self.number_of_queues,
                    queue_duration,
                )
            )

    def configure_input_queue(self):
        """
        configures the input queue that other services can use to schedule an event to be delivered
        """
        client = KafkaClient(hosts=self.kafka_hosts)
        client.ensure_topic_exists(self.input_topic)
        indexed_consumer = IndexedConsumer(self.input_topic, self.kafka_hosts)
        queue_consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.kafka_hosts,
            group_id=CONSUMER_GROUP
        )
        queue_producer = SimpleProducer(KafkaClient(hosts=self.kafka_hosts))
        self.queues.append(
            InputQueue(
                queue_consumer,
                indexed_consumer,
                queue_producer,
                self.number_of_queues
            )
        )

    def start_workers(self):
        for queue in self.queues:
            queue.start()

    def stop_workers(self):
        for queue in self.queues:
            queue.terminate()


class IndexedConsumer():
    """
    A simple consumer to retrieve messages from the input queue when it is time to send them
    """
    def __init__(self, input_topic, hosts):
        self.input_topic = input_topic
        self.consumer = KafkaConsumer(bootstrap_servers=hosts)

    def retrieve_event(self, event_reference):
        self.consumer.set_topic_partitions(
            (
                self.input_topic,
                event_reference.partition,
                event_reference.offset
            )
        )
        message = self.consumer.next()
        event = ScheduledEvent.from_dict(json.loads(message.value))
        return event
