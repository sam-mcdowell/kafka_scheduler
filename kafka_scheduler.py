from kafka_python import KafkaClient
from pykafka.exceptions import SocketDisconnectedError
from pykafka.protocol import PartitionFetchRequest

from math import log, floor
import json

from threading import Thread
from time import sleep, time
import re

from datetime import datetime

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
QUEUE_DURATION_FORMAT="%Ss"
SCHEDULER_QUEUE_FORMAT="scheduler_{}s"
CONSUMER_GROUP="schedulers"


class KafkaScheduler():

    def __init__(self,
                 kafka_hosts,
                 zookeeper_hosts,
                 input_topic="scheduled_events",
                 number_of_queues=15):

        self.kafka_hosts = kafka_hosts
        self.zookeeper_hosts = zookeeper_hosts
        self.input_topic = input_topic
        self.number_of_queues=number_of_queues
        self.client = KafkaClient(hosts=self.kafka_hosts)
        self.queues = []
        self.producers = {}
        self.configure_queues()
        self.running = True
        self.start_workers()

    def configure_queues(self):
        for i in range(self.number_of_queues):
            queue_name = SCHEDULER_QUEUE_FORMAT.format(2**i)
            print("configuring queue: {}".format(queue_name))
            queue_topic = self.client.topics[queue_name]
            queue_consumer = queue_topic.get_balanced_consumer(
                consumer_group=CONSUMER_GROUP,
                zookeeper_connect=self.zookeeper_hosts,
                auto_start=False,
            )
            try:
                queue_consumer.start()
            except SocketDisconnectedError:
                queue_consumer._zookeeper.restart()
                queue_consumer.start()

            queue_producer = queue_topic.get_producer()
            queue_worker = Thread(target=self.watch_queue, args=(i, ))
            self.queues.append(
                SchedulerQueue(
                    queue_name,
                    queue_topic,
                    queue_consumer,
                    queue_producer,
                    queue_worker,
                )
            )

    def start_workers(self):
        for queue in self.queues:
            queue.worker.start()

    def stop_workers(self):
        for queue in self.queues:
            queue.worker.stop()

    def calculate_next_queue(self, send_time):
        time_remaining = time() - send_time
        return int(min(
            floor(
                log(
                    time_remaining,
                    2
                )
            ),
            self.number_of_queues
        ))

    def send_event(self, event_reference):
        event = self.retrieve_event(event_reference)
        if not self.producers.has_key(event.topic):
            self.producers[event.topic] = self.client.topics[event.topic].get_producer()
        self.producers[event.topic].produce([event.message])

    def handle_reference(self, event_reference):
        """
        Consumer method for schedule references. Determines whether the event is ready to be
        sent, or publishes a new reference to the appropriate schedule queue.
        :param event_reference: re
        :return:
        """
        if event_reference.send_time > time():
            self.send_event(event_reference)
        else:
            event_reference.enqueue_time=time()
            message = str(event_reference.to_dict())
            queue_index = self.calculate_next_queue(event_reference.send_time)
            self.queues[queue_index].producer.produce([message])

    def handle_incoming_schedule_events(self):
        """
        Main task that processes events from the input queue, and places them into
        the appropriate schedule queues based on their specified send time.
        """
        topic = self.client.topics[self.input_topic]
        consumer = topic.get_balanced_consumer()
        for message in consumer:
            try:
                event = ScheduledEvent(json.load(message.value))
            except:
                print "malformed scheduled event, ignoring"

            event_reference = ScheduledEventReference(
                message.partition,
                message.offset,
                time(),
                event.send_time
            )
            self.handle_reference(event_reference)

    def watch_queue(self, queue_index):
        """
        threadable task to watch a schedule queue, moving items through as necessary, sleeping
        when the event in the queue is due more than the queue duration in the future

        :param queue_index: the "index" for the worker to watch, correllates to a queue duration of
                                2**queue_index
        :type queue_index:
        """
        queue_duration = 2**queue_index
        while self.running:
            for message in self.queues[queue_index].consumer:
                event_reference = ScheduledEventReference.from_dict(json.loads(message.value))
                if event_reference.enqueue_time < time.time() + queue_duration:
                    # we can assume that no events have been in the queue for longer than the this item
                    # therefore, we can safely sleep for a full queue_duration before proceeding
                    break
                else:
                    self.handle_reference(event_reference)
            sleep(queue_duration)

class MasterConsumer():
    """
    This is an incredibly hacky random access consumer. Random access isn't natively supported
    by any of the python kafka clients, so this is  pretty fragile if the broker implementation
    changes in pykafka
    """
    MAX_BYTES=2*1024
    # this value is up for debate, but I _really_ hope we never have messages over 2 megs
    # I would in fact recommend putting a specific message size limit on the input queue, and
    # mirroring that setting here, as this value hugely affects performance.

    def __init__(self, input_topic, client):
        self.input_topic = input_topic
        self.client = client

    def retrieve_event(self, event_reference):
        leader = self.client.topics[self.input_topic].partitions[event_reference.partition].leader
        request = PartitionFetchRequest(
            self.input_topic,
            event_reference.partition,
            event_reference.offset,
            self.MAX_BYTES
        )
        response = leader.fetch_messages([request])
        message = response.topics["test"][0].message[0]
        if message.partition == event_reference.partition & message.offset == event_reference.offset :
            return ScheduledEvent.from_dict(json.load(message.value))
        else:
            raise Exception("record not found")


class SchedulerQueue():
    """
    a bucket of a given size in the scheduler, provides a convenient way to index
    consumers and producers for each bucket
    """
    def __init__(self, name, topic, consumer, producer, worker):
        self.name = name
        self.topic = topic
        self.consumer = consumer
        self.producer = producer
        self.worker = worker


class ScheduledEvent():

    def __init__(self, topic, message, send_time):
        self.topic = topic
        self.message = message
        self.send_time = send_time

    def from_dict(self, dict):
        return ScheduledEvent(
            dict["topic"],
            dict["message"],
            dict["send_time"],
        )

    def to_dict(self):
        return {
            "topic":self.topic,
            "message":self.message,
            "send_time":self.send_time
        }


class ScheduledEventReference():

    def __init__(self, partition, offset, enqueue_time, send_time):
        self.partition = partition
        self.offset = offset
        self.enqueue_time = enqueue_time
        self.send_time = send_time


    def from_dict(self, dict):
        return ScheduledEventReference(
            dict["partition"],
            dict["offset"],
            dict["enqueue_time"],
            dict["send_time"],
        )

    def to_dict(self):
        return {
            "partition":self.partition,
            "offset":self.offset,
            "enqueue_time":self.enqueue_time,
            "send_time":self.send_time
        }
