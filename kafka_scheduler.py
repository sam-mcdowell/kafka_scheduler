from abc import ABCMeta, abstractmethod
from multiprocessing import Process
from kafka.client import KafkaClient
from kafka.common import ConsumerTimeout
from kafka.consumer import SimpleConsumer, KafkaConsumer
from kafka.producer import SimpleProducer

from math import log, floor
import json

from time import sleep, time

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
        self.queues = []
        self.configure_internal_queues()
        self.configure_input_queue()
        self.start_workers()

    def configure_internal_queues(self):
        for i in range(self.number_of_queues):
            client = KafkaClient(hosts=self.kafka_hosts)
            queue_name = SCHEDULER_QUEUE_FORMAT.format(2**i)
            indexed_consumer = IndexedConsumer(self.input_topic, self.kafka_hosts)
            queue_consumer = KafkaConsumer(
                queue_name,
                bootstrap_servers=self.kafka_hosts,
                group_id=CONSUMER_GROUP,
                consumer_timeout_ms=500,
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


class SchedulerQueue(Process):
    """
    provides the basic functionality of storing and transferring events, and event references
    """
    __metaclass__ = ABCMeta

    def __init__(self, consumer, indexed_consumer, producer, number_of_queues):
        self.consumer = consumer
        self.indexed_consumer = indexed_consumer
        self.producer = producer
        self.number_of_queues = number_of_queues
        super(SchedulerQueue, self).__init__()
        self.daemon = True

    def run(self):
        print("starting queue with consumer: {}".format(self.consumer))
        self.watch_queue()

    @abstractmethod
    def watch_queue(self):
        pass

    def calculate_next_queue(self, send_time):
        time_remaining = send_time - time()
        if time_remaining < 2:
            index = 0
        else:
            index = min(floor(log(time_remaining, 2)), self.number_of_queues)
        return int(index)

    def send_event(self, event_reference):
        """
        requeues the original message into the appropriate topic once its send time has come
        :param event_reference: reference to event to send
        """
        event = self.indexed_consumer.retrieve_event(event_reference)
        self.producer.send_messages(event.topic, str(event.message))

    def handle_reference(self, event_reference):
        """
        Consumer method for schedule references. Determines whether the event is ready to be
        sent, or publishes a new reference to the appropriate schedule queue.
        :param event_reference: reference to event to be updated or sent along
        """
        if event_reference.send_time <= time():
            self.send_event(event_reference)
        else:
            event_reference.enqueue_time=time()
            message = json.dumps(event_reference.to_dict())
            queue_index = self.calculate_next_queue(event_reference.send_time)
            queue_topic = SCHEDULER_QUEUE_FORMAT.format(2**queue_index)
            self.producer.send_messages(queue_topic, message)


class InternalQueue(SchedulerQueue):
    """
    defines an 'internal' queue, where references to scheduled events are sorted while
    they wait to be sent
    """
    def __init__(self, consumer, indexed_consumer, producer, number_of_queues, duration):
        self.duration = duration
        super(InternalQueue, self).__init__(consumer, indexed_consumer, producer, number_of_queues)

    def watch_queue(self):
        """
        task to watch a schedule queue, moving items through as necessary, sleeping
        when the current event in the queue is due more than the queue duration in the future

        :param queue: the queue to monitor
        :type queue: SchedulerQueue
        """
        while True:
            cutoff = time()
            try:
                for message in self.consumer:
                    event_reference = ScheduledEventReference.from_dict(json.loads(message.value))
                    self.handle_reference(event_reference)
                    if event_reference.enqueue_time > cutoff:
                        # we can assume that no events have been in the queue for longer than the this item
                        # so we can safely sleep for a full queue_duration before proceeding
                        break
            except ConsumerTimeout:
                # nothing currently in the queue, safe to sleep
                pass
            sleep(self.duration)


class InputQueue(SchedulerQueue):
    """
    watches the input queue and sorts incoming events into the appropriate bins
    """
    def __init__(self, consumer, indexed_consumer, producer, number_of_queues):
        super(InputQueue, self).__init__(consumer, indexed_consumer, producer, number_of_queues)

    def watch_queue(self):
        """
        Main task that processes events from the input queue, and places them into
        the appropriate schedule queues based on their specified send time.
        """

        for message in self.consumer:
            try:
                event = ScheduledEvent.from_dict(json.loads(message.value))
            except:
                print "malformed scheduled event, ignoring"
                continue

            event_reference = ScheduledEventReference(
                message.partition,
                message.offset,
                time(),
                event.send_time
            )
            self.handle_reference(event_reference)


class ScheduledEvent():

    def __init__(self, topic, message, send_time):
        self.topic = topic
        self.message = message
        self.send_time = send_time

    @classmethod
    def from_dict(cls, dict):
        return cls(
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

    def __repr__(self):
        return "{}: partition: {}, offset: {}, enqueue_time: {}, send_time: {}".format(
            self.__class__,
            self.partition,
            self.offset,
            self.enqueue_time,
            self.send_time
        )

    @classmethod
    def from_dict(cls, dict):
        return cls(
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
