from abc import ABCMeta, abstractmethod
from logging import ERROR, basicConfig, getLogger
from multiprocessing import Process
from time import sleep, time


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
        self.logger = self.configure_logger()

    def configure_logger(self):
        config = basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            filename=os.path.abspath(LOG_FILENAME),
        )
        schedule_log = getLogger(config)
        schedule_log.setLevel(ERROR)
        return schedule_log

    def run(self):
        print("starting queue with consumer: {}".format(self.consumer))
        self.watch_queue()

    @abstractmethod
    def watch_queue(self):
        pass

    def calculate_next_queue(self, delay):
        """
        Calculates the greatest power of two that is less than the remaining delay on an event.
        :param delay: seconds till event should be sent
        :return: name of the queue to move this reference to
        """
        if delay < 2:
            queue = 1
        else:
            queue = int(2**min(floor(log(delay, 2)), self.number_of_queues))
        return SCHEDULER_QUEUE_FORMAT.format(queue)

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
        delay_remaning = round(event_reference.send_time - time())
        if delay_remaning < 0:
            self.send_event(event_reference)
        else:
            event_reference.enqueue_time=time()
            message = json.dumps(event_reference.to_dict())
            queue_topic = self.calculate_next_queue(delay_remaning)
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
        when the current event in the queue was added after the current pass was started

        :param queue: the queue to monitor
        :type queue: SchedulerQueue
        """
        while True:
            start = time()
            next = start + self.duration - .1
            new_messages = False
            try:
                for message in self.consumer:
                    event_reference = ScheduledEventReference.from_dict(json.loads(message.value))
                    if event_reference.enqueue_time > start:
                        # if we don't explicitly set the offset here, we seem to sometimes drop references
                        self.consumer.set_topic_partitions(
                            (
                                self.consumer._topics[0][0],
                                message.partition,
                                message.offset
                            )
                        )
                        # we can assume that no events have been in the queue for longer than this item
                        # so we can safely sleep for a full queue_duration before proceeding
                        break
                    else:
                        self.handle_reference(event_reference)
                        self.consumer.task_done(message)
                        new_messages = True
            except ConsumerTimeout:
                # nothing currently in the queue, safe to sleep
                pass
            if new_messages:
                self.consumer.commit()
            sleep_time = next - time()
            if sleep_time > 0:
                sleep(sleep_time)


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
