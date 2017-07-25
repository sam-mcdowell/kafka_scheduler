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
