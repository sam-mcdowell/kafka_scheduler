from pykafka import BalancedConsumer
from pykafka import SimpleConsumer


class ScheduleConsumer(object, BalancedConsumer):

    def __init__(self, topic, cluster, consumer_group, **kwargs):
        super(BalancedConsumer, self).__init__(topic, cluster, consumer_group, **kwargs)

    def consume(self, block=True):
        """process all messages currently in the queue

        :param block: Whether to block while waiting for a message
        :type block: bool
        """

        def consumer_timed_out():
            """Indicates whether the consumer has received messages recently"""
            if self._consumer_timeout_ms == -1:
                return False
            disp = (time.time() - self._last_message_time) * 1000.0
            return disp > self._consumer_timeout_ms
        message = None
        self._last_message_time = time.time()
        while message is None and not consumer_timed_out():
            try:
                message = self._consumer.consume(block=block)
            except ConsumerStoppedException:
                if not self._running:
                    return
                continue
            if message:
                self._last_message_time = time.time()
            if not block:
                return message
        return message

    def consume_current(self):

class CurrentConsumer(object, SimpleConsumer):

    def __init__(self, topic, cluster, consumer_group=None, **kwargs):
        super(SimpleConsumer, self).__init__(topic, cluster, consumer_group=consumer_group, **kwargs)

    def fetch_current(self)
        """Fetch new messages for all partitions

        Create a FetchRequest for each broker and send it. Enqueue each of the
        returned messages in the approprate OwnedPartition.
        """
        def _handle_success(parts):
            for owned_partition, pres in parts:
                if len(pres.messages) > 0:
                    log.debug("Fetched %s messages for partition %s",
                              len(pres.messages), owned_partition.partition.id)
                    owned_partition.enqueue_messages(pres.messages)
                    log.debug("Partition %s queue holds %s messages",
                              owned_partition.partition.id,
                              owned_partition.message_count)

        for broker, owned_partitions in self._partitions_by_leader.iteritems():
            partition_reqs = {}
            for owned_partition in owned_partitions:
                # attempt to acquire lock, just pass if we can't
                if owned_partition.fetch_lock.acquire(False):
                    partition_reqs[owned_partition] = None
                    if owned_partition.message_count < self._queued_max_messages:
                        fetch_req = owned_partition.build_fetch_request(
                            self._fetch_message_max_bytes)
                        partition_reqs[owned_partition] = fetch_req
                    else:
                        log.debug("Partition %s above max queued count (queue has %d)",
                                  owned_partition.partition.id,
                                  owned_partition.message_count)
            if partition_reqs:
                try:
                    response = broker.fetch_messages(
                        [a for a in partition_reqs.itervalues() if a],
                        timeout=self._fetch_wait_max_ms,
                        min_bytes=self._fetch_min_bytes
                    )
                except SocketDisconnectedError:
                    # If the broker dies while we're supposed to stop,
                    # it's fine, and probably an integration test.
                    if not self._running:
                        return
                    else:
                        raise Exception("")

                parts_by_error = build_parts_by_error(response, self._partitions_by_id)
                # release the lock in these cases, since resolving the error
                # requires an offset reset and not releasing the lock would
                # lead to a deadlock in reset_offsets. For successful requests
                # or requests with different errors, we still assume that
                # it's ok to retain the lock since no offset_reset can happen
                # before this function returns
                out_of_range = parts_by_error.get(OffsetOutOfRangeError.ERROR_CODE, [])
                for owned_partition, res in out_of_range:
                    owned_partition.fetch_lock.release()
                    # remove them from the dict of partitions to unlock to avoid
                    # double-unlocking
                    partition_reqs.pop(owned_partition)
                # handle the rest of the errors that don't require deadlock
                # management
                handle_partition_responses(
                    self._default_error_handlers,
                    parts_by_error=parts_by_error,
                    success_handler=_handle_success)
                # unlock the rest of the partitions
                for owned_partition in partition_reqs.iterkeys():
                    owned_partition.fetch_lock.release()