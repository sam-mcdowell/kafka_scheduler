import threading
from time import sleep


class DelayedPrint(threading.Thread):
    def __init__(self, message, delay):
        threading.Thread.__init__(self)
        self.message = message
        self.delay = delay

    def run(self):
        sleep(self.delay)
        print self.message


print_goodbye_5  = DelayedPrint("Goodbye", 5)

print_hello_2  = DelayedPrint("Hello", 2)

print_goodbye_5.start()
print_hello_2.start()