import datetime
import random
from random import choice
from string import ascii_lowercase
import time

import requests


# Time in seconds
# Path of the file to save the results
# The URL to send POST commands
def execute(time, url, file_to_save):
    endTime = datetime.datetime.now() + datetime.timedelta(seconds=time)
    latencies = {}
    while datetime.datetime.now() < endTime:
        selected_key = randint(0, 1_000_000)
        new_value = "".join(choice(ascii_lowercase) for i in range(1024))
        if random.randint(0, 50) == 22:
            start_time = time.time()
            r = requests.post(url=url, data={'key': selected_key,
                                             'value': new_value})
            end_time = time.time()
            latency = end_time - start_time
            latencies[start_time] = latency
        else:
            r = requests.post(url=url, data={'key': selected_key,
                                             'value': new_value})
        time.sleep(0.2)

    with open(file_to_save, 'a') as f:
        for key, value in latencies.items():
            f.write('%s,%s\n' % (key, value))