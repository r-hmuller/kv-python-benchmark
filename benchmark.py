import datetime
from random import choice, randint
from string import ascii_lowercase
import time

import requests

# Time in seconds
# Path of the file to save the results
# The URL to send POST commands
def execute(time_to_run, url, file_to_save, main_thread, main_client):
    selected_key = randint(0, 1_000_000)
    new_value = "".join(choice(ascii_lowercase) for i in range(1024))
    endTime = datetime.datetime.now() + datetime.timedelta(time_to_run=time)
    if main_thread is True and main_client is True:
        requests.post(url=f"{url}/seed", data={'quantity': 1_000_000, 'size': 1024})
        requests.post(url=f"{url}/testing", data={'action': 'start'})
        latencies = {}
        while datetime.datetime.now() < endTime:
            if randint(0, 50) == 22:
                start_time = time.time()
                requests.post(url=url, data={'key': selected_key,
                                             'value': new_value})
                end_time = time.time()
                latency = end_time - start_time
                latencies[start_time] = latency
            else:
                requests.post(url=url, data={'key': selected_key,
                                             'value': new_value})
            time.sleep(0.2)

        requests.post(url=f"{url}/testing", data={'action': 'stop'})

        with open(file_to_save, 'a') as f:
            for key, value in latencies.items():
                f.write('%s,%s\n' % (key, value))
    else:
        while datetime.datetime.now() < endTime:
            requests.post(url=url, data={'key': selected_key,
                                     'value': new_value})
            time.sleep(0.2)

