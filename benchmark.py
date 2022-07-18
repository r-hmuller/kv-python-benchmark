import datetime
import sys
from random import choice, randint
from string import ascii_lowercase
import time

import requests


# Time in seconds
# Path of the file to save the results
# The URL to send POST commands
def execute(time_to_run, url, file_to_save, main_thread, main_client, should_seed):
    selected_key = randint(0, 1_000_000)
    new_value = "".join(choice(ascii_lowercase) for i in range(1024))
    endTime = datetime.datetime.now() + datetime.timedelta(seconds=time_to_run)
    session = requests.Session()
    if main_thread is True and main_client is True:
        print("Starting test")
        r = session.post(url=f"{url}/testing", data={'action': 'start'})
        print(f"Status: {r.status_code}")
        latencies = {}
        while datetime.datetime.now() < endTime:
            if randint(0, 50) == 22:
                start_time = time.time()
                r = session.post(url=url, data={'key': selected_key,
                                             'value': new_value})
                end_time = time.time()
                latency = end_time - start_time
                latencies[start_time] = latency
            else:
                r = session.post(url=url, data={'key': selected_key,
                                             'value': new_value})
            time.sleep(0.2)

        arguments = sys.argv[1:]
        print("Finishing test")
        r = session.post(url=f"{url}/testing", data={'action': 'stop', 'path': f"/data/{arguments[1]}_clients_{arguments[0]}_threads.log"})
        print(f"Status: {r.status_code}")
        if r.status_code != 204:
            print(r.content)

        r.close()
        with open(file_to_save, 'a') as f:
            for key, value in latencies.items():
                f.write('%s,%s\n' % (key, value))
    else:
        while datetime.datetime.now() < endTime:
            r = session.post(url=url, data={'key': selected_key,
                                         'value': new_value})
            time.sleep(0.2)
