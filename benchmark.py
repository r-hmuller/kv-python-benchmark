import datetime
import sys
from random import choice, randint
from string import ascii_lowercase
import time

import requests


# Time in seconds
# Path of the file to save the results
# The URL to send POST commands
def execute(time_to_run, url, file_to_save, main_thread, main_client, debug, thinking_time, store_to_file,
            read_from_file, requests_file):
    endTime = datetime.datetime.now() + datetime.timedelta(seconds=time_to_run)
    session = requests.Session()
    if read_from_file is False:
        if main_thread is True and main_client is True:
            print("Starting test")
            r = session.post(url=f"{url}/testing", data={'action': 'start'})
            print(f"Status: {r.status_code}")
            latencies = {}
            while datetime.datetime.now() < endTime:
                selected_key = randint(0, 1_000_000)
                new_value = "".join(choice(ascii_lowercase) for i in range(1024))
                if randint(0, 50) == 22:
                    generate_requests_main_thread(session=session, url=url, key=selected_key, value=new_value,
                                                  debug=debug, latencies=latencies)
                else:
                    generate_requests_secondaries_threads(session=session, url=url, key=selected_key, value=new_value)
                if store_to_file is True:
                    save_to_file(file=requests_file, key=selected_key, value=new_value)
                time.sleep(thinking_time)

            arguments = sys.argv[1:]
            print("Finishing test")
            r = session.post(url=f"{url}/testing",
                             data={'action': 'stop',
                                   'path': f"/data/{arguments[1]}_clients_{arguments[0]}_threads.log",
                                   'dumpMemory': f"/data/{arguments[1]}_clients_{arguments[0]}_threads_dumpmemory.log"
                                   })
            print(f"Status: {r.status_code}")
            if r.status_code != 204:
                print(r.content)

            r.close()
            with open(file_to_save, 'a') as f:
                for key, value in latencies.items():
                    f.write('%s,%s\n' % (key, value))
        else:
            while datetime.datetime.now() < endTime:
                selected_key = randint(0, 1_000_000)
                new_value = "".join(choice(ascii_lowercase) for i in range(1024))
                generate_requests_secondaries_threads(session=session, url=url, key=selected_key, value=new_value)
                if store_to_file is True:
                    save_to_file(file=requests_file, key=selected_key, value=new_value)
                time.sleep(thinking_time)
    else:
        if main_thread is True and main_client is True:
            reproduce_requests_main_thread(store_to_file)
        else:
            reproduce_requests_secondaries_threads(store_to_file)


def generate_requests_main_thread(session, url, key, value, debug, latencies):
    start_time = time.time()
    r = session.post(url=url, data={'key': key,
                                    'value': value})
    if debug is True:
        print("Debugging request")
        print(r.status_code)
        print(r.content)
    end_time = time.time()
    latency = end_time - start_time
    latencies[start_time] = latency


def generate_requests_secondaries_threads(session, url, key, value):
    r = session.post(url=url, data={'key': key,
                                    'value': value})


def reproduce_requests_main_thread(file, session, url, debug, latencies):
    lines = get_file_content(file)
    for line in lines:
        splitted_line = line.split(';')
        if randint(0, 50) == 22:
            start_time = time.time()
            r = session.post(url=url, data={'key': splitted_line[0],
                                            'value': splitted_line[1]})
            if debug is True:
                print("Debugging request")
                print(r.status_code)
                print(r.content)
            end_time = time.time()
            latency = end_time - start_time
            latencies[start_time] = latency


def reproduce_requests_secondaries_threads(file, session, url):
    lines = get_file_content(file)
    for line in lines:
        splitted_line = line.split(';')
        r = session.post(url=url, data={'key': splitted_line[0],
                                        'value': splitted_line[1]})


def save_to_file(file, key, value):
    with open(file, 'a') as f:
        f.write('%s;%s\n' % (key, value))


def get_file_content(file):
    with open(file) as f:
        return f.readlines()
