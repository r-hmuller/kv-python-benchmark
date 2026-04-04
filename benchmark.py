import asyncio
from collections import defaultdict
import datetime
import sys
from random import choice, randint
from string import ascii_lowercase
import time

import aiohttp
import requests as requests_sync


# Time in seconds
# Path of the file to save the results
# The URL to send POST commands
def execute(time_to_run, url, file_to_save, main_thread, main_client, debug, thinking_time, store_to_file,
            read_from_file, requests_file):
    asyncio.run(_execute_async(time_to_run, url, file_to_save, main_thread, main_client, debug, thinking_time,
                               store_to_file, read_from_file, requests_file))


async def _execute_async(time_to_run, url, file_to_save, main_thread, main_client, debug, thinking_time,
                         store_to_file, read_from_file, requests_file):
    endTime = datetime.datetime.now() + datetime.timedelta(seconds=time_to_run)
    timeout = aiohttp.ClientTimeout(total=240)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if read_from_file is False:
            if main_thread is True and main_client is True:
                print("Starting test")
                r = requests_sync.post(url=f"{url}/testing", json={'action': 'start'})
                print(f"Status: {r.status_code}")
                latencies = {}
                status_counts = defaultdict(int)
                pending_tasks = []
                while datetime.datetime.now() < endTime:
                    selected_key = randint(0, 1_000_000)
                    new_value = "".join(choice(ascii_lowercase) for i in range(1024))
                    if randint(0, 50) == 22 or randint(0, 50) == 12:
                        task = asyncio.create_task(
                            generate_requests_main_thread(session=session, url=url, key=selected_key,
                                                         value=new_value, debug=debug, latencies=latencies,
                                                         status_counts=status_counts))
                    else:
                        task = asyncio.create_task(
                            generate_requests_secondaries_threads(session=session, url=url, key=selected_key,
                                                                  value=new_value, status_counts=status_counts))
                    pending_tasks.append(task)
                    if store_to_file is True:
                        save_to_file(file=requests_file, key=selected_key, value=new_value)
                    await asyncio.sleep(thinking_time)

                # Wait for all in-flight requests to complete
                if pending_tasks:
                    await asyncio.gather(*pending_tasks, return_exceptions=True)

                print_status_counts(status_counts)

                arguments = sys.argv[1:]
                print("Finishing test")
                r = requests_sync.post(url=f"{url}/testing",
                                       json={'action': 'stop',
                                             'path': f"/data/{arguments[1]}_clients_{arguments[0]}_threads.log"
                                             })
                print(f"Status: {r.status_code}")
                if r.status_code != 204:
                    print(r.content)

                with open(file_to_save, 'a') as f:
                    f.write('--- Status Counts ---\n')
                    for status, count in sorted(status_counts.items(), key=lambda x: str(x[0])):
                        f.write('%s,%s\n' % (status, count))
                    f.write('--- Latencies ---\n')
                    for key, value in latencies.items():
                        f.write('%s,%s\n' % (key, value))
            else:
                status_counts = defaultdict(int)
                pending_tasks = []
                while datetime.datetime.now() < endTime:
                    selected_key = randint(0, 1_000_000)
                    new_value = "".join(choice(ascii_lowercase) for i in range(1024))
                    task = asyncio.create_task(
                        generate_requests_secondaries_threads(session=session, url=url, key=selected_key,
                                                              value=new_value, status_counts=status_counts))
                    pending_tasks.append(task)
                    if store_to_file is True:
                        save_to_file(file=requests_file, key=selected_key, value=new_value)
                    await asyncio.sleep(thinking_time)

                if pending_tasks:
                    await asyncio.gather(*pending_tasks, return_exceptions=True)

                print_status_counts(status_counts)
        else:
            if main_thread is True and main_client is True:
                await reproduce_requests_main_thread(store_to_file, session, url, debug, {})
            else:
                await reproduce_requests_secondaries_threads(store_to_file, session, url)


async def generate_requests_main_thread(session, url, key, value, debug, latencies, status_counts):
    start_time = time.time()
    try:
        async with session.post(url=url, data={'key': key, 'value': value}) as r:
            status_counts[r.status] += 1
            if debug is True:
                print("Debugging request")
                print(r.status)
                print(await r.read())
        end_time = time.time()
        latency = end_time - start_time
        latencies[start_time] = latency
    except Exception as e:
        status_counts["error"] += 1
        print(f"Request error: {e}")


async def generate_requests_secondaries_threads(session, url, key, value, status_counts):
    try:
        async with session.post(url=url, data={'key': key, 'value': value}) as r:
            status_counts[r.status] += 1
    except Exception:
        status_counts["error"] += 1


async def reproduce_requests_main_thread(file, session, url, debug, latencies):
    status_counts = defaultdict(int)
    lines = get_file_content(file)
    tasks = []
    for line in lines:
        splitted_line = line.split(';')
        if randint(0, 50) == 22:
            task = asyncio.create_task(
                _reproduce_single_main(session, url, splitted_line, debug, latencies, status_counts))
            tasks.append(task)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    print_status_counts(status_counts)


async def _reproduce_single_main(session, url, splitted_line, debug, latencies, status_counts):
    start_time = time.time()
    try:
        async with session.post(url=url, data={'key': splitted_line[0], 'value': splitted_line[1]}) as r:
            status_counts[r.status] += 1
            if debug is True:
                print("Debugging request")
                print(r.status)
                print(await r.read())
        end_time = time.time()
        latency = end_time - start_time
        latencies[start_time] = latency
    except Exception as e:
        status_counts["error"] += 1
        print(f"Request error: {e}")


async def reproduce_requests_secondaries_threads(file, session, url):
    status_counts = defaultdict(int)
    lines = get_file_content(file)
    tasks = []
    for line in lines:
        splitted_line = line.split(';')
        task = asyncio.create_task(
            generate_requests_secondaries_threads(session, url, splitted_line[0], splitted_line[1],
                                                  status_counts))
        tasks.append(task)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    print_status_counts(status_counts)


def print_status_counts(status_counts):
    parts = [f"{status}: {count}" for status, count in sorted(status_counts.items(), key=lambda x: str(x[0]))]
    print("Status counts - " + " | ".join(parts))


def save_to_file(file, key, value):
    with open(file, 'a') as f:
        f.write('%s;%s\n' % (key, value))


def get_file_content(file):
    with open(file) as f:
        return f.readlines()
