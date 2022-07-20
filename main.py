# Press the green button in the gutter to run the script.
import multiprocessing
import sys

import requests

import benchmark

if __name__ == '__main__':
    #1 Num thread
    #2 Num client
    #3 Seconds to run
    #4 URL
    #5 File to write
    #6 main client
    #7 Should populate database
    #8 Debug response
    arguments = sys.argv[1:]
    jobs = []

    if arguments[6] == 'True':
        print("Running seed")
        r = requests.post(url=f"{arguments[3]}/seed", data={'quantity': 1_000_000, 'size': 1024})
        print(f"Status: {r.status_code}")

    for i in range(int(arguments[0])):
        process = multiprocessing.Process(
            target=benchmark.execute,
            args=(int(arguments[2]), arguments[3], arguments[4], True if i == 0 else False, arguments[5] == 'True', arguments[7] == 'True')
        )
        jobs.append(process)

    for j in jobs:
        j.start()

    for j in jobs:
        j.join()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
