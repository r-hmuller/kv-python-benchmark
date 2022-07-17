# Press the green button in the gutter to run the script.
import multiprocessing
import sys

import benchmark

if __name__ == '__main__':
    #1 Num thread
    #2 Num client
    #3 Seconds to run
    #4 URL
    #5 File to write
    #6 main client
    #7 Should populate database

    arguments = sys.argv[1:]
    jobs = []

    for i in range(int(arguments[0])):
        process = multiprocessing.Process(
            target=benchmark.execute,
            args=(int(arguments[2]), arguments[3], arguments[4], True if i == 0 else False, arguments[5] == 'True', arguments[6] == 'True')
        )
        jobs.append(process)

    for j in jobs:
        j.start()

    for j in jobs:
        j.join()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
