# Press the green button in the gutter to run the script.
import multiprocessing
import sys

import benchmark

if __name__ == '__main__':
    #1 Num thread
    #2 Seconds to run
    #3 URL
    #4 File to write
    arguments = sys.argv[1:]
    jobs = []

    for i in range(int(arguments[0])):
        process = multiprocessing.Process(
            target=benchmark.execute,
            args=(int(arguments[1]), arguments[2], arguments[3], True if i == 0 else False)
        )
        jobs.append(process)

    for j in jobs:
        j.start()

    for j in jobs:
        j.join()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
