import sys

import requests

file_name = sys.argv[1]
url = sys.argv[2]

session = requests.Session()

with open(file_name, 'r') as file:
    for line in file:
        values = line.split(';')
        key = values[0]
        value = values[1]

        response = session.get(url=f"{url}?key={key}")
        if response.text != value:
            print(f"Key: {key} is different")

print("Done")
