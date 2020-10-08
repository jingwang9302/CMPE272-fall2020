import yaml
import schedule
import requests
import time

with open(r'./input.yaml') as file:
    yaml_file = yaml.safe_load(file)
URL = yaml_file['Steps'][0][1]['outbound_url']
METHOD = yaml_file['Steps'][0][1]['method']
SCHEDULER_WHEN = yaml_file['Scheduler']['when']

print(URL)
print(SCHEDULER_WHEN)
print(METHOD)


def http_client():
    #Make http call
    response = requests.request(METHOD, URL)
    print(response)
    return response


def run_steps():
    print("hello world")


def set_scheduler():
    mm = 1
    schedule.every(mm).minutes.do(run_steps)


http_client()
# set_scheduler()

# while True:
#     schedule.run_pending()
#     time.sleep(1)